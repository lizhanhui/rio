#include <asm-generic/errno-base.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#include <liburing.h>
#include <liburing/io_uring.h>

unsigned int io_depth = 1024;

int do_write(io_uring *ring) {
  const char *file_path = "/data/data0";
  int flags = O_CREAT | O_APPEND | O_RDWR | O_NOATIME;
  int mode = S_IRWXU | S_IRWXG;
  int fd = open(file_path, flags, mode);

  if (-1 == fd) {
    std::cerr << "Failed to open " << file_path << std::endl;
    return -1;
  }
  std::cout << "Open " << file_path << " OK" << std::endl;

  const size_t buf_size = 1024 * 16;
  void *buf = malloc(buf_size);
  memset(buf, 1, sizeof(buf));

  uint64_t pos = 0;

  uint64_t file_size_10_GiB = ((uint64_t)10) * 1024 * 1024 * 1024;
  io_uring_cqe *cqe;
  uint64_t seq = 0;

  int writes = 0;
  while (true) {
    bool need_submit = false;
    while (pos < file_size_10_GiB && writes < io_depth) {
      io_uring_sqe *sqe = io_uring_get_sqe(ring);
      if (nullptr == sqe) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cerr << "Failed to get SQE" << std::endl;
        break;
      }

      io_uring_prep_write(sqe, fd, buf, buf_size, pos);

      io_uring_sqe_set_data(sqe, (void *)(seq++));
      pos += buf_size;
      writes++;
      need_submit = true;
    }

    if (need_submit) {
      int ret = io_uring_submit(ring);
      if (ret < 0) {
        std::cerr << "Failed to submit: errno: " << ret << std::endl;
      }
    }

    // reap
    bool reaped = false;
    int ret;
    while (writes) {
      if (!reaped) {
        ret = io_uring_wait_cqe(ring, &cqe);
        reaped = true;
      } else {
        ret = io_uring_peek_cqe(ring, &cqe);
        if (-EAGAIN == ret) {
          cqe = nullptr;
          ret = 0;
        }
      }

      if (ret < 0) {
        std::cerr << "Something is wrong" << std::endl;
        return -1;
      }

      if (!cqe) {
        break;
      }

      if (cqe->res < 0) {
        // TODO: error handling
        std::cerr << "Something is wrong with CQE" << std::endl;
      }

      writes--;
      io_uring_cqe_seen(ring, cqe);
    }

    // All writes are completed
    if (pos >= file_size_10_GiB && !writes) {
      std::cout << "All writes are completed. Pos: " << pos << std::endl;
      break;
    }
  }

  int ret = close(fd);
  if (-1 == ret) {
    std::cerr << "Failed to close FD: " << fd << std::endl;
  }

  return 0;
}

int main(int argc, char *argv[]) {

  io_uring uring;

  unsigned int flags = IORING_SETUP_SQPOLL | IORING_SETUP_IOPOLL;

  // Reset flags for now
  flags = 0;

  int ret = io_uring_queue_init(io_depth, &uring, flags);
  if (ret) {
    std::cerr << "Failed to set up io_uring" << strerror(-ret) << std::endl;
  }

  do_write(&uring);

  io_uring_queue_exit(&uring);

  return EXIT_SUCCESS;
}
