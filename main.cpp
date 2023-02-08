#include <array>
#include <bits/types/struct_iovec.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <ratio>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#include <liburing.h>
#include <liburing/io_uring.h>

unsigned int io_depth = 1024;

class FD {
public:
  explicit FD(int fd) : fd_(fd) {}

  ~FD() {
    if (-1 == close(fd_)) {
      std::cerr << "Failed to close FD: " << fd_ << std::endl;
    }
  }

private:
  int fd_;
};

bool need_sync_data(int flags, uint64_t seq, int writes) {
  if ((flags & O_DIRECT) == O_DIRECT) {
    return false;
  }

  if (!seq || seq % 1024) {
    return false;
  }

  if (writes >= io_depth) {
    return false;
  }

  return true;
}

int do_write(io_uring *ring) {
  const char *file_path = "/data/data0";
  int flags = O_CREAT | O_RDWR | O_NOATIME | O_DIRECT | O_NONBLOCK;
  int mode = S_IRWXU | S_IRWXG;
  int fd = open(file_path, flags, mode);

  if (-1 == fd) {
    std::cerr << "Failed to open " << file_path << std::endl;
    return -1;
  }
  FD fd_(fd);
  std::cout << "Open " << file_path << " OK" << std::endl;

  int fds[1];
  fds[0] = fd;
  int ret = io_uring_register_files(ring, fds, 1);
  if (ret) {
    std::cerr << "Failed to register files: " << strerror(-ret) << std::endl;
    return -1;
  } else {
    std::cout << "Register files OK" << std::endl;
  }

  // 4KiB
  // int buf_size = 4096;

  // 16KiB
  // int buf_size = 16384;

  // 64KiB
  int buf_size = 65536;

  int alignment = 4096;
  void *buf = aligned_alloc(alignment, buf_size);
  memset(buf, 1, buf_size);

  iovec iov[1];
  iov[0].iov_base = buf;
  iov[0].iov_len = buf_size;

  ret = io_uring_register_buffers(ring, iov, 1);
  if (ret) {
    std::cerr << "Failed to register buffers: " << strerror(-ret) << std::endl;
    return -1;
  }

  uint64_t pos = 0;

  uint64_t file_size_10_GiB = ((uint64_t)10) * 1024 * 1024 * 1024;

  auto start = std::chrono::steady_clock::now();
  ret = fallocate(fd, 0, 0, file_size_10_GiB);
  if (ret) {
    std::cerr << "Failed to fallocate file: " << strerror(errno) << std::endl;
    return -1;
  }
  auto fallocate_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();
  std::cout << "fallocate costs " << fallocate_us << "us." << std::endl;

  io_uring_cqe *cqe;
  uint64_t seq = 0;

  uint64_t bytes_written = 0;
  uint64_t iops = 0;
  auto current = std::chrono::steady_clock::now();

  int writes = 0;
  while (true) {
    bool need_submit = false;
    auto start = std::chrono::steady_clock::now();
    while (pos < file_size_10_GiB && writes < io_depth) {
      io_uring_sqe *sqe = io_uring_get_sqe(ring);
      if (nullptr == sqe) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cerr << "Failed to get SQE" << std::endl;
        break;
      }

      io_uring_prep_write_fixed(sqe, 0, iov[0].iov_base, buf_size, pos, 0);
      sqe->flags |= IOSQE_FIXED_FILE;
      sqe->user_data = seq++;

      pos += buf_size;
      writes++;
      need_submit = true;

      // fdatasync
      if (need_sync_data(flags, seq, writes)) {
        io_uring_sqe *sqe = io_uring_get_sqe(ring);
        io_uring_prep_fsync(sqe, 0, IORING_FSYNC_DATASYNC);
        sqe->flags |= IOSQE_FIXED_FILE;
        sqe->user_data = seq;
        seq++;
        writes++;
      }
    }
    auto delta = std::chrono::steady_clock::now() - start;
    auto micro_secs =
        std::chrono::duration_cast<std::chrono::microseconds>(delta).count();
    if (micro_secs >= 10) {
      std::cout << "Filling IO Depth[" << writes << "] costs " << micro_secs
                << "us." << std::endl;
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
    int prev = writes;
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
        std::cerr << "Something is wrong with CQE: res: " << strerror(-cqe->res)
                  << ", user_data: " << cqe->user_data << std::endl;
        int code = EIO;
        // std::this_thread::sleep_for(std::chrono::seconds(1));
      } else {
        bytes_written += cqe->res;
        iops += 1;

        if (std::chrono::steady_clock::now() - current >
            std::chrono::seconds(1)) {
          std::cout << "IOPS: " << iops
                    << ", Throughput: " << bytes_written / 1024 / 1024 << "MiB/s"
                    << std::endl;
          bytes_written = 0;
          iops = 0;
          current = std::chrono::steady_clock::now();
        }
      }

      writes--;
      io_uring_cqe_seen(ring, cqe);
    }

    int reap_cnt = prev - writes;
    if (reap_cnt >= 10) {
      std::cout << "Reaped " << reap_cnt << "/" << prev << " IO" << std::endl;
    }

    // All writes are completed
    if (pos >= file_size_10_GiB && !writes) {
      std::cout << "All writes are completed. Pos: " << pos << std::endl;
      break;
    }
  }

  return 0;
}

static const char *op_strs[] = {
    "IORING_OP_NOP",
    "IORING_OP_READV",
    "IORING_OP_WRITEV",
    "IORING_OP_FSYNC",
    "IORING_OP_READ_FIXED",
    "IORING_OP_WRITE_FIXED",
    "IORING_OP_POLL_ADD",
    "IORING_OP_POLL_REMOVE",
    "IORING_OP_SYNC_FILE_RANGE",
    "IORING_OP_SENDMSG",
    "IORING_OP_RECVMSG",
    "IORING_OP_TIMEOUT",
    "IORING_OP_TIMEOUT_REMOVE",
    "IORING_OP_ACCEPT",
    "IORING_OP_ASYNC_CANCEL",
    "IORING_OP_LINK_TIMEOUT",
    "IORING_OP_CONNECT",
    "IORING_OP_FALLOCATE",
    "IORING_OP_OPENAT",
    "IORING_OP_CLOSE",
    "IORING_OP_FILES_UPDATE",
    "IORING_OP_STATX",
    "IORING_OP_READ",
    "IORING_OP_WRITE",
    "IORING_OP_FADVISE",
    "IORING_OP_MADVISE",
    "IORING_OP_SEND",
    "IORING_OP_RECV",
    "IORING_OP_OPENAT2",
    "IORING_OP_EPOLL_CTL",
    "IORING_OP_SPLICE",
    "IORING_OP_PROVIDE_BUFFERS",
    "IORING_OP_REMOVE_BUFFERS",
    "IORING_OP_TEE",
    "IORING_OP_SHUTDOWN",
    "IORING_OP_RENAMEAT",
    "IORING_OP_UNLINKAT",
    "IORING_OP_MKDIRAT",
    "IORING_OP_SYMLINKAT",
    "IORING_OP_LINKAT",
    "IORING_OP_MSG_RING",
    "IORING_OP_FSETXATTR",
    "IORING_OP_SETXATTR",
    "IORING_OP_FGETXATTR",
    "IORING_OP_GETXATTR",
    "IORING_OP_SOCKET",
    "IORING_OP_URING_CMD",
    "IORING_OP_SEND_ZC",
    "IORING_OP_SENDMSG_ZC",
};

void report_features(io_uring *ring) {
  io_uring_probe *features = io_uring_get_probe_ring(ring);
  if (nullptr == features) {
    std::cerr << "Failed to probe supported io_uring features" << std::endl;
  } else {
    printf("Report of your kernel's list of supported io_uring operations:\n");
    for (char i = 0; i < IORING_OP_LAST; i++) {
      printf("%s: ", op_strs[i]);
      if (io_uring_opcode_supported(features, i))
        printf("yes.\n");
      else
        printf("no.\n");
    }
    free(features);
  }
}

int main(int argc, char *argv[]) {

  io_uring uring;
  io_uring_params params = {};
  params.sq_thread_idle = 2000;
  params.sq_thread_cpu = 1;

  unsigned int flags = IORING_SETUP_SQPOLL | IORING_SETUP_IOPOLL;

  // Reset flags for now
  flags = IORING_SETUP_SQPOLL | IORING_SETUP_IOPOLL;

  params.flags = flags;

  int ret = io_uring_queue_init_params(io_depth, &uring, &params);
  if (ret) {
    std::cerr << "Failed to set up io_uring" << strerror(-ret) << std::endl;
  }

  report_features(&uring);

  do_write(&uring);

  io_uring_queue_exit(&uring);

  return EXIT_SUCCESS;
}
