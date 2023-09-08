// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <string>
#include <string_view>
#include <utility>

#include "io/io.h"
#include "server/common.h"
#include "util/cloud/aws.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/uring/uring_file.h"

namespace dfly {
namespace detail {

using namespace std::literals;

constexpr std::string_view kS3Prefix = "s3://"sv;

const size_t kBucketConnectMs = 2000;

enum FileType : uint8_t {
  FILE = (1u << 0),
  CLOUD = (1u << 1),
  IO_URING = (1u << 2),
  DIRECT = (1u << 3),
};

class SnapshotStorage {
 public:
  virtual ~SnapshotStorage() = default;

  // Opens the file at the given path, and returns the open file and file
  // type, which is a bitmask of FileType.
  virtual io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenFile(
      const std::string& path) = 0;
};

class FileSnapshotStorage : public SnapshotStorage {
 public:
  FileSnapshotStorage(FiberQueueThreadPool* fq_threadpool);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenFile(
      const std::string& path) override;

 private:
  util::fb2::FiberQueueThreadPool* fq_threadpool_;
};

class AwsS3SnapshotStorage : public SnapshotStorage {
 public:
  AwsS3SnapshotStorage(util::cloud::AWS* aws);

  io::Result<std::pair<io::Sink*, uint8_t>, GenericError> OpenFile(
      const std::string& path) override;

 private:
  util::cloud::AWS* aws_;
};

// Returns bucket_name, obj_path for an s3 path.
std::optional<std::pair<std::string, std::string>> GetBucketPath(std::string_view path);

// takes ownership over the file.
class LinuxWriteWrapper : public io::Sink {
 public:
  LinuxWriteWrapper(util::fb2::LinuxFile* lf) : lf_(lf) {
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  std::error_code Close() {
    return lf_->Close();
  }

 private:
  std::unique_ptr<util::fb2::LinuxFile> lf_;
  off_t offset_ = 0;
};

}  // namespace detail
}  // namespace dfly
