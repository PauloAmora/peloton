
#include "util/file_util.h"
#include <dirent.h>
#include <fstream>

namespace peloton {


bool FileUtil::CreateDirectory(const char *dir_name, int mode) {
  int return_val = mkdir(dir_name, mode);
  if (return_val == 0) {
    LOG_DEBUG("Created directory %s successfully", dir_name);
  } else if (errno == EEXIST) {
    LOG_DEBUG("Directory %s already exists", dir_name);
  } else {
    LOG_DEBUG("Creating directory failed: %s", strerror(errno));
    return false;
  }
  return true;
}

bool FileUtil::CheckDirectoryExistence(const char *dir_name) {
  struct stat info;
  int return_val = stat(dir_name, &info);
  return return_val == 0 && S_ISDIR(info.st_mode);
}

/**
 * @return false if fail to remove directory
 */
bool FileUtil::RemoveDirectory(const char *dir_name, bool only_remove_file) {
  struct dirent *file;
  DIR *dir;

  dir = opendir(dir_name);
  if (dir == nullptr) {
    return true;
  }

  // XXX readdir is not thread safe???
  while ((file = readdir(dir)) != nullptr) {
    if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
      continue;
    }
    char complete_path[256];
    strcpy(complete_path, dir_name);
    strcat(complete_path, "/");
    strcat(complete_path, file->d_name);
    auto ret_val = remove(complete_path);
    if (ret_val != 0) {
      LOG_ERROR("Failed to delete file: %s, error: %s", complete_path,
                strerror(errno));
    }
  }
  closedir(dir);
  if (!only_remove_file) {
    auto ret_val = remove(dir_name);
    if (ret_val != 0) {
      LOG_ERROR("Failed to delete dir: %s, error: %s", file->d_name,
                strerror(errno));
    }
  }
  return true;
}

void FileUtil::FFlushFsync(FileHandle &file_handle) {
  // First, flush
  PL_ASSERT(file_handle.fd != INVALID_FILE_DESCRIPTOR);
  if (file_handle.fd == INVALID_FILE_DESCRIPTOR) return;
  //int ret = fflush(file_handle.file);
  //if (ret != 0) {
  //  LOG_ERROR("Error occured in fflush(%d)", ret);
  //}
  // Finally, sync
  int ret = fsync(file_handle.fd);
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }
}

bool FileUtil::CreateFile(const char *complete_path) {
    std::ofstream os;

    os.open(complete_path, std::ios::binary);
    os.close();

    return true;
}

bool FileUtil::OpenWriteFile(const char *name, const char *mode UNUSED_ATTRIBUTE, FileHandle &file_handle) {
  auto file = open(name, O_DIRECT | O_WRONLY | O_SYNC | O_CREAT, 0777);
  if (file < 0) {
    LOG_ERROR("Checkpoint File is NULL");
    return false;
  } else {
    file_handle.fd = file;
  }

  file_handle.size = GetFileSize(file_handle);
  return true;
}

bool FileUtil::OpenReadFile(const char *name, FileHandle &file_handle) {
  auto file = open(name, O_DIRECT | O_SYNC | O_RDONLY);
  if (file < 0) {
    LOG_ERROR("Checkpoint File is NULL");
    return false;
  } else {
    file_handle.fd = file;
  }
  file_handle.size = GetFileSize(file_handle);
  return true;
}

bool FileUtil::CloseWriteFile(FileHandle &file_handle) {
  //PL_ASSERT(file_handle.file != nullptr && file_handle.fd != INVALID_FILE_DESCRIPTOR);
  int ret = close(file_handle.fd);

  if (ret == 0) {
    file_handle.file = nullptr;
    file_handle.fd = INVALID_FILE_DESCRIPTOR;
  } else {
    LOG_ERROR("Error when closing log file");
  }

  return ret == 0;
}

bool FileUtil::CloseReadFile(FileHandle &file_handle) {
 // PL_ASSERT(file_handle.file != nullptr && file_handle.fd != INVALID_FILE_DESCRIPTOR);
  int ret = close(file_handle.fd);

  if (ret == 0) {
    file_handle.file = nullptr;
    file_handle.fd = INVALID_FILE_DESCRIPTOR;
  } else {
    LOG_ERROR("Error when closing log file");
  }

  return ret == 0;
}

bool FileUtil::IsFileTruncated(FileHandle &file_handle,
                                  size_t size_to_read) {
  // Cache current position
  size_t current_position = ftell(file_handle.file);

  // Check if the actual file size is less than the expected file size
  // Current position + frame length
  if (current_position + size_to_read <= file_handle.size) {
    return false;
  } else {
    fseek(file_handle.file, 0, SEEK_END);
    return true;
  }
}

size_t FileUtil::GetFileSize(FileHandle &file_handle) {
  struct stat file_stats;
  fstat(file_handle.fd, &file_stats);
  return file_stats.st_size;
}

bool FileUtil::ReadNBytesFromFile(FileHandle &file_handle, void *bytes_read, size_t n) {
//  PL_ASSERT(file_handle.fd != INVALID_FILE_DESCRIPTOR && file_handle.file != nullptr);

    uint res = read(file_handle.fd, bytes_read, n);
    //int res = fread(bytes_read, n, 1, file_handle.file);
  return res == n;
}
}
