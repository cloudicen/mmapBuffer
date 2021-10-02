#include "mmapBlock.h"

void mmapBlock::MapRegion(int fd, uint64_t file_offset, char *&base,
                          size_t map_size) {
  void *ptr = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                   file_offset);
  if (unlikely(ptr == MAP_FAILED)) {
    base = nullptr;
    return;
  }
  base = reinterpret_cast<char *>(ptr);
}

void mmapBlock::UnMapRegion(char *base, size_t map_size) {
  munmap(base, map_size);
}

//自旋锁，实现低开销多线程同时对一个缓存块写入
std::pair<size_t, bool> mmapBlock::append(const char *_data, size_t len) {
  auto writePos = 0;
  auto wirteLen = 0;
  auto isFull = false;
  if (usedSpace.load() == blockSize) { //缓冲区满，直接返回
    return {0, true};
  }
  std::shared_lock lock(mtx_writeOut); //所有写操作结束后释放锁
  while (blockSpinLock.test_and_set()) {
    ;
  }
  if (blockSize - usedSpace.load() <= len) {
    writePos = usedSpace.load();
    wirteLen = blockSize - usedSpace.load();
    usedSpace.store(blockSize);
    isFull = true;
    blockSpinLock.clear();
  } else {
    writePos = usedSpace.fetch_add(len);
    wirteLen = len;
    blockSpinLock.clear();
  }
  memcpy(data + writePos, _data, wirteLen);
  return {wirteLen, isFull};
}

bool mmapBlock::isValid() { return fd != -1 && data != nullptr; }

int mmapBlock::getFd() const { return fd; }

const std::string &mmapBlock::getFilePath() const { return filePath; }

size_t mmapBlock::getUsedSpace() const { return usedSpace.load(); }

size_t mmapBlock::getUsedPages(unsigned int pageSize) const {
  return (usedSpace % pageSize) == 0 ? (usedSpace / pageSize)
                                     : (usedSpace / pageSize) + 1;
}

size_t mmapBlock::getFreeSpace() const { return blockSize - usedSpace.load(); }

bool mmapBlock::isEmpty() const { return 0 == usedSpace.load(); }

void mmapBlock::clear() { usedSpace.store(0); }

size_t mmapBlock::writeOut(int fd, size_t offset, size_t len) {
  assert(fd);
  std::scoped_lock lk(mtx_writeOut); //等待所有写缓存操作结束
  if (len == 0) {
    len = blockSize;
  }
  size_t writeLen = pwrite64(fd, data, len, offset);
  return writeLen;
}