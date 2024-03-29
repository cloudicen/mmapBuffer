#ifndef __MMAPBLOCK__
#define __MMAPBLOCK__
#include <assert.h>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/unistd.h>

class mmapBlock {
  /**
   * @brief 执行内存映射
   * @param fd 被映射的文件描述符
   * @param file_offset 映射起点偏移量
   * @param base 存储内存映射块的头指针
   * @param map_size 映射大小
   */
  void MapRegion(int fd, uint64_t file_offset, char *&base, size_t map_size);

  /**
   * @brief 取消内存映射
   * @param base 内存映射块的头指针
   * @param map_size 映射大小
   */
  void UnMapRegion(char *base, size_t map_size);

public:
  /**
   * @brief 内存映射缓存块构造函数
   * @param _filePath 对应文件路径
   * @param _blockSize 缓存块大小
   * @param _prev 缓存块前驱指针
   * @param _next 缓存块后继指针
   */
  mmapBlock(const std::string &_filePath, size_t _blockSize,
            mmapBlock *_prev = nullptr, mmapBlock *_next = nullptr)
      : filePath(_filePath), blockSize(_blockSize), prev(_prev), next(_next) {
    fd = open(filePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0645);
    if (fd > 0) {
      if (posix_fallocate(fd, 0, blockSize) == 0) {
        MapRegion(fd, 0, data, blockSize);
      }
    }
  };

  //删除复制构造函数
  explicit mmapBlock(const mmapBlock &) = delete;
  //删除赋值运算符重载
  mmapBlock &operator=(const mmapBlock &) = delete;

  /**
   * @brief 析构时解除内存映射，关闭和删除磁盘上对应的文件
   */
  ~mmapBlock() {
    if (data != nullptr) {
      UnMapRegion(data, blockSize);
    }
    close(fd);
    remove(filePath.c_str());
  }

  /**
   * @brief 向block中写入数据
   * @param _data 数据指针
   * @param len 写入长度
   * @return 返回成功写入的长度,缓存区满返回0
   */
  std::pair<size_t, bool> append(const char *_data, size_t len);

  /**
   * @brief 检查block的有效性
   */
  bool isValid();

  /**
   * @brief 获取已用空间大小(byte)
   */
  size_t getUsedSpace() const;

  /**
   * @brief 计算当前数据占用系统页面的数量
   * @param pageSize 系统页面大小(byte)
   * @return 返回页面数量
   */
  size_t getUsedPages(unsigned int pageSize) const;

  /**
   * @brief 获取空闲空间大小
   */
  size_t getFreeSpace() const;

  /**
   * @brief 获取block对应的文件描述符
   */
  int getFd() const;

  /**
   * @brief 获取block对应的文件路径
   */
  const std::string &getFilePath() const;

  /**
   * @brief 返回block是否为空
   */
  bool isEmpty() const;

  /**
   * @brief 清空block
   */
  void clear();

  /**
   * @brief 将block数据写入文件
   * @param fd 写入文件的描述符
   * @param offset 写入偏移量
   * @param len 写入长度，默认为整个block的大小
   * @return 返回成功写入的长度
   */
  size_t writeOut(int fd, size_t offset = 0, size_t len = 0);

public:
  mmapBlock *prev; // block前驱指针
  mmapBlock *next; // block后继指针

private:
  char *data = nullptr; // block的数据块头指针
  int fd = -1;          // block对应的文件描述符
  std::string filePath; // block对应的文件路径

  size_t blockSize = 0; // block大小

  std::atomic_uint64_t usedSpace = 0;                // block被使用的空间
  std::atomic_flag fullFlag = ATOMIC_FLAG_INIT;      // block被使用的空间
  std::atomic_flag blockSpinLock = ATOMIC_FLAG_INIT; // 用于实现block的自旋锁
  std::shared_mutex mtx_writeOut;                    //控制缓冲区刷新
};

#endif