#ifndef __MMAPBUFFER__
#define __MMAPBUFFER__

#include "mmapBlock.h"
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

class mmapBuffer {
private:
  //全局构造锁
  static std::mutex instenceMapMutex;

  //全局buffer实例Map
  static std::unordered_map<std::string, std::shared_ptr<mmapBuffer>>
      bufferInstances;

  //实例初始化标志位
  bool initFlag = false;

  // buffer的标识名称
  std::string bufferName;

  //整体buffer的互斥锁
  std::mutex bufferMutex;

  std::mutex writeCur_mtx;
  std::mutex persistCur_mtx;

  std::condition_variable writeCur_cv;

  //强制持久化标志位
  bool forcePersist = false;
  //整体buffer为空标志位
  bool bufferEmpty = false;
  //允许写入标志位
  bool enableWrite = true;

  // buffer block 已满的条件变量
  std::condition_variable blockIsFull;
  // buffer block 持久化完成的条件变量
  std::condition_variable blockPersistenceDone;
  //整体buffer空的条件变量
  std::condition_variable bufferIsEmpty;
  //允许写入标志位发生变更的条件变量
  std::condition_variable enableWriteFlagChanged;

  //缓存块头部指针
  mmapBlock *head = nullptr;
  //缓存块写指针
  mmapBlock *writeCur = nullptr;
  //缓存块持久化指针
  mmapBlock *persistenceCur = nullptr;

  //当前缓存块数量
  size_t blockCount = 0;
  //最大缓存块数量
  size_t maxBlockCount = 0;
  //缓存块大小
  size_t blockSize = 0;
  //系统页面大小
  size_t systemPageSize = 4096;

  //持久化数据大小计数，在使用pwrite()的时候可以作为参数传入
  size_t persistenceFileOffset = 0;

  //实际的数据长度，不计页面对齐时的补足字节
  size_t actualDataLen = 0;

  //持久化等待超时，即等待缓冲区满的时间间隔
  unsigned int persistenceWaitTimeOut = 0;

  //持久化文件路径
  std::string persistenceFilePath = "";
  //持久化文件标识符
  int persistenceFileFd = 0;
  // mmap临时文件的基础文件名，新建的文件会在后面跟上编号（从0开始）
  std::string bufferFileBasePath = "";

  /**
   * @brief
   * 添加一个缓存块，会进行缓存块限制检查和缓存块初始化检查（即头部指针为空的情况）
   * @param _mmapFd 打开的缓存块文件标识符
   * @param _filePath 缓存块文件路径
   * @param _blockSize 缓存块大小
   * @param _insertCur 在该指针指向的缓存块后插入新缓存块
   * @return 操作成功返回true
   */
  bool addBufferBlock(const std::string &_filePath, size_t _blockSize,
                      mmapBlock *_insertCur);

  /**
   * @brief 删除一个缓存块，会同时删除被映射的文件
   * @param block 要删除的缓存块指针
   * @return none
   */
  void removeBufferBlock(mmapBlock *block);

  /**
   * @brief 执行数据持久化逻辑
   */
  void persist();

  /**
   * @brief 受保护的默认构造函数，防止在程序的其他位置被构造
   */
  mmapBuffer(){};

  //删除复制构造函数
  mmapBuffer(const mmapBuffer &) = delete;
  //删除赋值运算符重载
  mmapBuffer &operator=(const mmapBuffer &) = delete;

public:
  /**
   * @brief 获取缓存实例指针，若缓存实例名称不存在，则创建
   * @param _bufferName 缓存实例的名称
   * @return 缓存实例指针
   */
  static std::shared_ptr<mmapBuffer> &
  getBufferInstance(const std::string &_bufferName) {
    std::unique_lock<std::mutex> lock(instenceMapMutex);
    const auto [ins, success] = bufferInstances.emplace(
        _bufferName, std::shared_ptr<mmapBuffer>(new mmapBuffer));
    ins->second->bufferName = _bufferName;
    return ins->second;
  };

  /**
   * @brief 从map中移除指定的缓存，释放缓存所有资源
   * @param _bufferName 缓存实例的名称
   */
  static void removeBufferInstance(const std::string &_bufferName) {
    std::unique_lock<std::mutex> lock(instenceMapMutex);
    bufferInstances.erase(_bufferName);
  }

  /**
   * @brief 初始化缓冲区的各项参数
   * @param _persistenceFilePath 持久化文件的路径
   * @param _bufferFileBasePath
   * buffer文件的基本路径，多个buffer会自动在基本路径后添加编号
   * @param _maxBlockCount 最大缓存块数量
   * @param _blockCount 初始缓存块数量
   * @param _blockSize 单个缓存块大小
   * @param _persistenceTimeOut 持久化等待超时(ms)
   * @param _systemPageSize 系统页面大小(bytes)默认4k
   * @return none
   */
  void
  initBuffer(const std::string &_persistenceFilePath = std::string("data"),
             const std::string &_bufferFileBasePath = std::string("buffer"),
             size_t _maxBlockCount = 50, size_t _blockCount = 2,
             size_t _blockSize = 4096 * 100000,
             unsigned int _persistenceTimeOut = 10,
             unsigned int _systemPageSize = 4096);

  /**
   * @brief 更改持久化写入文件
   * @param _persistenceFilePath 新文件的路径
   */
  void changePersistFile(const std::string &_persistenceFilePath);

  /**
   * @brief 阻塞等待缓冲区的所有内容被持久化到硬盘
   */
  void waitForBufferPersist();

  /**
   * @brief 析构时释放所有block，关闭持久化文件和临时文件，并删除临时文件
   */
  ~mmapBuffer() {
    if (head != nullptr) {
      waitForBufferPersist();
      for (size_t i = 0; i < blockCount - 1; i++) {
        mmapBlock *next = head->next;
        delete head;
        head = next;
      }
      delete head;
      close(persistenceFileFd);
    }
  }

  /**
   * @brief 写入缓存
   * @param data 写入数据的指针
   * @param len 写入长度
   * @param noLose true：缓存区满时阻塞等待，false：缓存区满丢弃数据，默认丢弃
   * @return 写入成功返回true
   */
  bool try_append(char *data, size_t len, bool noLose = false);

  /**
   * @brief 获取目前已经写入的持久化文件的大小
   * @note 该函数并非线程安全，数据读取时不加锁
   */
  size_t getPersistenceFileLen() const;

  /**
   * @brief 获取目前写入的实际数据的长度（不计入页对齐补足的长度）
   * @note 该函数并非线程安全，数据读取时不加锁
   */
  size_t getActualDataLen() const;
};

#endif