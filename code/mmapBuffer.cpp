#include "mmapBuffer.h"

std::mutex mmapBuffer::instenceMapMutex;

std::unordered_map<std::string, std::shared_ptr<mmapBuffer>>
    mmapBuffer::bufferInstances;

bool mmapBuffer::addBufferBlock(const std::string &_filePath, size_t _blockSize,
                                mmapBlock *_insertCur) {
  //该函数只会被成员函数调用，使用递归锁来保护
  std::lock_guard<std::recursive_mutex> bufferLock(recursive_bufferMutex);
  if (blockCount + 1 <= maxBlockCount) {
    // head = nullptr,初始化block
    if (head == nullptr) {
      head = new mmapBlock(_filePath, _blockSize, nullptr, nullptr);
      head->prev = head;
      head->next = head;
    } else {
      _insertCur->next =
          new mmapBlock(_filePath, _blockSize, _insertCur, _insertCur->next);
    }
    blockCount++;
    return true;
  } else {
    return false;
  }
}

void mmapBuffer::removeBufferBlock(mmapBlock *block) {
  //该函数只会被成员函数调用，使用递归锁来保护
  std::lock_guard<std::recursive_mutex> bufferLock(recursive_bufferMutex);
  if (block != nullptr) {
    block->prev->next = block->next;
    close(block->getFd());
    remove(block->getFilePath().c_str());
    delete block;
    blockCount--;
  }
}

void mmapBuffer::initBuffer(const std::string &_persistenceFilePath,
                            const std::string &_bufferFileBasePath,
                            size_t _maxBlockCount, size_t _blockCount,
                            size_t _blockSize,
                            unsigned int _persistenceWaitTimeOut,
                            unsigned int _systemPageSize) {
  std::unique_lock<std::mutex> lk(bufferMutex);
  //全局只初始化buffer一次，之后可通过其他方法更改相关配置参数
  if (initFlag) {
    return;
  }
  initFlag = true;

  persistenceFilePath = _persistenceFilePath;
  bufferFileBasePath = _bufferFileBasePath;
  maxBlockCount = _maxBlockCount;
  blockSize = _blockSize;
  persistenceWaitTimeOut = _persistenceWaitTimeOut;
  systemPageSize = _systemPageSize;

  //初始化持久化写入文件
  persistenceFileFd =
      ::open(_persistenceFilePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0645);
  assert(persistenceFileFd >= 0);

  //初始化缓存block
  size_t initBlockCount = _blockCount;
  for (size_t i = 0; i < initBlockCount; i++) {
    std::string newFilePath = bufferFileBasePath + std::to_string(i);
    addBufferBlock(newFilePath, _blockSize, head);
  }

  //初始化写入指针和持久化指针
  writeCur = head;
  persistenceCur = head;

  std::thread persistWorkThread(
      [&] { mmapBuffer::getBufferInstance(bufferName)->persist(); });
  //持久化线程转为后台线程，这里使用detach。当主线程终止，后台线程也会进入终止态并立即被销毁。
  persistWorkThread.detach();
}

void mmapBuffer::changePersistFile(const std::string &_persistenceFilePath) {
  //等待当前缓冲区数据全部持久化
  waitForBufferPersist();

  std::unique_lock<std::mutex> lock(bufferMutex);

  //关闭旧持久化文件
  close(persistenceFileFd);

  //打开新持久化文件
  persistenceFilePath = _persistenceFilePath;
  persistenceFileFd =
      ::open(persistenceFilePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0645);
  assert(persistenceFileFd >= 0);

  //重置文件长度信息
  persistenceFileOffset = 0;
  actualDataLen = 0;
}

void mmapBuffer::persist() {
  using namespace std::chrono_literals;
  while (true) {
    assert(persistenceFileFd >= 0);

    //涉及条件变量，使用互斥锁保护
    std::unique_lock<std::mutex> lock(persistCur_mtx);

    //整个缓冲区为空，直接返回
    if (persistenceCur->isEmpty()) {
      bufferEmpty = true;
      lock.unlock();
      //发送信号
      bufferIsEmpty.notify_all();
      blockPersistenceDone.notify_all();
      continue;
    }

    if (persistenceCur->getFreeSpace() > 0) {
      //当持久化区块未满时，写入指针和持久化指针应该指向同一个区块
      assert(writeCur == persistenceCur);

      //线程等待缓存区满，若等待超时，则进一步判断强制写入标志位
      blockIsFull.wait_for(
          lock, std::chrono_literals::operator""ms(persistenceWaitTimeOut),
          [&] { return persistenceCur->getFreeSpace() == 0; });
      if (persistenceCur->getFreeSpace() == 0) {
        printf("block is full\n");
      }
    }

    //若缓存区满，则开始持久化
    //若缓存区未满而执行强制持久化在mmap情景下效率降低，因为缓存不会因为程序崩溃而丢失，所以大可以等到缓存区块满了再进行持久化
    if (persistenceCur->getFreeSpace() == 0) {
      size_t writeLen = 0;
      size_t actualLen = persistenceCur->getUsedSpace();

      //进行写入对齐，对齐到页面大小的整数倍
      writeLen = blockSize;

      //持久化数据
      persistenceCur->writeOut(persistenceFileFd, persistenceFileOffset,
                               writeLen);

      //更新持久化文件长度
      persistenceFileOffset += writeLen;
      actualDataLen += actualLen;

      //清空buffer block(状态置为free)
      persistenceCur->clear();

      //缓存持久化指针后移,若持久化指针和写指针相同，则说明当前缓存块是刚刚好满的状态，则不移动持久化指针
      if (persistenceCur != writeCur) {
        persistenceCur = persistenceCur->next;
      }

      lock.unlock();
      //发送持久化完成信号
      blockPersistenceDone.notify_one();
      continue;
    }

    //检测强制持久化标志位
    if (unlikely(forcePersist && persistenceCur->getUsedSpace() != 0)) {
      //只有当缓冲区未满的时候才有可能调用强制持久化，此时写指针和持久化指针应指向同一block
      assert(writeCur == persistenceCur);
      persistenceCur->setFullFlag();

      printf("force persist\n");

      size_t writeLen = 0;
      size_t actualLen = persistenceCur->getUsedSpace();

      //进行写入对齐，对齐到页面大小的整数倍
      writeLen = persistenceCur->getUsedPages(systemPageSize) * systemPageSize;

      //持久化数据
      persistenceCur->writeOut(persistenceFileFd, persistenceFileOffset,
                               writeLen);

      //更新持久化文件长度,这里不计入写入对齐时候的补足长度
      persistenceFileOffset += writeLen;
      actualDataLen += actualLen;

      //清空buffer block(状态置为free)
      persistenceCur->clear();

      //重置强制持久化标志位
      forcePersist = false;

      //发送持久化完成信号
      blockPersistenceDone.notify_one();
    }
  }
}

void mmapBuffer::waitForBufferPersist() {
  //获取整体的缓存锁，涉及条件变量，使用互斥锁保护
  std::unique_lock<std::mutex> lock(persistCur_mtx);
  //设置缓存不可写
  enableWrite = false;
  //设定强制写入标志位
  forcePersist = true;
  //等待持久化完成
  bufferIsEmpty.wait(lock, [&] { return bufferEmpty; });
  //恢复缓存可写
  enableWrite = true;
  lock.unlock();
  enableWriteFlagChanged.notify_all();
}

bool mmapBuffer::try_append(char *data, size_t len, bool noLose) {
  // //获取整体的缓存锁，使用了条件变量，故应该使用互斥锁
  // std::unique_lock<std::mutex> lock(bufferMutex);

  // //等待缓存可写
  // enableWriteFlagChanged.wait(lock, [&] { return enableWrite; });

  // //存在写入动作，设置缓冲区空标志位为false
  bufferEmpty = false;

  //写入缓存块，如果写入长度不符，则说明缓存块已满
  auto [actualLen, isFull] = writeCur->append(data, len);

  if (actualLen == 0) {
    printf("缓冲区已经是满的状态，需要等待指针调整\n");
    //缓冲区已经是满的状态，需要等待指针调整
    //这里的等待队列是使用notify_one唤醒的
    std::unique_lock<std::mutex> lock(writeCur_mtx);
    while (writeCur->getFreeSpace() == 0) {
      writeCur_cv.wait(lock);
    }
    assert(writeCur->getFreeSpace() > 0);
    lock.unlock();
    try_append(data, len, noLose);
    writeCur_cv.notify_all(); //通知下一个正在等待的线程
  } else if (len != actualLen) {
    printf("仅写入了部分数据，长度%ld,进行指针调整\n", actualLen);
    //仅写入了部分数据，进行指针调整
    std::unique_lock<std::mutex> lock(writeCur_mtx);
    int remainLen = len - actualLen;
    int dataPos = actualLen;
    while (1) {
      if (writeCur->getFreeSpace() > 0) {
        auto [writeLen, isFull] = writeCur->append(data + dataPos, remainLen);
        if (writeLen == remainLen && !isFull) {
          //直接写入完成，并且当前缓冲区不满
          printf("调整完成，且当前缓冲区不满\n");
          lock.unlock();
          writeCur_cv.notify_all(); //通知正在等待的线程
          return true;
        } else if (writeLen == remainLen && isFull) {
          printf("调整完成，当前缓冲区已满\n");
          //直接写入完成，但当前缓冲区已满
          if (writeCur->next->isEmpty()) {
            writeCur = writeCur->next; //下一个缓冲区可用，直接移动指针
          } else {
            if (blockCount + 1 <= maxBlockCount) { //可添加新缓冲区
              std::string newFilePath =
                  bufferFileBasePath + std::to_string(blockCount);
              addBufferBlock(newFilePath, blockSize, writeCur);
              writeCur = writeCur->next;
            } else { //无法添加更多的缓冲区，需要等待
              std::unique_lock<std::mutex> persistLock(persistCur_mtx);
              blockPersistenceDone.wait(
                  persistLock, [&] { return writeCur->next->isEmpty(); });
              writeCur = writeCur->next;
              assert(writeCur->isEmpty());
            }
          }
          lock.unlock();
          blockIsFull.notify_one();
          writeCur_cv.notify_all(); //通知一个正在等待的线程
          return true;
        } else { //写入未完成，需要更新位置参数，继续写入
          printf("调整未完成，当前缓冲区已满\n");
          remainLen = remainLen - writeLen;
          dataPos += writeLen;
          blockIsFull.notify_one();
          continue;
        }
      } else if (writeCur->next->isEmpty()) { //下一个缓冲区可用
        printf("调整中，下一个缓冲区可用\n");
        writeCur = writeCur->next;
        assert(writeCur->isEmpty());
        continue;
      } else {
        if (blockCount + 1 <= maxBlockCount) { //可添加新缓冲区
          printf("调整中，可添加新缓冲区\n");
          std::string newFilePath =
              bufferFileBasePath + std::to_string(blockCount);
          addBufferBlock(newFilePath, blockSize, writeCur);
          writeCur = writeCur->next;
          assert(writeCur->isEmpty());
          continue;
        } else { //无法添加更多的缓冲区，需要等待
          printf("调整中，无法添加更多的缓冲区，需要等待\n");
          std::unique_lock<std::mutex> persistLock(persistCur_mtx);
          blockPersistenceDone.wait(persistLock,
                                    [&] { return writeCur->next->isEmpty(); });
          writeCur = writeCur->next;
          assert(writeCur->isEmpty());
          continue;
        }
      }
    }
  } else if (len == actualLen && isFull) {
    printf("无冲突正常写入,刚好把缓存区填满\n");
    //无冲突正常写入,刚好把缓存区填满
    std::unique_lock<std::mutex> lock(writeCur_mtx);
    if (writeCur->next->isEmpty()) { //下一个缓冲区可用
      writeCur = writeCur->next;
      assert(writeCur->isEmpty());
    } else {
      if (blockCount + 1 <= maxBlockCount) { //可添加新缓冲区
        std::string newFilePath =
            bufferFileBasePath + std::to_string(blockCount);
        addBufferBlock(newFilePath, blockSize, writeCur);
        writeCur = writeCur->next;
        assert(writeCur->isEmpty());
      } else { //无法添加更多的缓冲区，需要等待
        std::unique_lock<std::mutex> persistLock(persistCur_mtx);
        blockPersistenceDone.wait(persistLock,
                                  [&] { return writeCur->next->isEmpty(); });
        writeCur = writeCur->next;
        assert(writeCur->isEmpty());
      }
    }
    lock.unlock();
    blockIsFull.notify_one();
    writeCur_cv.notify_all(); //通知一个正在等待的线程
    return true;
  } else {
    //无冲突正常写入，且缓冲区不满
    return true;
  }
  return true;
}

size_t mmapBuffer::getPersistenceFileLen() const {
  return persistenceFileOffset;
}

size_t mmapBuffer::getActualDataLen() const { return actualDataLen; }