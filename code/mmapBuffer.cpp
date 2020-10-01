#include "mmapBuffer.h"

std::mutex mmapBuffer::instenceMapMutex;

std::map<std::string, std::unique_ptr<mmapBuffer>> mmapBuffer::bufferInstances;

bool mmapBuffer::addBufferBlock(const std::string &_filePath, size_t _blockSize, mmapBlock *_insertCur)
{
    //该函数只会被成员函数调用，使用递归锁来保护
    std::lock_guard<std::recursive_mutex> bufferLock(recursive_bufferMutex);
    if (blockCount + 1 <= maxBlockCount)
    {
        //head = nullptr,初始化block
        if (head == nullptr)
        {
            head = new mmapBlock(_filePath, _blockSize, nullptr, nullptr);
            head->prev = head;
            head->next = head;
        }
        else
        {
            _insertCur->next = new mmapBlock(_filePath, _blockSize, _insertCur, _insertCur->next);
        }
        blockCount++;
        return true;
    }
    else
    {
        return false;
    }
}

void mmapBuffer::removeBufferBlock(mmapBlock *block)
{
    //该函数只会被成员函数调用，使用递归锁来保护
    std::lock_guard<std::recursive_mutex> bufferLock(recursive_bufferMutex);
    if (block != nullptr)
    {
        block->prev->next = block->next;
        close(block->getFd());
        remove(block->getFilePath().c_str());
        delete block;
        blockCount--;
    }
}

void mmapBuffer::initBuffer(const std::string &_persistFilePath, const std::string &_batchFileBasePath, size_t _maxBlockCount, size_t _blockCount, size_t _blockSize, unsigned int _persistTimeOut, unsigned int _systemPageSize)
{
    //全局只初始化buffer一次，之后可通过其他方法更改相关配置参数
    if (initFlag.test_and_set())
    {
        return;
    }
    persistenceFilePath = _persistFilePath;
    bufferFileBasePath = _batchFileBasePath;
    maxBlockCount = _maxBlockCount;
    blockSize = _blockSize;
    persistenceTimeOut = _persistTimeOut;
    pageSize = _systemPageSize;

    //初始化持久化写入文件
    persistenceFileFd = ::open(_persistFilePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0645);
    assert(persistenceFileFd >= 0);

    //初始化缓存block
    size_t initBlockCount = _blockCount;
    for (size_t i = 0; i < initBlockCount; i++)
    {
        std::string newFilePath = bufferFileBasePath + std::to_string(i);
        addBufferBlock(newFilePath, _blockSize, head);
    }

    //初始化写入指针和持久化指针
    writeCur = head;
    persistenceCur = head;

    std::thread persistWorkThread([&] { mmapBuffer::getBufferInstance(bufferName)->persist(); });
    //持久化线程转为后台线程，这里使用detach。当主线程终止，后台线程也会进入终止态并立即被销毁。
    persistWorkThread.detach();
}

void mmapBuffer::changePersistFile(const std::string &_persistenceFilePath)
{
    waitForBufferPersist();

    close(persistenceFileFd);
    remove(persistenceFilePath.c_str());
    persistenceFilePath = _persistenceFilePath;

    persistenceFileFd = ::open(persistenceFilePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0645);
    assert(persistenceFileFd >= 0);
}

void mmapBuffer::persist()
{
    using namespace std::chrono_literals;
    while (true)
    {
        assert(persistenceFileFd >= 0);

        //涉及条件变量，使用互斥锁保护
        std::unique_lock<std::mutex> lock(bufferMutex);

        if (persistenceCur->blockStatus == mmapBlock::status::free)
        {
            //当持久化区块未满时，写入指针和持久化指针应该指向同一个区块
            assert(writeCur == persistenceCur);

            //线程等待缓存区满，若等待超时，则进一步判断强制写入标志位
            blockIsFull.wait_for(lock, std::chrono_literals::operator""ms(persistenceTimeOut), [&] { return persistenceCur->blockStatus == mmapBlock::status::full; });
        }

        //整个缓冲区为空，直接返回
        if (persistenceCur->isEmpty())
        {
            bufferEmpty = true;
            lock.unlock();
            //发送信号
            bufferIsEmpty.notify_all();
            blockPersistDone.notify_all();
            continue;
        }

        //若缓存区满，则开始持久化
        //若缓存区未满而执行强制持久化在mmap情景下效率降低，因为缓存不会因为程序崩溃而丢失，所以大可以等到缓存区块满了再进行持久化
        if (persistenceCur->blockStatus == mmapBlock::status::full)
        {
            size_t writeLen = 0;
            size_t actualLen = persistenceCur->getUsedSpace();

            //进行写入对齐，对齐到页面大小的整数倍
            writeLen = persistenceCur->getUsedPages(pageSize) * pageSize;

            //释放缓冲区锁，让写入线程可以在持久化过程中继续向缓冲区写入
            lock.unlock();

            //持久化数据
            persistenceCur->writeOut(persistenceFileFd, persistenceFileOffset, writeLen);

            //上锁，持久化完成之后需要对缓冲区的成员变量进行修改
            lock.lock();

            //更新持久化文件长度
            persistenceFileOffset += writeLen;
            actualDataLen += actualLen;

            //清空buffer block(状态置为free)
            persistenceCur->clear();

            //缓存持久化指针后移,若持久化指针和写指针相同，则说明当前缓存块是刚刚好满的状态，则不移动持久化指针
            if (persistenceCur != writeCur)
            {
                persistenceCur = persistenceCur->next;
            }

            lock.unlock();
            //发送持久化完成信号
            blockPersistDone.notify_all();
            continue;
        }

        //检测强制持久化标志位
        if (unlikely(forceWrite && persistenceCur->getUsedSpace() != 0))
        {
            //只有当缓冲区未满的时候才有可能调用强制持久化，此时写指针和持久化指针应指向同一block
            assert(writeCur == persistenceCur);
            persistenceCur->setFullFlag();

            size_t writeLen = 0;
            size_t actualLen = persistenceCur->getUsedSpace();

            //进行写入对齐，对齐到页面大小的整数倍
            writeLen = persistenceCur->getUsedPages(pageSize) * pageSize;

            //释放缓冲区锁，让写入线程可以在持久化过程中继续向缓冲区写入
            lock.unlock();

            //持久化数据
            persistenceCur->writeOut(persistenceFileFd, persistenceFileOffset, writeLen);

            //上锁，持久化完成之后需要对缓冲区的成员变量进行修改
            lock.lock();

            //更新持久化文件长度,这里不计入写入对齐时候的补足长度
            persistenceFileOffset += writeLen;
            actualDataLen += actualLen;

            //清空buffer block(状态置为free)
            persistenceCur->clear();

            //重置强制持久化标志位
            forceWrite = false;

            //发送持久化完成信号
            blockPersistDone.notify_all();
        }
    }
}

void mmapBuffer::waitForBufferPersist()
{
    //获取整体的缓存锁，涉及条件变量，使用互斥锁保护
    std::unique_lock<std::mutex> lock(bufferMutex);
    //设定强制写入标志位
    forceWrite = true;
    //等待持久化完成
    bufferIsEmpty.wait(lock, [&] { return bufferEmpty; });
}

bool mmapBuffer::try_append(char *data, size_t len, bool noLose)
{
    //获取整体的缓存锁，使用了条件变量，故应该使用互斥锁
    std::unique_lock<std::mutex> lock(bufferMutex);

    //存在写入动作，设置缓冲区空标志位为false
    bufferEmpty = false;

    //写入缓存块，如果写入长度不符，则说明缓存块已满
    if (writeCur->append(data, len) != len)
    {
        //下一个block为free，转移到下一个block
        writeCur->setFullFlag();
        if (writeCur->next->blockStatus == mmapBlock::status::free)
        {
            writeCur = writeCur->next;
            writeCur->append(data, len);
        }
        else
        {
            //判断是否有空余空间，有则插入新的缓存区块，否则丢弃日志or等待持久化
            if (blockCount + 1 <= maxBlockCount)
            {
                std::string newFilePath = bufferFileBasePath + std::to_string(blockCount);
                addBufferBlock(newFilePath, blockSize, writeCur);
                writeCur = writeCur->next;
                writeCur->append(data, len);
            }
            else
            {
                //no lose模式开启，不会丢弃日志，而是等待
                if (noLose)
                {
                    //等待缓存持久化，这里满足下一个缓存块有空间可写或者当前缓存块有空间可写就行
                    //当前缓存块有空间可写的情况发生在同时有多个线程在阻塞等待，前n个阻塞解除时，目前写指针所在缓存块未满
                    //则当前线程直接在当前缓存块写入
                    blockPersistDone.wait(lock, [&] { return writeCur->next->isEmpty() || writeCur->getFreeSpace() > len; });
                    if (writeCur->getFreeSpace() < len)
                    {
                        writeCur = writeCur->next;
                    }
                    writeCur->append(data, len);
                    return true;
                }
                else
                {
                    //丢弃日志
                    return false;
                }
            }
        }
        //当前线程把缓存区写满了，释放整体缓存区锁并通知持久化线程
        lock.unlock();
        //只有一个线程（持久化线程）会使用该条件变量，故使用notify_one即可
        blockIsFull.notify_one();
    }
    return true;
}

size_t mmapBuffer::getPersistenceFileLen() const
{
    return persistenceFileOffset;
}

size_t mmapBuffer::getActualDataLen() const
{
    return actualDataLen;
}