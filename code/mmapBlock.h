#ifndef __MMAPBLOCK__
#define __MMAPBLOCK__
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <iostream>
#include <functional>
#include <chrono>
#include <assert.h>
#include <condition_variable>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

class mmapBlock
{
private:
    //block的数据块头指针
    char *data = nullptr;
    //block对应的文件描述符
    int fd = -1;
    //block对应的文件路径
    std::string filePath;
    //block大小
    size_t blockSize = 0;
    //block被使用的空间
    size_t usedSpace = 0;

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

    /**
     * @brief 表示block的状态
     */
    enum status
    {
        free,
        full
    };

    //block状态
    status blockStatus = free;

public:
    //block前驱指针
    mmapBlock *prev;
    //block后继指针
    mmapBlock *next;

    /**
     * @brief 内存映射缓存块构造函数
     * @param _filePath 对应文件路径
     * @param _blockSize 缓存块大小
     * @param _prev 缓存块前驱指针
     * @param _next 缓存块后继指针
     */
    mmapBlock(const std::string &_filePath, size_t _blockSize, mmapBlock *_prev = nullptr, mmapBlock *_next = nullptr) : filePath(_filePath), blockSize(_blockSize), prev(_prev), next(_next)
    {
        fd = open(filePath.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0645);
        if (fd > 0)
        {
            if (posix_fallocate(fd, 0, blockSize) == 0)
            {
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
    ~mmapBlock()
    {
        if (data != nullptr)
        {
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
    size_t append(const char *_data, size_t len);

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
     * @brief 设置block状态标志为free
     */
    void mmapBlock::setFreeFlag();

    /**
     * @brief 设置block状态标志为full
     */
    void setFullFlag();

    /**
     * @brief 检查block状态标志是否为full
     */
    bool testFullFlag();

    /**
     * @brief 将block数据写入文件
     * @param fd 写入文件的描述符
     * @param offset 写入偏移量
     * @param len 写入长度，默认为整个block的大小
     * @return 返回成功写入的长度
     */
    size_t writeOut(int fd, size_t offset = 0, size_t len = 0);
};

#endif