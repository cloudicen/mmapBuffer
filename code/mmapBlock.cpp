#include "mmapBlock.h"

void mmapBlock::MapRegion(int fd, uint64_t file_offset, char *&base, size_t map_size)
{
    void *ptr = mmap(nullptr, map_size, PROT_READ | PROT_WRITE,
                     MAP_SHARED,
                     fd,
                     file_offset);
    if (unlikely(ptr == MAP_FAILED))
    {
        base = nullptr;
        return;
    }
    base = reinterpret_cast<char *>(ptr);
}

void mmapBlock::UnMapRegion(char *base, size_t map_size)
{
    munmap(base, map_size);
}

size_t mmapBlock::append(const char *_data, size_t len)
{
    if (blockSize - usedSpace < len)
    {
        return 0;
    }
    else
    {
        memcpy(data + usedSpace, _data, len);
        usedSpace += len;
        if (usedSpace == blockSize)
        {
            blockStatus = full;
        }
        return len;
    }
}

bool mmapBlock::isValid()
{
    return fd != -1 && data != nullptr;
}

size_t mmapBlock::getUsedSpace() const
{
    return usedSpace;
}

size_t mmapBlock::getUsedPages(unsigned int pageSize) const
{
    return (usedSpace % pageSize) == 0 ? (usedSpace / pageSize) : (usedSpace / pageSize) + 1;
}

size_t mmapBlock::getFreeSpace() const
{
    return blockSize - usedSpace;
}

int mmapBlock::getFd() const
{
    return fd;
}

const std::string &mmapBlock::getFilePath() const
{
    return filePath;
}

bool mmapBlock::isEmpty() const
{
    return 0 == usedSpace && blockStatus == free;
}

void mmapBlock::clear()
{
    usedSpace = 0;
    blockStatus = free;
}

void mmapBlock::setFullFlag()
{
    blockStatus = full;
}

size_t mmapBlock::writeOut(int fd, size_t offset, size_t len)
{
    assert(fd);
    if (len == 0)
    {
        len = blockSize;
    }
    size_t writeLen = pwrite64(fd, data, len, offset);
    //size_t writeLen = write(fd, data, len);
    return writeLen;
}