#include "../code/mmapBuffer.h"

#define LOGS_PER_THREAD 10000000UL
#define THREAD_COUNT 5
#define LOG_SIZE 100
void writeToBuffer(char *data, size_t len, int index)
{
    auto ins = mmapBuffer::getBufferInstance("TEST");
    for (auto i = 0UL; i < LOGS_PER_THREAD; i++)
    {
        ins->try_append(data, len, true);
    }
    std::cout << "worker : " << index << "finished!\n";
}

int main()
{
    static char data[LOG_SIZE] = {'t', 'e', 's', 't', '\n'};
    mmapBuffer::getBufferInstance("TEST")->initBuffer();
    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        std::thread worker(writeToBuffer, data, LOG_SIZE, i);
        threads.push_back(std::move(worker));
    }
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        threads[i].join();
    }
    //for (int i = 0; i < THREAD_COUNT; i++)
    {
        mmapBuffer::getBufferInstance("TEST")->waitForBufferPersist();
    }

    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = std::chrono::duration<double>(end - start);

    std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";
    std::cout << "totoal logs : " << LOGS_PER_THREAD * THREAD_COUNT << "\n";
    std::cout << "log data size: " << LOGS_PER_THREAD * THREAD_COUNT * LOG_SIZE << " bytes\n";
    std::cout << "actual data write size: " << mmapBuffer::getBufferInstance("TEST")->getActualDataLen() << " bytes\n";
    std::cout << "actual file size: " << mmapBuffer::getBufferInstance("TEST")->getPersistenceFileLen() << " bytes\n";
    /*
    for (int i = 0; i < THREAD_COUNT; i++)
    {
        std::cout << "worker " << i << ", actual data write size: " << mmapBuffer::getBufferInstance(std::to_string(i))->getActualDataLen() << " bytes\n";
        std::cout << "worker " << i << ",actual file size: " << mmapBuffer::getBufferInstance(std::to_string(i))->getPersistenceFileLen() << " bytes\n";
    }*/
}