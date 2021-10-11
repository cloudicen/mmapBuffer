#include "../code/mmapBuffer.h"
#include "chrono"
#include <ctime>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <thread>
#include <vector>

#define LOGS_PER_THREAD 10000000UL
#define THREAD_COUNT 5
#define LOG_SIZE 103

time_t rawtime;
time_t last_sec;
time_t last_min;
struct timeval curTime;
char timeBuf[50];

void updateTime() {
  gettimeofday(&curTime, NULL);
  if (last_sec == curTime.tv_sec) {
    //同一秒内的日志
  } else if (curTime.tv_sec / 60 == last_min) {
    //同一分钟的日志
    auto sec = curTime.tv_sec % 60;
    timeBuf[17] = sec / 10 + '0';
    timeBuf[18] = sec % 10 + '0';
  } else {
    time(&rawtime);
    std::strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%d %H:%M:%S",
                  std::localtime(&rawtime));
  }
  last_sec = curTime.tv_sec;
}

void writeToBuffer(char *data, size_t len, int index) {
  auto ins = mmapBuffer::getBufferInstance("TEST");
  std::time_t time;
  char data_[LOG_SIZE] = {'t', 'e', 's', 't', '\n'};
  std::stringstream tag;
  auto i = 0UL;
  tag << " [" << index << "] ";
  memcpy(data_ + 19, tag.str().c_str(), 5);
  data_[LOG_SIZE - 1] = '\n';
  for (; i < LOGS_PER_THREAD; i++) {
    updateTime();
    memcpy(data_, timeBuf, 19);
    ins->try_append(data_, len, true);
  }
  std::cout << "worker : " << index << "finished!\n";
}

int main() {
  static char data[LOG_SIZE] = {'t', 'e', 's', 't', '\n'};
  mmapBuffer::getBufferInstance("TEST")->initBuffer();
  auto start = std::chrono::steady_clock::now();
  std::vector<std::thread> threads;
  for (int i = 0; i < THREAD_COUNT; i++) {
    std::thread worker(writeToBuffer, data, LOG_SIZE, i);
    threads.push_back(std::move(worker));
  }
  for (int i = 0; i < THREAD_COUNT; i++) {
    threads[i].join();
  }
  // for (int i = 0; i < THREAD_COUNT; i++)
  { mmapBuffer::getBufferInstance("TEST")->waitForBufferPersist(); }

  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> elapsed_seconds =
      std::chrono::duration<double>(end - start);

  std::cout << "elapsed time: " << elapsed_seconds.count() << "s\n";
  std::cout << "totoal logs : " << LOGS_PER_THREAD * THREAD_COUNT << "\n";
  std::cout << "log data size: " << LOGS_PER_THREAD * THREAD_COUNT * LOG_SIZE
            << " bytes\n";
  std::cout << "actual data write size: "
            << mmapBuffer::getBufferInstance("TEST")->getActualDataLen()
            << " bytes\n";
  std::cout << "actual file size: "
            << mmapBuffer::getBufferInstance("TEST")->getPersistenceFileLen()
            << " bytes\n";
}