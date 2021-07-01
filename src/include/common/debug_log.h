//
// Created by Ning Wang on 2021/7/1.
//

#ifndef BUSTUB_DEBUG_LOG_H
#define BUSTUB_DEBUG_LOG_H
#include <atomic>
#include <map>
#include <vector>
namespace bustub {
class ThreadLog {
 public:
  ThreadLog(int bufferNumber) : buffer_number(bufferNumber) {
    buffer = new char *[bufferNumber];
    for (int i = 0; i < bufferNumber; ++i) {
      buffer[i] = new char[65535];
    }
  }

  //  void flush();
  virtual ~ThreadLog() {
    for (int i = 0; i < buffer_number; ++i) {
      delete[] buffer[i];
    }
  }

  void flush() {
    //    flush to std out
    // order by logic order id
  }
  inline char *getLogPosition() { return nullptr; }
  void updatePosition(char *position);
  int getOrderId(char *position) {
    //    todo
    return 0;
  }

 private:
  std::map<int, char *> thread_id_to_buffer_position;
  int buffer_number;
  char **buffer;
  std::atomic<int> order_id;
};
}  // namespace bustub

#endif  // BUSTUB_DEBUG_LOG_H
