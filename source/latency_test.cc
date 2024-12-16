#include <string>
#include <iostream>
#include <fstream>
#include <chrono>
#include <vector>
#include <algorithm>
#include <iomanip>
#include "kv_engine.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"

#define NUM_CNS 16

std::vector<double> alloc_time[NUM_CNS];
std::vector<double> free_time[NUM_CNS];
double total_time[NUM_CNS] = {1.0};

double calculateP95(std::vector<double>& arr) {
    int n = arr.size();
    
    if (n == 0) return -1;  
    
    // 计算P99的索引
    int index = static_cast<int>((n - 1) * 0.95);
    
    // 使用nth_element找到P99位置的元素
    std::nth_element(arr.begin(), arr.begin() + index, arr.end());
    
    return arr[index];
}

void throughput_test(kv::LocalEngine *kv_imp, const int id) {
  uint64_t count = 0;
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  std::chrono::duration<double, std::micro> d;
  std::chrono::duration<double> duration;
  auto start = std::chrono::high_resolution_clock::now();
  int ret = 0;
  uint64_t raddr;

  while(count < 5000000) {
    auto start_alloc = std::chrono::high_resolution_clock::now();
    //ret = kv_imp->allocate_remote_page(raddr);
    ret = kv_imp->allocate_remote_page_batch(remote_addrs, 16);
    if(ret) {
      std::cout << "allocate_remote_page_batch fail. count = " << count << " , ret = " << ret << std::endl;
      return;
    }
    auto end_alloc = std::chrono::high_resolution_clock::now();
    d = end_alloc - start_alloc;
    alloc_time[id].push_back(d.count());

    auto start_free = std::chrono::high_resolution_clock::now();
    //ret = kv_imp->free_remote_page(raddr);
    ret = kv_imp->free_remote_page_batch(remote_addrs, 16);
    if(ret) {
      std::cout << "free_remote_page_batch fail." << std::endl;
      return;
    }
    auto end_free = std::chrono::high_resolution_clock::now();
    d = end_free - start_free;
    free_time[id].push_back(d.count());

    count+=2;
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  duration = end - start;

  total_time[id] = duration.count();

  return;
}

int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  //const uint64_t interval = atoi(argv[3]);

  kv::LocalEngine *kv_imps[NUM_CNS];
  std::thread* ths[NUM_CNS];

  for(int i = 0;i < NUM_CNS; ++i) {
    kv_imps[i] = new kv::LocalEngine();
    assert(kv_imps[i]);
    kv_imps[i]->start(rdma_addr, rdma_port);
  }

  for(int i = 0;i < NUM_CNS; ++i) {
    ths[i] = new std::thread(&throughput_test, kv_imps[i], i);
  }

  for(int i = 0;i < NUM_CNS; ++i) {
    ths[i]->join();
  }

  double throughput_total = 0.0;

  for(int i = 0; i < NUM_CNS; ++i) {
    std::cout << "thread id: " << i << std::endl;
    std::cout << "tail latency alloc: " << calculateP95(alloc_time[i]) << "us" << std::endl;
    std::cout << "tail latency free: " << calculateP95(free_time[i]) << "us" << std::endl;
    double throughput = 5/total_time[i];
    throughput_total += throughput;
    std::cout << "throughput: " << throughput << "MOP/s" << std::endl;
  }

  std::ofstream outfile("/mydata/latency_batch.txt");

  if (!outfile.is_open()) {
    std::cerr << "Error: Could not open file " << "/mydata/latency.txt" << " for writing." << std::endl;
    return 0;
  }

  for (const auto& value : alloc_time[0]) {
    outfile << std::setprecision(2) << std::fixed << value << std::endl;
  }

  outfile.close();
  return 0;
}