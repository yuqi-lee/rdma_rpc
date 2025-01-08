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


std::vector<double>* alloc_time;
std::vector<double>* free_time;
double* total_time;

double calculateP95(std::vector<double>& arr) {
    int n = arr.size();
    
    if (n == 0) return -1;  
    
    // 计算P99的索引
    int index = static_cast<int>((n - 1) * 0.95);
    
    // 使用nth_element找到P99位置的元素
    std::nth_element(arr.begin(), arr.begin() + index, arr.end());
    
    return arr[index];
}

double calculateAVG(std::vector<double>& arr) {
    double sum = 0.0;
    
    if (arr.size() == 0) return -1;  
    
    for(auto latency : arr) {
      sum += latency;
    }
    
    return sum/arr.size();
}

void throughput_test(kv::LocalEngine *kv_imp, const int id, const bool use_batch) {
  uint64_t count = 0;
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  std::chrono::duration<double, std::micro> d;
  std::chrono::duration<double> duration;
  auto start = std::chrono::high_resolution_clock::now();
  int ret = 0;
  uint64_t raddr;

  while(count < 5000000) {
    auto start_alloc = std::chrono::high_resolution_clock::now();
    if(use_batch)
      ret = kv_imp->allocate_remote_page_batch(remote_addrs, 128);
    else
      ret = kv_imp->allocate_remote_page(raddr);
    if(ret) {
      std::cout << "allocate_remote_page_batch fail. count = " << count << " , ret = " << ret << std::endl;
      return;
    }
    auto end_alloc = std::chrono::high_resolution_clock::now();
    d = end_alloc - start_alloc;
    alloc_time[id].push_back(d.count());

    auto start_free = std::chrono::high_resolution_clock::now();
    if(use_batch)
      ret = kv_imp->free_remote_page_batch(remote_addrs, 128);
    else
      ret = kv_imp->free_remote_page(raddr);
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
  if(argc != 5) {
    std::cerr << "Usage: ./latency_test <server_ip> <server_port> <num_threads> <use_batch> " << std::endl;
  }
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  const uint64_t num_threads = atoi(argv[3]);
  const bool use_batch = atoi(argv[4]);

  alloc_time = new std::vector<double>[num_threads];
  free_time = new std::vector<double>[num_threads];
  total_time = new double[num_threads];

  kv::LocalEngine** kv_imps;
  std::thread** ths;

  kv_imps = new kv::LocalEngine*[num_threads];
  ths = new std::thread*[num_threads];

  for(int i = 0;i < num_threads; ++i) {
    kv_imps[i] = new kv::LocalEngine();
    assert(kv_imps[i]);
    kv_imps[i]->start(rdma_addr, rdma_port);
  }

  for(int i = 0;i < num_threads; ++i) {
    ths[i] = new std::thread(&throughput_test, kv_imps[i], i, use_batch);
  }

  for(int i = 0;i < num_threads; ++i) {
    ths[i]->join();
  }

  double throughput_total = 0.0;

  for(int i = 0; i < num_threads; ++i) {
    std::cout << "thread id: " << i << std::endl;
    std::cout << "tail latency alloc: " << calculateP95(alloc_time[i]) << "us" << std::endl;
    std::cout << "tail latency free: " << calculateP95(free_time[i]) << "us" << std::endl;
    std::cout << "avg latency alloc: " << calculateAVG(alloc_time[i]) << "us" << std::endl;
    std::cout << "avg latency free: " << calculateAVG(free_time[i]) << "us" << std::endl;
    double throughput = 5/total_time[i];
    throughput_total += throughput;
    std::cout << "throughput: " << throughput << "MOP/s" << std::endl;
  }

  std::string file_name;
  if(use_batch) {
    file_name = std::string("/mydata/latency_batch_" + std::to_string(num_threads) + ".txt");
  } else {
    file_name = std::string("/mydata/latency_single_" + std::to_string(num_threads) + ".txt");
  }

  std::ofstream outfile(file_name);

  if (!outfile.is_open()) {
    std::cerr << "Error: Could not open file " << file_name << " for writing." << std::endl;
    return 0;
  }

  for (const auto& value : alloc_time[0]) {
    outfile << std::setprecision(2) << std::fixed << value << std::endl;
  }

  outfile.close();
  return 0;
}