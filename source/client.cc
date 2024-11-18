#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <algorithm>
#include "kv_engine.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"

void throughput_test(kv::LocalEngine *kv_imp, 
                    const uint64_t interval) {
  //kv::LocalEngine *kv_imp = new kv::LocalEngine();
  //assert(kv_imp);
  //kv_imp->start(ip, port);
  uint64_t addr;
  //uint64_t count = 0;
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::micro> duration = end - start;
  double max_duratuon = 0.0;
  std::vector<double> res;
  uint64_t count = 0;
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  uint64_t remote_addr;

  while(count < 20) {
    count++;
    /*
    if(count % interval == 0) {
      end = std::chrono::high_resolution_clock::now();
      duration = end - start;
      auto throughput = 2 * interval / (duration.count()/1000000L);
      std::cout << "current throughput is " << throughput << ",  duration is :" << duration.count()/1000000L << "s" << std::endl;
      start = end;
    }*/
    start = std::chrono::high_resolution_clock::now();
    kv_imp->allocate_remote_page_batch(remote_addrs, 128);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "batch allocate latency is " << duration.count() << "us" << std::endl;

    start = std::chrono::high_resolution_clock::now();
    kv_imp->free_remote_page_batch(remote_addrs, 128);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "batch free latency is " << duration.count() << "us" << std::endl;

    start = std::chrono::high_resolution_clock::now();
    kv_imp->allocate_remote_page(remote_addr);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "allocate latency is " << duration.count() << "us" << std::endl;

    start = std::chrono::high_resolution_clock::now();
    kv_imp->free_remote_page(remote_addr);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "free latency is " << duration.count() << "us" << std::endl;
    
    //res.push_back(duration.count());
    //count++;
  }
  
  
  //std::cout << "max duration is " << max_duratuon << std::endl;
  //sort(res.begin(), res.end());
  //auto it = res.rbegin();
  //for(int i = 0;i < 20; ++i) {
    //std::cout << "duration is " << *it << "us"<< std::endl;
    //it = next(it);
  //}

  kv_imp->stop();
  delete kv_imp;
}

int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  const uint64_t interval = atoi(argv[3]);

  kv::LocalEngine *kv_imp = new kv::LocalEngine();
  assert(kv_imp);
  kv_imp->start(rdma_addr, rdma_port);
  auto t  = new std::thread(&throughput_test, kv_imp, interval);
  t->join();

  return 0;
}