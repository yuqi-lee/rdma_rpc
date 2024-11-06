#include <string>
#include <iostream>
#include <chrono>
#include "kv_engine.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"

const uint64_t interval = 100000;

int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  
  kv::LocalEngine *kv_imp = new kv::LocalEngine();
  assert(kv_imp);
  kv_imp->start(rdma_addr, rdma_port);
  uint64_t addr;
  uint64_t count = 0;
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  /*
  do {
    count++;
    if(count % interval == 0) {
      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::micro> duration = end - start;
      auto throughput = interval / duration.count();
      std::cout << "current throughput is " << throughput << std::endl;
    }
    kv_imp->allocate_remote_page(addr);
    
  } while (kv_imp->alive());*/

  do {
    count++;
    start = std::chrono::high_resolution_clock::now();
    kv_imp->allocate_remote_page(addr);
    end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> duration = end - start;
    std::cout << "latency is " << duration.count() << std::endl;
  } while (count < 100);

  kv_imp->stop();
  delete kv_imp;

  return 0;
}