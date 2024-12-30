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

const uint64_t NUM_BLOCKS = 16ULL * 1024;


int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  //const uint64_t interval = atoi(argv[3]);
  char data_buffer[4096];
  int ret;
  std::chrono::duration<double, std::micro> d;

  kv::LocalEngine *kv_imp;

  std::vector<double> read_latency;
  std::vector<double> write_latency;
  std::vector<uint64_t> remote_addrs;
  std::vector<uint32_t> rkeys;

  kv_imp = new kv::LocalEngine();
  assert(kv_imp);
  kv_imp->start(rdma_addr, rdma_port);
  
  for(uint32_t i = 0;i < NUM_BLOCKS; ++i) {
    uint64_t raddr;
    uint32_t rkey;
    ret = kv_imp->allocate_remote_block(raddr, rkey);
    if(ret){
        std::cout << "allocate remote block fail." << std::endl;
        continue;
    }
    remote_addrs.push_back(raddr);
    rkeys.push_back(rkey);
  }

  ret = kv_imp->read_block((uint64_t)data_buffer, remote_addrs[0], 4096, rkeys[0]);
  if(ret){
    std::cout << "read remote block fail." << std::endl;
  }
  for(int i = 0;i < 4; ++i) {
    std::cout << "data is " << data_buffer[i]<<std::endl;
  }

  for(uint32_t i = 0;i < remote_addrs.size(); ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    ret = kv_imp->read_block((uint64_t)data_buffer, remote_addrs[i], 4096, rkeys[i]);
    if(ret){
        std::cout << "read remote block fail." << std::endl;
    }
    auto end = std::chrono::high_resolution_clock::now();
    d = end - start;
    read_latency.push_back(d.count());
  }

  for(uint32_t i = 0;i < remote_addrs.size(); ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    memset(data_buffer, 1, 4096);
    ret = kv_imp->write_block((uint64_t)data_buffer, remote_addrs[i], 4096, rkeys[i]);
    if(ret){
        std::cout << "write remote block fail." << std::endl;
    }
    auto end = std::chrono::high_resolution_clock::now();
    d = end - start;
    write_latency.push_back(d.count());
  }

  double sum_read = 0.0;
  double sum_write = 0.0;

  for(auto rl : read_latency) {
    sum_read += rl;
  }

  for(auto wl : write_latency) {
    sum_write += wl;
  }

  std::cout << "avg read latency is : " << sum_read / read_latency.size() << std::endl;
  std::cout << "avg write latency is : " << sum_write / write_latency.size() << std::endl;

  return 0;
}