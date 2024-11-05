#include <string>
#include <iostream>
#include "kv_engine.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"

int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  
  kv::LocalEngine *kv_imp = new kv::LocalEngine();
  assert(kv_imp);
  kv_imp->start(rdma_addr, rdma_port);
  uint64_t addr;

  do {
    kv_imp->allocate_remote_page(addr);
    std::cout << "current addr is " << addr << std::endl;
  } while (kv_imp->alive());

  kv_imp->stop();
  delete kv_imp;

  return 0;
}