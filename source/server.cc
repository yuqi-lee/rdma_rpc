#include <string>
#include "kv_engine.h"
#include "rdma_conn_manager.h"

int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  
  kv::RemoteEngine *kv_imp = new kv::RemoteEngine();
  assert(kv_imp);
  if(!kv_imp->start(rdma_addr, rdma_port)){
    std::cout << "start error" << std::endl;
  }

  do {
    // sleep
  } while (kv_imp->alive());

  kv_imp->stop();
  delete kv_imp;

  return 0;
}