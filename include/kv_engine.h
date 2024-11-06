#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <mutex>
#include <iostream>
#include "kv_engine.h"
#include "msg.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "string"
#include "thread"
#include "unordered_map"

#define SHARDING_NUM 32
static_assert(((SHARDING_NUM & (~SHARDING_NUM + 1)) == SHARDING_NUM),
              "RingBuffer's size must be a positive power of 2");

namespace kv {

struct PageQueue {
  uint64_t begin;
  uint64_t end;
  uint64_t* pages_addr;
  std::mutex mtx;

  PageQueue(uint64_t len) : begin(0) {
    end = len - 1;
    pages_addr = new uint64_t[len];
    for(uint64_t i = 0;i < len; ++i)
      pages_addr[i] = i;
  }

  int allocate(uint64_t& addr) {
    if(begin == end)
      return -1;
    addr = pages_addr[begin];
    begin++;
  }
};

/* Abstract base engine */
class Engine {
 public:
  virtual ~Engine(){};

  virtual bool start(const std::string addr, const std::string port) = 0;
  virtual void stop() = 0;

  virtual bool alive() = 0;
};

/* Local-side engine */
class LocalEngine : public Engine {
 public:
  struct internal_value_t {
    uint64_t remote_addr;
    uint32_t rkey;
    uint32_t size;
  };

  ~LocalEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

  bool write(const std::string key, const std::string value);
  bool read(const std::string key, std::string &value);
  int allocate_remote_page(uint64_t& value);

 private:
  kv::ConnectionManager *m_rdma_conn_;
  /* NOTE: should use some concurrent data structure, and also should take the
   * extra memory overhead into consideration */
  std::unordered_map<std::string, internal_value_t> m_map_[SHARDING_NUM];
  std::mutex m_mutex_[SHARDING_NUM];
  RDMAMemPool *m_rdma_mem_pool_;
};

/* Remote-side engine */
class RemoteEngine : public Engine {
 public:
  struct WorkerInfo {
    CmdMsgBlock *cmd_msg;
    CmdMsgRespBlock *cmd_resp_msg;
    struct ibv_mr *msg_mr;
    struct ibv_mr *resp_mr;
    rdma_cm_id *cm_id;
    struct ibv_cq *cq;
  };

  ~RemoteEngine(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;

 private:
  void handle_connection();

  int create_connection(struct rdma_cm_id *cm_id);

  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey,
                   uint32_t length, uint64_t remote_addr, uint32_t rkey);

  int allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                   uint64_t size);

  int allocate_page(uint64_t &addr);

  void worker(WorkerInfo *work_info, uint32_t num);

  struct rdma_event_channel *m_cm_channel_;
  struct rdma_cm_id *m_listen_id_;
  struct ibv_pd *m_pd_;
  struct ibv_context *m_context_;
  struct PageQueue* page_queue;
  bool m_stop_;
  std::thread *m_conn_handler_;
  WorkerInfo **m_worker_info_;
  uint32_t m_worker_num_;
  std::thread **m_worker_threads_;
};

}  // namespace kv