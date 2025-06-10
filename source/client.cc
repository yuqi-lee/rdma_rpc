#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <algorithm>

#include "kv_engine.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
#include "two_level_queues.h"

int get_remote_global_rkey(kv::LocalEngine *kv_imp) {
  uint32_t rkey = 0;
  kv_imp->get_global_rkey(rkey);
  if(rkey == 0) {
    perror("get global rkey fail.\n");
    return -1;
  }

  assert(queues_allocator != nullptr);
  for(uint32_t i = 0;i < NUM_ONLINE_CPUS; ++i) {
    auto queue_allocator = &queues_allocator->queues[i];
    queue_allocator->rkey.store(rkey);
  }
  return 0;
}




int fill_allocate_page_queue(local_pool* pool, const std::vector<int>& online_cpus) {
  uint64_t remote_addr;
  uint64_t count = 0;
    for(auto id : online_cpus) {
      while(get_length_allocator(id) < ALLOCATE_BUFFER_SIZE - 1) {
          remote_addr = pool->allocate_one_page();
          assert(remote_addr != 0);
          assert((remote_addr & ((1 << PAGE_SHIFT) - 1)) == 0);
          push_queue_allocator(remote_addr, id);
          count++;
          if(count == 1) {
            std::cout << "remote addr is " << remote_addr << std::endl;
          }
        }
      } 
  return 0;
}



void allocation_thread(kv::LocalEngine *kv_imp,  const std::vector<int>& online_cpus) {
  uint64_t count = 0;
  std::vector<double> res;
  int ret;
  local_pool* pool = new local_pool(kv_imp);

  fill_allocate_page_queue(pool, online_cpus);

  while(true) {
    count++;
    for(auto id : online_cpus) {
      uint64_t cur_queue_allocator_len = get_length_allocator(id);
      uint64_t cur_queue_deallocator_len = get_length_deallocator(id);

      if(cur_queue_allocator_len < ALLOCATE_BUFFER_SIZE - 1) {
        push_queue_allocator(pool->allocate_one_page(), id);
      }

      if(cur_queue_deallocator_len > 0) {
        pool->deallocate_one_page(pop_queue_deallocator(id));
      }
    }
    
    /*
    if(count % 500000000 == 0) {
      for(auto id : online_cpus) {
        auto queue_allocator = &queues_allocator->queues[id];
        //std::cout << "count: " << count << std::endl;
        //printf("allocator queue: len = %d, begin = %d, end = %d, first = %ld\n", (int)get_length_allocator(id), (int)queue_allocator->begin, (int)queue_allocator->end, (unsigned long)queue_allocator->begin);
        //std::cout << "allocator queue: len = " << get_length_allocator() << ", begin = " << queue_allocator->begin << ""<< std::endl;
        //std::cout << "deallocator queue len:" << get_length_deallocator(id) << std::endl;
      }
    }*/
  }
  
  kv_imp->stop();
  delete kv_imp;
}

int main(int argc, char *argv[]) {
  const std::string rdma_addr(argv[1]);
  const std::string rdma_port(argv[2]);
  //const uint64_t interval = atoi(argv[3]);

  page_queue_shm_init();

  std::vector<std::vector<int>> online_cpus;
  int start = 0;  
  while (start < NUM_ONLINE_CPUS) {
      int end = std::min(start + NUM_CPUS_PER_THREAD, NUM_ONLINE_CPUS);
      std::vector<int> currentVec;
      for (int i = start; i < end; ++i) {
          currentVec.push_back(i);
      }
      online_cpus.push_back(currentVec);
      start = end;  
  }

  std::vector<std::thread *> adaptive_scaler;
  for(auto v : online_cpus) {
    kv::LocalEngine *kv_imp;
    kv_imp = new kv::LocalEngine();
    assert(kv_imp);
    kv_imp->start(rdma_addr, rdma_port);
    get_remote_global_rkey(kv_imp);
    auto t = new std::thread(&allocation_thread, kv_imp, v);
    adaptive_scaler.push_back(t);
  }

  for(auto t : adaptive_scaler) {
    t->join();
  }

  return 0;
}