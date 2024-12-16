#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "kv_engine.h"
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"


#define ALLOCATE_BUFFER_SIZE (4 << 10) // 16MB
#define RECLAIM_ALLOCATE_BUFFER_SIZE (16 << 10) // 64MB
#define DEALLOCATE_BUFFER_SIZE (16 << 10) // 64MB
#define NUM_ONLINE_CPUS 48
#define NUM_RECLAIM_CPUS 4
#define ALLOCATOR_DEVICE "/allocator_page_queue"
#define DEALLOCATOR_DEVICE "/deallocator_page_queue"

struct allocator_page_queue {
    std::atomic<int32_t> rkey;
    std::atomic<int64_t> begin;
    std::atomic<int64_t> end;
    std::atomic<int64_t> pages[ALLOCATE_BUFFER_SIZE];
};

struct reclaim_allocator_page_queue {
    std::atomic<int64_t> begin;
    std::atomic<int64_t> end;
    std::atomic<int64_t> pages[RECLAIM_ALLOCATE_BUFFER_SIZE];
};

struct deallocator_page_queue {
    std::atomic<int64_t> begin;
    std::atomic<int64_t> end;
    std::atomic<int64_t> pages[DEALLOCATE_BUFFER_SIZE];
};

struct allocator_page_queues {
  struct allocator_page_queue queues[NUM_ONLINE_CPUS];
  struct reclaim_allocator_page_queue reclaim_queues[NUM_RECLAIM_CPUS];
};

struct deallocator_page_queues {
  struct deallocator_page_queue queues[NUM_ONLINE_CPUS];
};

struct allocator_page_queues *queues_allocator = nullptr;
struct deallocator_page_queues *queues_deallocator = nullptr;

int page_queue_shm_init() {
  int fd = shm_open(ALLOCATOR_DEVICE, O_RDWR, 0);
  if (fd < 0) {
    fd = shm_open(ALLOCATOR_DEVICE, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (ftruncate(fd, sizeof(struct allocator_page_queues)) == -1) {
      perror("ftruncate");
      return -1;
    }
  }
  queues_allocator = (struct allocator_page_queues *)mmap(NULL, sizeof(struct allocator_page_queues), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (queues_allocator == MAP_FAILED) {
    perror("Failed to mmap queue_allocator.");
    close(fd);
    return -1;
  }
  //memset(queue_allocator, 0, sizeof(struct allocator_page_queue));
  for(uint32_t i = 0;i < NUM_ONLINE_CPUS; ++i) {
    auto queue_allocator = &queues_allocator->queues[i];
    queue_allocator->rkey.store(0);
    queue_allocator->begin.store(0);
    queue_allocator->end.store(0);
    for(uint32_t j = 0;j < ALLOCATE_BUFFER_SIZE; ++j) {
      queue_allocator->pages[j].store(0);
    }
  }
  for(uint32_t i = 0;i < NUM_RECLAIM_CPUS; ++i) {
    auto queue_allocator = &queues_allocator->reclaim_queues[i];
    queue_allocator->begin.store(0);
    queue_allocator->end.store(0);
    for(uint64_t i = 0;i < RECLAIM_ALLOCATE_BUFFER_SIZE; ++i) {
      queue_allocator->pages[i].store(0);
    }
  }
  
  

  fd = shm_open(DEALLOCATOR_DEVICE, O_RDWR, 0);
  if (fd < 0) {
    fd = shm_open(DEALLOCATOR_DEVICE, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (ftruncate(fd, sizeof(struct deallocator_page_queues)) == -1) {
      perror("ftruncate");
      return -1;
    }
  }
  queues_deallocator = (deallocator_page_queues *)mmap(NULL, sizeof(struct deallocator_page_queues), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (queues_deallocator == MAP_FAILED) {
    perror("Failed to mmap queue_deallocator.");
    close(fd);
    return -1;
  }
  //memset(queue_deallocator, 0, sizeof(struct deallocator_page_queue));
  for(uint32_t i = 0;i < NUM_ONLINE_CPUS; ++i) {
    auto queue_deallocator = &queues_deallocator->queues[i];
    queue_deallocator->begin.store(0);
    queue_deallocator->end.store(0);
    for(uint64_t i = 0;i < DEALLOCATE_BUFFER_SIZE; ++i) {
      queue_deallocator->pages[i].store(0);
    }
  }
  
  return 0;
}

uint64_t get_length_allocator(uint32_t id) {
    auto queue_allocator = &queues_allocator->queues[id];
    uint64_t begin = queue_allocator->begin.load();
    uint64_t end = queue_allocator->end.load();
    if (begin == end) {
        return 0;
    }
    if (end > begin) {
        return (end - begin);
    } else {
        return (ALLOCATE_BUFFER_SIZE - begin + end);
    }
}

uint64_t get_length_reclaim_allocator(uint32_t id) {
    auto queue_allocator = &queues_allocator->reclaim_queues[id];
    uint64_t begin = queue_allocator->begin.load();
    uint64_t end = queue_allocator->end.load();
    if (begin == end) {
        return 0;
    }
    if (end > begin) {
        return (end - begin);
    } else {
        return (RECLAIM_ALLOCATE_BUFFER_SIZE - begin + end);
    }
}

uint64_t get_length_deallocator(uint32_t id) {
    auto queue_deallocator = &queues_deallocator->queues[id];
    uint64_t begin = queue_deallocator->begin.load();
    uint64_t end = queue_deallocator->end.load();
    if (begin == end) {
        return 0;
    }
    if (end > begin) {
        return (end - begin);
    } else {
        return (DEALLOCATE_BUFFER_SIZE - begin + end);
    }
}

int push_queue_allocator(uint64_t page_addr, uint32_t id) {
    auto queue_allocator = &queues_allocator->queues[id];
    int ret = 0;
    uint64_t prev_end = queue_allocator->end.load();
    while(get_length_allocator(id) >= ALLOCATE_BUFFER_SIZE - 1) ;
    queue_allocator->end.store((prev_end+ 1) % ALLOCATE_BUFFER_SIZE);
    queue_allocator->pages[prev_end].store(page_addr);
    return ret;
}

int push_queue_reclaim_allocator(uint64_t page_addr, uint32_t id) {
    auto queue_allocator = &queues_allocator->reclaim_queues[id];
    int ret = 0;
    uint64_t prev_end = queue_allocator->end.load();
    while(get_length_reclaim_allocator(id) >= RECLAIM_ALLOCATE_BUFFER_SIZE - 1) ;
    queue_allocator->end.store((prev_end+ 1) % RECLAIM_ALLOCATE_BUFFER_SIZE);
    queue_allocator->pages[prev_end].store(page_addr);
    return ret;
}

uint64_t pop_queue_deallocator(uint32_t id) {
    auto queue_deallocator = &queues_deallocator->queues[id];
    uint64_t ret = 0;
    while(get_length_deallocator(id) == 0) ;
    uint64_t prev_begin = queue_deallocator->begin.load();
    queue_deallocator->begin.store((prev_begin + 1) % DEALLOCATE_BUFFER_SIZE);
    while(queue_deallocator->pages[prev_begin].load() == 0) ;
    ret = queue_deallocator->pages[prev_begin].load();
    queue_deallocator->pages[prev_begin].store(0);
    return ret;
}

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

int fill_allocate_page_queue(kv::LocalEngine *kv_imp, const uint32_t begin, const uint32_t end) {
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  uint64_t count = 0;
  int ret = 0;

    for(uint32_t id = begin;id <= end; ++id) {
      while(get_length_allocator(id) < ALLOCATE_BUFFER_SIZE - 1) {
        uint64_t batch_size = std::min(uint64_t(MAX_BATCH_SIZE), ALLOCATE_BUFFER_SIZE - 1 - get_length_allocator(id));
        ret = kv_imp->allocate_remote_page_batch(remote_addrs, batch_size);
        if(ret) {
          std::cout << "allocate_remote_page_batch fail." << std::endl;
          continue;
        }
        if(count < 1) {
          for(uint64_t i = 0;i < batch_size; ++i) {
            std::cout << (void*) remote_addrs[i] << " ";
          }
          std::cout << std::endl;
        }
        count++;
        for(uint64_t i = 0;i < batch_size; ++i) {
          assert(remote_addrs[i] != 0);
          assert((remote_addrs[i] & ((1 << PAGE_SHIFT) - 1)) == 0);
          push_queue_allocator(remote_addrs[i], id);
        }
      } 
      std::cout << "fill allocate page queue successfully. allocate page queue len is " << get_length_allocator(id) << std::endl;
    } 
  return 0;
}

void reclaim_filler(kv::LocalEngine *kv_imp, const uint64_t interval) {
  int ret;
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];

  while(true) {
    for(uint32_t id = 0;id < NUM_RECLAIM_CPUS; ++id) {
      uint64_t cur_queue_allocator_len = get_length_reclaim_allocator(id);
      if(cur_queue_allocator_len < RECLAIM_ALLOCATE_BUFFER_SIZE - 1) {
        uint64_t batch_size = std::min(uint64_t(MAX_BATCH_SIZE), RECLAIM_ALLOCATE_BUFFER_SIZE - 1 - cur_queue_allocator_len);
        ret = kv_imp->allocate_remote_page_batch(remote_addrs, batch_size);
        if(ret) {
          std::cout << "allocate_remote_page_batch fail." << std::endl;
          continue;
        }
        for(uint64_t i = 0;i < batch_size; ++i) {
          assert(remote_addrs[i] != 0);
          assert((remote_addrs[i] & ((1 << PAGE_SHIFT) - 1)) == 0);
          push_queue_reclaim_allocator(remote_addrs[i], id);
        }
      }
    }
  }
}

void deallocation_thread(kv::LocalEngine *kv_imp, const uint64_t interval) {
  uint64_t addr;
  uint64_t count = 0;
  double max_duratuon = 0.0;
  std::vector<double> res;
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  uint64_t remote_addr;
  int ret;

  while(true) {
    count++;
    for(int32_t id = 0; id < NUM_ONLINE_CPUS; ++id) {
      uint64_t cur_queue_deallocator_len = get_length_deallocator(id);
      if(cur_queue_deallocator_len > 0) {
        int batch_size = MAX_BATCH_SIZE > cur_queue_deallocator_len ? (int)cur_queue_deallocator_len : MAX_BATCH_SIZE;
        for(int i = 0;i < batch_size; ++i) {
          remote_addrs[i] = pop_queue_deallocator(id);
        }
        ret = kv_imp->free_remote_page_batch(remote_addrs, batch_size);
        if(ret) {
          std::cout << "free_remote_page_batch fail." << std::endl;
          continue;
        }
      }
    }
  }
}


void allocation_thread(kv::LocalEngine *kv_imp, 
                    const uint64_t interval, const uint32_t begin, const uint32_t end) {
  //kv::LocalEngine *kv_imp = new kv::LocalEngine();
  //assert(kv_imp);
  //kv_imp->start(ip, port);
  uint64_t addr;
  uint64_t count = 0;
  double max_duratuon = 0.0;
  std::vector<double> res;
  //uint64_t count = 0;
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  uint64_t remote_addr;
  int ret;

  fill_allocate_page_queue(kv_imp, begin, end);

  while(true) {
    count++;
    for(uint32_t id = begin;id <= end; ++id) {
      uint64_t cur_queue_allocator_len = get_length_allocator(id);
      if(cur_queue_allocator_len < ALLOCATE_BUFFER_SIZE - 1) {
        uint64_t batch_size = std::min(uint64_t(MAX_BATCH_SIZE), ALLOCATE_BUFFER_SIZE - 1 - cur_queue_allocator_len);
        ret = kv_imp->allocate_remote_page_batch(remote_addrs, batch_size);
        if(ret) {
          std::cout << "allocate_remote_page_batch fail." << std::endl;
          continue;
        }
        for(uint64_t i = 0;i < batch_size; ++i) {
          assert(remote_addrs[i] != 0);
          assert((remote_addrs[i] & ((1 << PAGE_SHIFT) - 1)) == 0);
          push_queue_allocator(remote_addrs[i], id);
        }
      }
    }
    
    if(count % 500000000 == 0 && begin == 0) {
      auto queue_allocator = &queues_allocator->queues[0];
      std::cout << "count: " << count << std::endl;
      printf("allocator queue: len = %d, begin = %d, end = %d, first = %ld\n", (int)get_length_allocator(0), (int)queue_allocator->begin, (int)queue_allocator->end, (unsigned long)queue_allocator->begin);
      //std::cout << "allocator queue: len = " << get_length_allocator() << ", begin = " << queue_allocator->begin << ""<< std::endl;
      std::cout << "deallocator queue len:" << get_length_deallocator(0) << std::endl;
    }
    
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

  page_queue_shm_init();
  kv::LocalEngine *kv_imps[3];
  for(int i = 0;i < 3; ++i) {
    kv_imps[i] = new kv::LocalEngine();
    assert(kv_imps[i]);
    kv_imps[i]->start(rdma_addr, rdma_port);
  }

  
  get_remote_global_rkey(kv_imps[0]);


  auto t1  = new std::thread(&allocation_thread, kv_imps[0], interval, 0, 0);
  auto t2  = new std::thread(&allocation_thread, kv_imps[1], interval, 1, 47);
  auto t3  = new std::thread(&reclaim_filler, kv_imps[2], interval);
  auto t4  = new std::thread(&deallocation_thread, kv_imps[1], interval);
  t1->join();
  t2->join();

  return 0;
}