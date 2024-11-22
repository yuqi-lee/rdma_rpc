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


#define ALLOCATE_BUFFER_SIZE (128 << 10) // 512MB
#define DEALLOCATE_BUFFER_SIZE (512 << 10) // 2GB
#define ALLOCATOR_DEVICE "/allocator_page_queue"
#define DEALLOCATOR_DEVICE "/deallocator_page_queue"

struct allocator_page_queue {
    uint32_t rkey = 0;
    uint64_t begin = 0;
    uint64_t end = 0;
    uint64_t pages[ALLOCATE_BUFFER_SIZE] = {0};
};

struct deallocator_page_queue {
    uint64_t begin = 0;
    uint64_t end = 0;
    uint64_t pages[DEALLOCATE_BUFFER_SIZE] = {0};
};

struct allocator_page_queue *queue_allocator = nullptr;
struct deallocator_page_queue *queue_deallocator = nullptr;

int page_queue_shm_init() {
  int fd = shm_open(ALLOCATOR_DEVICE, O_RDWR, 0);
  if (fd < 0) {
    fd = shm_open(ALLOCATOR_DEVICE, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (ftruncate(fd, sizeof(struct allocator_page_queue)) == -1) {
      perror("ftruncate");
      return -1;
    }
  }
  queue_allocator = (allocator_page_queue *)mmap(NULL, sizeof(struct allocator_page_queue), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (queue_allocator == MAP_FAILED) {
    perror("Failed to mmap queue_allocator.");
    close(fd);
    return -1;
  }
  memset(queue_allocator, 0, sizeof(struct allocator_page_queue));

  fd = shm_open(DEALLOCATOR_DEVICE, O_RDWR, 0);
  if (fd < 0) {
    fd = shm_open(DEALLOCATOR_DEVICE, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (ftruncate(fd, sizeof(struct deallocator_page_queue)) == -1) {
      perror("ftruncate");
      return -1;
    }
  }
  queue_deallocator = (deallocator_page_queue *)mmap(NULL, sizeof(struct deallocator_page_queue), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (queue_deallocator == MAP_FAILED) {
    perror("Failed to mmap queue_deallocator.");
    close(fd);
    return -1;
  }
  memset(queue_deallocator, 0, sizeof(struct deallocator_page_queue));

  return 0;
}

uint64_t get_length_allocator() {
    uint64_t begin = queue_allocator->begin;
    uint64_t end = queue_allocator->end;
    if (begin == end) {
        return 0;
    }
    if (end > begin) {
        return (end - begin);
    } else {
        return (ALLOCATE_BUFFER_SIZE - begin + end);
    }
}

uint64_t get_length_deallocator() {
    uint64_t begin = queue_deallocator->begin;
    uint64_t end = queue_deallocator->end;
    if (begin == end) {
        return 0;
    }
    if (end > begin) {
        return (end - begin);
    } else {
        return (DEALLOCATE_BUFFER_SIZE - begin + end);
    }
}

int push_queue_allocator(uint64_t page_addr) {
    int ret = 0;
    uint64_t prev_end = queue_allocator->end;
    while(get_length_allocator() >= ALLOCATE_BUFFER_SIZE - 1) ;
    queue_allocator->end = (queue_allocator->end + 1) % ALLOCATE_BUFFER_SIZE;
    queue_allocator->pages[prev_end] = page_addr;
    return ret;
}

uint64_t pop_queue_deallocator() {
    uint64_t ret = 0;
    while(get_length_allocator() == 0) ;
    uint64_t prev_begin = queue_deallocator->begin;
    queue_deallocator->begin = (queue_deallocator->begin + 1) % DEALLOCATE_BUFFER_SIZE;
    while(queue_deallocator->pages[prev_begin] == 0) ;
    ret = queue_deallocator->pages[prev_begin];
    queue_deallocator->pages[prev_begin] = 0;
    return ret;
}

int get_remote_global_rkey(kv::LocalEngine *kv_imp) {
  uint32_t rkey = 0;
  kv_imp->get_global_rkey(rkey);
  if(rkey == 0) {
    perror("get global rkey fail.\n");
    return -1;
  }

  assert(queue_allocator != nullptr);
  queue_allocator->rkey = rkey;
  return 0;
}

int fill_allocate_page_queue(kv::LocalEngine *kv_imp) {
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  uint64_t count = 0;
  while(true) {
    uint64_t len = get_length_allocator();
    if(len < ALLOCATE_BUFFER_SIZE - 1) {
      uint64_t batch_size = std::min(uint64_t(MAX_BATCH_SIZE), ALLOCATE_BUFFER_SIZE - 1 - len);
      kv_imp->allocate_remote_page_batch(remote_addrs, batch_size);
      if(count < 1) {
        for(uint64_t i = 0;i < batch_size; ++i) {
          std::cout << (void*) remote_addrs[i] << " ";
        }
        std::cout << std::endl;
      }
      count++;
      for(uint64_t i = 0;i < batch_size; ++i) {
        push_queue_allocator(remote_addrs[i]);
      }
    } else {
      std::cout << "fill allocate page queue successfully. allocate page queue len is " << get_length_allocator() << std::endl;
      break;
    }
  }
  return 0;
}

void throughput_test(kv::LocalEngine *kv_imp, 
                    const uint64_t interval) {
  //kv::LocalEngine *kv_imp = new kv::LocalEngine();
  //assert(kv_imp);
  //kv_imp->start(ip, port);
  uint64_t addr;
  uint64_t count = 0;
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::micro> duration = end - start;
  double max_duratuon = 0.0;
  std::vector<double> res;
  //uint64_t count = 0;
  uint64_t* remote_addrs = new uint64_t[MAX_BATCH_SIZE];
  uint64_t remote_addr;

  while(true) {
    count++;
    uint64_t cur_queue_allocator_len = get_length_allocator();
    if(cur_queue_allocator_len < ALLOCATE_BUFFER_SIZE - 1) {
      if(get_length_deallocator() > 0) {
        remote_addr = pop_queue_deallocator();
        push_queue_allocator(remote_addr);
      } else {
        uint64_t batch_size = std::min(uint64_t(MAX_BATCH_SIZE), ALLOCATE_BUFFER_SIZE - 1 - cur_queue_allocator_len);
        kv_imp->allocate_remote_page_batch(remote_addrs, batch_size);
        for(uint64_t i = 0;i < batch_size; ++i) {
          push_queue_allocator(remote_addrs[i]);
        }
      }
    } else {
      uint64_t cur_queue_deallocator_len = get_length_deallocator();
      if(cur_queue_deallocator_len > 0) {
        int batch_size = MAX_BATCH_SIZE > cur_queue_deallocator_len ? (int)cur_queue_deallocator_len : MAX_BATCH_SIZE;
        for(int i = 0;i < batch_size; ++i) {
          remote_addrs[i] = pop_queue_deallocator();
        }
        kv_imp->free_remote_page_batch(remote_addrs, batch_size);
      }
    }
    if(count % 500000000 == 0) {
      std::cout << "count: " << count << std::endl;
      std::cout << "allocator queue len:" << get_length_allocator() << std::endl;
      std::cout << "deallocator queue len:" << get_length_deallocator() << std::endl;
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
  kv::LocalEngine *kv_imp = new kv::LocalEngine();
  assert(kv_imp);
  kv_imp->start(rdma_addr, rdma_port);

  
  get_remote_global_rkey(kv_imp);
  fill_allocate_page_queue(kv_imp);


  auto t  = new std::thread(&throughput_test, kv_imp, interval);
  t->join();

  return 0;
}