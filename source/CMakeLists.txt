include_directories(${PROJECT_SOURCE_DIR}/include)

set(BASE_SOURCE
    local_engine.cc remote_engine.cc rdma_conn_manager.cc rdma_conn.cc rdma_mem_pool.cc)

add_library(polarkv STATIC ${BASE_SOURCE})

add_definitions(-DTBB_PREVIEW_CONCURRENT_ORDERED_CONTAINERS)

add_executable(server server.cc remote_engine.cc  rdma_conn.cc rdma_conn_manager.cc)
add_executable(client client.cc local_engine.cc rdma_conn.cc rdma_conn_manager.cc rdma_mem_pool.cc)
add_executable(latency_test latency_test.cc local_engine.cc rdma_conn.cc rdma_conn_manager.cc rdma_mem_pool.cc)
add_executable(multi_mrs multi_mrs.cc local_engine.cc rdma_conn.cc rdma_conn_manager.cc rdma_mem_pool.cc)


target_link_libraries(server
    ibverbs
    rdmacm
    rt
    tbb
)

target_link_libraries(client
    ibverbs
    rdmacm
    rt
)

target_link_libraries(latency_test
    ibverbs
    rdmacm
    rt
)

target_link_libraries(multi_mrs
    ibverbs
    rdmacm
    rt
)

install(TARGETS polarkv
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)

