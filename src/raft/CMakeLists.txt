add_library(simkv_raft
    rpc.cpp
    log.cpp
    node.cpp
)
target_include_directories(simkv_raft PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv_raft PUBLIC simkv_sim simkv_storage simkv_compile_flags)
