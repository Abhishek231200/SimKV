add_library(simkv_harness
    history.cpp
    workload.cpp
    fault_injector.cpp
    runner.cpp
)
target_include_directories(simkv_harness PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv_harness PUBLIC simkv_raft simkv_compile_flags)
