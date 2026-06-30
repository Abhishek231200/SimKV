add_executable(simkv_server server.cpp dispatch_queue.cpp real_clock.cpp tcp_transport.cpp)
target_include_directories(simkv_server PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv_server PRIVATE simkv_raft simkv_compile_flags)
target_compile_options(simkv_server PRIVATE -Wno-unused-parameter)
