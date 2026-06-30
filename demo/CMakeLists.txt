add_executable(simkv_demo demo.cpp)
target_include_directories(simkv_demo PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv_demo PRIVATE simkv_raft simkv_compile_flags)
