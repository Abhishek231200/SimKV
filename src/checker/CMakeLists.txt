add_library(simkv_checker
    own_checker.cpp
)
target_include_directories(simkv_checker PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv_checker PUBLIC simkv_harness simkv_compile_flags)
