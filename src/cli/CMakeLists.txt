add_executable(simkv main.cpp)
target_include_directories(simkv PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv PRIVATE simkv_checker simkv_compile_flags)
