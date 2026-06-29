add_library(simkv_storage
    durable_store.cpp
    wal.cpp
    kv_store.cpp
)
target_include_directories(simkv_storage PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv_storage PUBLIC simkv_compile_flags)
