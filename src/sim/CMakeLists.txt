add_library(simkv_sim
    prng.cpp
    event_queue.cpp
    simulator.cpp
    network.cpp
)
target_include_directories(simkv_sim PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(simkv_sim PUBLIC simkv_compile_flags)
