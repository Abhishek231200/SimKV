include(GoogleTest)

function(add_simkv_test name)
    add_executable(${name} ${name}.cpp)
    target_include_directories(${name} PRIVATE ${CMAKE_SOURCE_DIR}/src)
    target_link_libraries(${name} PRIVATE simkv_checker GTest::gtest_main)
    gtest_discover_tests(${name})
endfunction()

add_simkv_test(sim_determinism_test)
add_simkv_test(sim_network_test)
add_simkv_test(storage_wal_test)
add_simkv_test(raft_election_test)
add_simkv_test(raft_replication_test)
add_simkv_test(raft_partition_test)
add_simkv_test(raft_snapshot_test)
add_simkv_test(raft_membership_test)
add_simkv_test(raft_read_index_test)
add_simkv_test(linearizability_test)

# Integration test: real TCP processes, leader election, failover.
# Requires bash and nc (netcat); skipped on non-Unix platforms.
if(UNIX)
    add_test(
        NAME integration_test
        COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/integration_test.sh $<TARGET_FILE:simkv_server>
    )
    set_tests_properties(integration_test PROPERTIES
        TIMEOUT 90
        LABELS "integration"
        RUN_SERIAL TRUE)   # don't compete with parallel unit tests for CPU
endif()
