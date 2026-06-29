#include "sim/network.hpp"
#include "sim/simulator.hpp"
#include <gtest/gtest.h>

TEST(Network, PingPongDelivery) {
    Simulator sim(42);
    NetworkConfig cfg;
    cfg.drop_prob = 0.0;
    Network net(sim, cfg);

    int count = 0;
    net.set_handler(1, [&](Message) { ++count; });
    net.send(2, 1, {0xFF});
    net.send(2, 1, {0xFF});
    sim.run_until_idle();
    EXPECT_EQ(count, 2);
}

TEST(Network, DropProbability) {
    // With drop_prob=1.0, no messages should arrive.
    Simulator sim(7);
    NetworkConfig cfg;
    cfg.drop_prob = 1.0;
    Network net(sim, cfg);

    int count = 0;
    net.set_handler(1, [&](Message) { ++count; });
    for (int i = 0; i < 100; ++i) net.send(2, 1, {0xAB});
    sim.run_until_idle();
    EXPECT_EQ(count, 0);
}

TEST(Network, Partition) {
    Simulator sim(13);
    NetworkConfig cfg;
    cfg.drop_prob = 0.0;
    Network net(sim, cfg);

    int received_1 = 0, received_2 = 0;
    net.set_handler(1, [&](Message) { ++received_1; });
    net.set_handler(2, [&](Message) { ++received_2; });

    // Partition: {1} | {2}
    net.partition({{1}, {2}});

    net.send(1, 2, {0x01}); // cross-group: should be dropped
    net.send(2, 1, {0x02}); // cross-group: should be dropped
    sim.run_until_idle();
    EXPECT_EQ(received_1, 0);
    EXPECT_EQ(received_2, 0);

    // Heal and send.
    net.heal();
    net.send(1, 2, {0x01});
    sim.run_until_idle();
    EXPECT_EQ(received_2, 1);
}

TEST(Network, DuplicationDeliversBoth) {
    Simulator sim(99);
    NetworkConfig cfg;
    cfg.dup_prob  = 1.0; // always duplicate
    cfg.drop_prob = 0.0;
    Network net(sim, cfg);

    int count = 0;
    net.set_handler(2, [&](Message) { ++count; });
    net.send(1, 2, {0xCC});
    sim.run_until_idle();
    EXPECT_EQ(count, 2);
}

TEST(Network, PartitionIntraGroupAllowed) {
    Simulator sim(55);
    NetworkConfig cfg;
    cfg.drop_prob = 0.0;
    Network net(sim, cfg);

    int received_2 = 0, received_3 = 0;
    net.set_handler(2, [&](Message) { ++received_2; });
    net.set_handler(3, [&](Message) { ++received_3; });

    // {1,2} vs {3}: 1 can talk to 2 but not to 3.
    net.partition({{1, 2}, {3}});
    net.send(1, 2, {0x01}); // intra-group: allowed
    net.send(1, 3, {0x02}); // cross-group: dropped
    sim.run_until_idle();
    EXPECT_EQ(received_2, 1);
    EXPECT_EQ(received_3, 0);
}
