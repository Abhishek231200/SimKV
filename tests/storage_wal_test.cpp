#include "storage/durable_store.hpp"
#include "storage/kv_store.hpp"
#include "storage/wal.hpp"
#include <gtest/gtest.h>

TEST(DurableStore, BasicAppendFlush) {
    DurableStore store;
    std::vector<uint8_t> bytes = {1, 2, 3};
    store.append(bytes);
    EXPECT_EQ(store.total_size(), 3u);
    EXPECT_EQ(store.durable_size(), 0u); // not flushed yet

    store.flush();
    EXPECT_EQ(store.durable_size(), 3u);
}

TEST(DurableStore, CrashLosesUnflushed) {
    DurableStore store;
    std::vector<uint8_t> a = {1, 2, 3};
    store.append(a);
    store.flush();

    std::vector<uint8_t> b = {4, 5, 6};
    store.append(b);
    EXPECT_EQ(store.total_size(), 6u);

    store.crash();
    EXPECT_EQ(store.total_size(), 3u);
    EXPECT_EQ(store.durable_size(), 3u);
    auto durable = store.read_durable();
    ASSERT_EQ(durable.size(), 3u);
    EXPECT_EQ(durable[0], 1);
    EXPECT_EQ(durable[2], 3);
}

TEST(Wal, AppendAndReplay) {
    DurableStore store;
    Wal wal(store);

    wal.append(CmdPut{"key1", "val1"});
    wal.append(CmdPut{"key2", "val2"});
    wal.append(CmdDelete{"key1"});
    wal.flush();

    auto cmds = wal.replay();
    ASSERT_EQ(cmds.size(), 3u);
    EXPECT_TRUE(std::holds_alternative<CmdPut>(cmds[0]));
    EXPECT_EQ(std::get<CmdPut>(cmds[0]).key, "key1");
    EXPECT_EQ(std::get<CmdPut>(cmds[1]).value, "val2");
    EXPECT_TRUE(std::holds_alternative<CmdDelete>(cmds[2]));
}

TEST(Wal, CrashLosesUnflushedRecords) {
    DurableStore store;
    Wal wal(store);

    wal.append(CmdPut{"a", "1"});
    wal.flush(); // durable

    wal.append(CmdPut{"b", "2"}); // unflushed
    store.crash();

    auto cmds = wal.replay();
    ASSERT_EQ(cmds.size(), 1u);
    EXPECT_EQ(std::get<CmdPut>(cmds[0]).key, "a");
}

TEST(Wal, CasRoundTrip) {
    DurableStore store;
    Wal wal(store);

    wal.append(CmdCas{"k", "old", "new"});
    wal.flush();

    auto cmds = wal.replay();
    ASSERT_EQ(cmds.size(), 1u);
    auto& c = std::get<CmdCas>(cmds[0]);
    EXPECT_EQ(c.key, "k");
    EXPECT_EQ(c.expected, "old");
    EXPECT_EQ(c.value, "new");
}

TEST(Wal, ReplayIntoKvStore) {
    DurableStore store;
    Wal wal(store);

    wal.append(CmdPut{"x", "10"});
    wal.append(CmdPut{"y", "20"});
    wal.append(CmdCas{"x", "10", "11"});
    wal.append(CmdDelete{"y"});
    wal.flush();

    auto cmds = wal.replay();
    KvStore kv;
    for (const auto& cmd : cmds) kv.apply(cmd);

    EXPECT_EQ(kv.get("x"), "11");
    EXPECT_EQ(kv.get("y"), std::nullopt);
}
