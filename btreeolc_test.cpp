#include <gtest/gtest.h>

#include <future>
#include <iostream>
#include <thread>
#include <utility>
#include <vector>
#include <chrono>
#include <thread>
#include <type_traits>  // std::is_trivial

#include "common/btree_olc/BTreeOLC.h"

using namespace std;

class Value {
public:
    Value() : value(-1) {}
    Value(int v) : value(v) {}

    Value(const Value &v) : value(v.value) {}

    Value &operator=(const Value &v) {
        value = v.value;
        return *this;
    }
    void setValue(int v) { value = v;}
    int getValue() const { return value; }
private:
    int value;
};


template<typename T>
class TrivialKey {
public:
    TrivialKey() = default;
    TrivialKey(T v) : value(v) {}

    T getValue() const { return value; }

    char *transfer() const { return nullptr; }  // just for convenience

private:
    T value;
};
static_assert(std::is_trivial<TrivialKey<int>>::value == true, "xxx");

template<typename T>
struct Comparator {
    int operator()(const T &a, const T &b) const {
        if (a.getValue() > b.getValue())
            return 1;
        else if (a.getValue() == b.getValue())
            return 0;
        else
            return -1;
    }
};


template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
using btree_t = btreeolc::BPlusTree<KeyType, Value, Comparator<KeyType>, Comparator<Value>, 10, leaf_size, inner_size>;


template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
void singleInsertTest(btree_t<KeyType, leaf_size, inner_size> &btree) {
    std::vector<int> keys;
    constexpr int num_keys = 100000;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back(i);
    }
    std::random_shuffle(keys.begin(), keys.end());
    for (int i = 0; i < num_keys; i++) {
        KeyType k{keys[i]};
        bool res = btree.insert(k, keys[i]);
        ASSERT_EQ(res, true);
    }
    for (int i = 0; i < 100000; i++) {
        KeyType k{keys[i]};
        bool res = btree.insert(k, 0);
        ASSERT_EQ(res, false); // except insert failed
        delete[] k.transfer();
    }
    auto kv_size = sizeof(KeyType) + sizeof(Value);
    auto inner_nodes = btree.getNumInnerNodes();
    auto leaf_nodes = btree.getNumLeafNodes();
    auto tree_bytes = leaf_size * leaf_nodes + inner_nodes * inner_size;
    std::cout << "inner nodes " << inner_nodes << ", leaf nodes " <<  leaf_nodes << ", kv_size " << kv_size << ", tree_bytes " << tree_bytes << ", # keys " << 100000 << ", # keys per leave " << 100000.0 / leaf_nodes << std::endl;;
}

template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
void singleLookupTest(btree_t<KeyType, leaf_size, inner_size> &btree) {
    Value res;
    for (int i = 0; i < 100000; i++) {
        KeyType k{i};
        bool lookup_flag = btree.lookup(k, res);
        ASSERT_EQ(lookup_flag, true);
        ASSERT_EQ(res.getValue(), i);
        delete[] k.transfer();
    }
}

template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
void singleDeleteTest(btree_t<KeyType, leaf_size, inner_size> &btree) {
    std::vector<int> keys;
    constexpr int num_keys = 100000;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back(i);
    }
    std::random_shuffle(keys.begin(), keys.end());
    for (int i = 50000; i < 100000; i++) {
        KeyType k{keys[i]};
        bool res = btree.remove(k);
        ASSERT_EQ(res, true);
        delete[] k.transfer();
    }
    for (int i = 0; i < 50000; i++) {
        KeyType k{keys[i]};
        bool res = btree.remove(k);
        ASSERT_EQ(res, true);
        delete[] k.transfer();
    }
    for (int i = 0; i < 100000; i++) {
        KeyType k{keys[i]};
        bool res = btree.remove(k);
        ASSERT_EQ(res, false);
        delete[] k.transfer();
    }
    auto kv_size = sizeof(KeyType) + sizeof(Value);
    auto inner_nodes = btree.getNumInnerNodes();
    auto leaf_nodes = btree.getNumLeafNodes();
    auto tree_bytes = leaf_size * leaf_nodes + inner_nodes * inner_size;
    std::cout << "After deletes, inner nodes " << inner_nodes << ", leaf nodes " <<  leaf_nodes << ", kv_size " << kv_size << ", tree_bytes " << tree_bytes << std::endl;;
}

template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
void singleGetValueTest(btree_t<KeyType, leaf_size, inner_size> &btree) {
    for (int i = 0; i < 100000; i++) {
        KeyType k{i};
        Value res = btree.getValue(k, [i]() { return Value{i}; });
        ASSERT_EQ(res.getValue(), i);
    }
}

template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
void singleScanTest(btree_t<KeyType, leaf_size, inner_size> &btree) {
    vector<pair<KeyType, Value>> res;

    {
        KeyType l{0}, h{50000};
        btree.scan(l, h, true, true, 0, res);
        ASSERT_EQ(res.size(), 50001);
        for (int i = 0; i < res.size(); i++) {
            ASSERT_EQ(res[i].second.getValue(), i);
        }
        delete[] l.transfer();
        delete[] h.transfer();
    }
    {
        KeyType l{0}, h{50000};
        btree.scan(l, h, false, true, 0, res);
        ASSERT_EQ(res.size(), 50000);
        int start = 1;
        for (int i = 0; i < res.size(); i++) {
            ASSERT_EQ(res[i].second.getValue(), start++);
        }
        delete[] l.transfer();
        delete[] h.transfer();
    }
    {
        KeyType l{0}, h{50000};
        btree.scan(l, h, false, false, 0, res);
        ASSERT_EQ(res.size(), 49999);
        int start = 1;
        for (int i = 0; i < res.size(); i++) {
            ASSERT_EQ(res[i].second.getValue(), start++);
        }
        delete[] l.transfer();
        delete[] h.transfer();
    }
    {
        KeyType l{0}, h{50000};
        btree.scan(l, h, true, true, 100, res);
        ASSERT_EQ(res.size(), 100);
        for (int i = 0; i < res.size(); i++) {
            ASSERT_EQ(res[i].second.getValue(), i);
        }
        delete[] l.transfer();
        delete[] h.transfer();
    }
    {
        KeyType l{0}, h{50000};
        btree.scan(l, h, true, true, 1000000, res);
        ASSERT_EQ(res.size(), 50001);
        for (int i = 0; i < res.size(); i++) {
            ASSERT_EQ(res[i].second.getValue(), i);
        }
        delete[] l.transfer();
        delete[] h.transfer();
    }
}

template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
void singleScanForUpdateTest(btree_t<KeyType, leaf_size, inner_size> &btree) {
    {
        KeyType l{0}, h{50000};
        int vSeq = 0;
        btree.scanForUpdate(l, [&](const KeyType & key, Value & v, bool lastItem) {
            if (key.getValue() > h.getValue())
                return true; // exit
            assert(v.getValue() == vSeq);
            vSeq++;
            v.setValue(1);
            return false;
        });;
        delete[] l.transfer();
        delete[] h.transfer();
    }

    {
        KeyType l{0}, h{50000};
        btree.scanForUpdate(l, [&](const KeyType & key, Value & v, bool lastItem) {
            if (key.getValue() > h.getValue())
                return true; // exit
            assert(v.getValue() == 1);
            return false;
        });
        delete[] l.transfer();
        delete[] h.transfer();
    }
}

template<typename KeyType, uint64_t leaf_size, uint64_t inner_size>
void multiScanDelInsert1Test(btree_t<KeyType, leaf_size, inner_size> &btree) {
    for (int i = 0; i < 100000; i++) {
        KeyType k{i};
        bool res = btree.remove(k);
        ASSERT_EQ(res, true);
        delete[] k.transfer();
    }

    auto addFunc = [&btree](int start, int end) {
        vector<bool> res(end - start, false);
        for (int i = 0; i < (end - start) / 2; i++) {
            bool ret;
            res[i] = btree.insert(i + start, i + start, &ret).getValue() == -1;
            res[end - i - 1 - start] = btree.insert(end - i - 1, end - i - 1, &ret).getValue() == -1;
        }
        return res;
    };

    auto removeFunc = [&btree](int start, int end) {
        vector<bool> res(end - start, false);
        for (int i = 0; i < (end - start) / 2; i++) {
            res[i] = btree.remove(i + start, i + start);
            res[end - i - 1 - start] = btree.remove(end - i - 1, end - i - 1);
        }
        return res;
    };

    auto scanFunc = [&btree](int start, int end) {
        vector<pair<KeyType, Value>> res;
        btree.scan(start, end, true, true, 0, res);
        for (int i = 0; i < res.size(); i++) {
            ASSERT_EQ(res[i].first.getValue(), res[i].second.getValue());
        }
    };

    // threads for adding value
    auto f1 = async(addFunc, 0, 200);
    auto f2 = async(addFunc, 200, 600);
    auto f3 = async(addFunc, 600, 700);
    auto f4 = async(addFunc, 700, 1000);

    // thread for scan
    auto f_s_0_500 = async(scanFunc, 0, 500);
    auto f_s_3_800 = async(scanFunc, 300, 800);
    auto f_s_1_200 = async(scanFunc, 1, 200);

    // threads for deleting value
    auto f0_200 = async(removeFunc, 0, 200);
    auto f6_700 = async(removeFunc, 600, 700);
    auto f9_1000 = async(removeFunc, 900, 1000);

    auto res9_1000 = f9_1000.get();
    auto res6_700 = f6_700.get();
    auto res0_200 = f0_200.get();

    f_s_1_200.wait();
    f_s_3_800.wait();
    f_s_0_500.wait();

    auto res_4 = f4.get();
    auto res_3 = f3.get();
    auto res_2 = f2.get();
    auto res_1 = f1.get();

    for (int i = 0; i < 1000; i++) {
        Value v;
        bool res = btree.lookup(i, v);
        if (i < 200 && i >= 0) {
            assert(res != res0_200[i]);
        } else if (i < 700 && i >= 600) {
            assert(res != res6_700[i - 600]);
        } else if (i < 1000 && i >= 900) {
            assert(res != res9_1000[i - 900]);
        } else {
            assert(res == true);
        }
    }
}

template<typename T, uint64_t leaf_size, uint64_t inner_size>
void multiScanDelInsert2Test(btree_t<T, leaf_size, inner_size> &btree) {
    for (int i = 0; i < 100000; i++) {
        btree.remove(i * 2);
    }

    auto addFunc = [&btree](int start, int end) {
        vector<bool> res(end - start, false);
        for (int i = 0; i < (end - start) / 2; i++) {
            res[i] = btree.insert(i + start, i + start).getValue() == -1;
            res[end - i - 1 - start] = btree.insert(end - i - 1, end - i - 1).getValue() == -1;
        }
        return res;
    };

    auto removeFunc = [&btree](int start, int end) {
        vector<bool> res(end - start, false);
        for (int i = 0; i < (end - start) / 2; i++) {
            res[i] = btree.remove(i + start, i + start);
            res[end - i - 1 - start] = btree.remove(end - i - 1, end - i - 1);
        }
        return res;
    };

    auto scanFunc = [&btree](int start, int end) {
        vector<pair<T, T>> res;
        btree.scan(start, end, true, true, 0, res);
        for (int i = 0; i < res.size(); i++) {
            ASSERT_EQ(res[i].first.getValue(), res[i].second.getValue());
        }
    };

    // thread for adding value
    auto f1 = async(addFunc, 0, 200000);

    // thread for scan
    auto f2 = async(scanFunc, 0, 500000);

    // thread for deleting value
    auto f3 = async(removeFunc, 0, 200000);

    f3.wait();
    f2.get();
    f1.get();
}


//=====================================================
// Basic Test
//=====================================================

#define CONCAT4(a, b, c, d) a ## _ ## b ## _ ## c ## _ ## d
#define CONCAT3(a, b, c)    a ## _ ## b ## _ ## c
#define CONCAT2(a, b)       a ## _ ## b

#define TestAllCase()               \
    singleInsertTest(btree);        \
    singleLookupTest(btree);        \
    singleDeleteTest(btree);        \
    singleGetValueTest(btree);      \
    singleScanTest(btree);          \
    singleScanForUpdateTest(btree); \
    multiScanDelInsert1Test(btree);
    //multiScanDelInsert2Test(btree);

#define BasicTrivialKeyTest(type, page_size)                     \
    TEST(BTreeBasicTest, CONCAT3(TrivialKey, type, page_size)) \
    {                                                            \
        using Type = TrivialKey<type>;                           \
        btree_t<Type, page_size, page_size> btree;               \
        TestAllCase();                                           \
    }

#define BasicNonTrivialKeyTest(type, page_size)                     \
    TEST(BTreeBasicTest, CONCAT3(NonTrivialKey, type, page_size)) \
    {                                                               \
        using Type = NonTrivialKey<type>;                           \
        btree_t<Type, page_size, page_size> btree;                  \
        TestAllCase();                                              \
    }

BasicTrivialKeyTest(int, 100);
BasicTrivialKeyTest(int, 1000);
BasicTrivialKeyTest(int, 4096);
BasicTrivialKeyTest(long, 100);
BasicTrivialKeyTest(long, 1000);
BasicTrivialKeyTest(long, 4096);

// BasicNonTrivialKeyTest(int, 100);
// BasicNonTrivialKeyTest(int, 1000);
// BasicNonTrivialKeyTest(int, 4096);
// BasicNonTrivialKeyTest(long, 100);
// BasicNonTrivialKeyTest(long, 1000);
// BasicNonTrivialKeyTest(long, 4096);

//=====================================================
// Concurrency Test
//=====================================================

#define spin_n_us(n)                            \
    do {                                        \
        for (int __j = 0; __j < n; ++__j) {     \
            for (int __i = 0; __i < 6; ++__i) { \
                _mm_pause();                    \
            }                                   \
        }                                       \
    } while (0)


template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_QueryDelete() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        // 1. insert first
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k{i};
            bool res = btree.insert(k, i);
            ASSERT_EQ(res, true);
        }
        // 2. spawn a thread to delete
        std::thread delete_thread{[&btree]() {
            // std::cout << "begin delete...\n";
            for (int i = 0; i < kElementCount; ++i) {
                KeyType k{i};
                bool res = btree.remove(k);
                ASSERT_EQ(res, true);
                delete[] k.transfer();
            }
            // std::cout << "finish delete\n";
        }};
        // 3. spawn 4 threads to query
        std::vector<std::thread> query_threads;
        for (int i = 0; i < 4; ++i) {
            query_threads.emplace_back([&btree](int id) {
                std::this_thread::sleep_for(std::chrono::milliseconds(id * 40));
                // std::cout << "thread #" << id << " begin query...\n";
                for (int i = 0; i < kElementCount; ++i) {
                    Value res;
                    KeyType k{i};
                    if (btree.lookup(k, res)) {
                        ASSERT_EQ(res.getValue(), i);
                    } else {
                        // std::cout << "#" << i << ": Not found.\n";
                    }
                    delete[] k.transfer();
                    spin_n_us(1);
                }
                // std::cout << "thread #" << id << " finish query\n";
            }, i);
        }
        
        for (int i = 0; i < 4; ++i) {
            query_threads[i].join();
        }
        delete_thread.join();
    }
    std::cout << '\n';
}

template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_QueryInsert() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        // 1. spawn a thread to insert
        std::thread insert_thread{[&btree]() {
            // std::cout << "begin insert...\n";
            for (int i = 0; i < kElementCount; ++i) {
                KeyType k{i};
                bool res = btree.insert(k, i);
                ASSERT_EQ(res, true);
            }
            // std::cout << "finish insert\n";
        }};
        // 2. spawn 4 threads to query
        std::vector<std::thread> query_threads;
        for (int i = 0; i < 4; ++i) {
            query_threads.emplace_back([&btree](int id) {
                std::this_thread::sleep_for(std::chrono::milliseconds(id * 5));
                // std::cout << "thread #" << id << " begin query...\n";
                for (int i = 0; i < kElementCount; ++i) {
                    Value res;
                    KeyType k{i};
                    if (btree.lookup(k, res)) {
                        ASSERT_EQ(res.getValue(), i);
                    } else {
                        // std::cout << "#" << i << ": Not found.\n";
                    }
                    delete[] k.transfer();
                    spin_n_us(2);
                }
                // std::cout << "thread #" << id << " finish query\n";
            }, i);
        }
    
        for (int i = 0; i < 4; ++i) {
            query_threads[i].join();
        }
        insert_thread.join();

        // 3. delete all
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k{i};
            bool res = btree.remove(k);
            ASSERT_EQ(res, true);
            delete[] k.transfer();
        }
    }
    std::cout << '\n';
}

template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_DeleteDelete() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        // 1. first insert
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k{i};
            bool res = btree.insert(k, i);
            ASSERT_EQ(res, true);
        }

        // 2. then spawn 4 threads to delete
        std::vector<std::thread> delete_threads;
        for (int i = 0; i < 4; ++i) {
            delete_threads.emplace_back([&btree](int id) {
                // std::cout << "thread #" << id << " begin delete...\n";
                for (int i = 0; i < kElementCount; ++i) {
                    KeyType k{i};
                    btree.remove(k);
                    delete[] k.transfer();
                }
                // std::cout << "thread #" << id << " finish delete\n";
            }, i);
        }
    
        for (int i = 0; i < 4; ++i) {
            delete_threads[i].join();
        }

        // 3. all elements should be deleted
        for (int i = 0; i < kElementCount; ++i) {
            Value res;
            KeyType k{i};
            ASSERT_FALSE(btree.lookup(k, res));
            delete[] k.transfer();
        }
    }
    std::cout << '\n';
}

template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_DeleteDeletePredicate() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        // 1. first insert
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k{i};
            bool res = btree.insert(k, i);
            ASSERT_EQ(res, true);
        }

        // 2. then spawn 4 threads to delete
        std::vector<std::thread> delete_threads;
        for (int i = 0; i < 4; ++i) {
            delete_threads.emplace_back([&btree](int id) {
                // std::cout << "thread #" << id << " begin delete...\n";
                for (int i = 0; i < kElementCount; ++i) {
                    KeyType k{i};
                    btree.remove(k, [](const Value&) -> btreeolc::RemovePredicateResult { return btreeolc::RemovePredicateResult::GOOD; });
                    delete[] k.transfer();
                }
                // std::cout << "thread #" << id << " finish delete\n";
            }, i);
        }
    
        for (int i = 0; i < 4; ++i) {
            delete_threads[i].join();
        }

        // 3. all elements should be deleted
        for (int i = 0; i < kElementCount; ++i) {
            Value res;
            KeyType k{i};
            ASSERT_FALSE(btree.lookup(k, res));
            delete[] k.transfer();
        }
    }
    std::cout << '\n';
}

template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_InsertInsert() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        // 1. spawn 4 threads to insert
        std::vector<std::thread> insert_threads;
        for (int i = 0; i < 4; ++i) {
            insert_threads.emplace_back([&btree](int id) {
                std::this_thread::sleep_for(std::chrono::milliseconds(id * 10));
                for (int i = id; i < kElementCount; i += 4) {
                    KeyType k{i};
                    bool ret = btree.insert(k, i);
                    if (!ret) {
                        delete[] k.transfer();
                    }
                }
            }, i);
        }

        for (int i = 0; i < 4; ++i) {
            insert_threads[i].join();
        }

        // 2. all elements should be inserted
        for (int i = 0; i < kElementCount; ++i) {
            Value res;
            KeyType k{i};
            ASSERT_TRUE(btree.lookup(k, res));
            ASSERT_EQ(res.getValue(), i);
            delete[] k.transfer();
        }

        // 3. delete all
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k{i};
            bool res = btree.remove(k);
            ASSERT_EQ(res, true);
            delete[] k.transfer();
        }
    }
    std::cout << '\n';
}

template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_InsertDeleteScanForUpdate() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        std::vector<KeyType> keys;
        for (int i = 0; i < kElementCount; ++i) {
            keys.push_back(KeyType(i));
        }
        std::random_shuffle(keys.begin(), keys.end());
        // 1. spawn 4 threads to insert
        std::vector<std::thread> insert_threads;
        std::vector<std::thread> delete_threads;
        for (int i = 0; i < 4; ++i) {
            insert_threads.emplace_back([&btree, &keys](int id) {
                for (int i = id; i < kElementCount; i += 4) {
                    bool ret;
                    KeyType k(keys[i]);
                    Value v = btree.insert(k, (int)keys[i].getValue(), &ret);
                    if (ret) {
                        ASSERT_EQ(v.getValue(), (int)keys[i].getValue());
                    } else {
                        delete[] k.transfer();
                    }
                }
            }, i);
        }

        std::vector<KeyType> keysForDelete = keys;
        std::random_shuffle(keysForDelete.begin(), keysForDelete.end());
        keysForDelete.resize(keysForDelete.size() / 2);
        for (int i = 0; i < 2; ++i) {
            delete_threads.emplace_back([&btree, &keysForDelete](int id) {
                std::this_thread::sleep_for(std::chrono::milliseconds((id + 1) * 40));
                for (int i = id; i < keysForDelete.size(); i += 2) {
                    KeyType k(keysForDelete[i]);
                    bool res = btree.remove(k);
                }
            }, i);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        btree.scanForUpdate(0, [](const KeyType & k, Value & v, bool lastItem){
            v.setValue(0);
            if(lastItem) {
                return true; // end on last item
            }
            return false;
        });
        
        for (int i = 0; i < 4; ++i) {
            insert_threads[i].join();
        }

        btree.scanForUpdate(0, [](const KeyType & k, Value & v, bool lastItem){
            v.setValue(0);
            if(lastItem) {
                return true; // end on last item
            }
            return false;
        });
        for (int i = 0; i < 2; ++i) {
            delete_threads[i].join();
        }
        int left = 0;
        for (int i = 0; i < kElementCount; ++i) {
            Value res;
            KeyType k(keys[i]);
            if (btree.lookup(k, res)) {
                ASSERT_EQ(res.getValue(), 0);
                left++;
            }
            
            delete[] k.transfer();
        }

        std::cout << "Before deletes, " << left << " elements" << std::endl;
        // 3. delete all
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k(keys[i]);
            bool res = btree.remove(k);
            delete[] k.transfer();
        }

        auto kv_size = sizeof(KeyType) + sizeof(Value);
        auto inner_nodes = btree.getNumInnerNodes();
        auto leaf_nodes = btree.getNumLeafNodes();
        auto tree_bytes = page_size * leaf_nodes + inner_nodes * page_size;
        std::cout << "After deletes, inner nodes " << inner_nodes << ", leaf nodes " <<  leaf_nodes << ", kv_size " << kv_size << ", tree_bytes " << tree_bytes << std::endl;;
    }
    std::cout << '\n';
}

template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_InsertDelete() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        // 1. spawn 4 threads to insert
        std::vector<std::thread> insert_threads;
        for (int i = 0; i < 4; ++i) {
            insert_threads.emplace_back([&btree](int id) {
                for (int i = 0; i < kElementCount; ++i) {
                    bool ret;
                    KeyType k{i};
                    Value v = btree.insert(k, i, &ret);
                    if (ret) {
                        ASSERT_EQ(v.getValue(), i);
                    } else {
                        delete[] k.transfer();
                    }
                }
            }, i);
        }

        // 2. then spawn 4 threads to delete
        std::vector<std::thread> delete_threads;
        for (int i = 0; i < 4; ++i) {
            delete_threads.emplace_back([&btree](int id) {
                for (int i = 0; i < kElementCount; ++i) {
                    KeyType k{i};
                    btree.remove(k);
                    delete[] k.transfer();
                }
            }, i);
        }
    
        for (int i = 0; i < 4; ++i) {
            insert_threads[i].join();
            delete_threads[i].join();
        }

        // 3. delete all 
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k{i};
            btree.remove(k);
            delete[] k.transfer();
        }
    }
    std::cout << '\n';
}

template<typename KeyType, uint64_t page_size = 4096>
void ConcurrentTest_InsertDeleteQuery() {
    // using Type = SharedPodWrapper<long>;
    btree_t<KeyType, page_size, page_size> btree;

    constexpr int kIterNum = 10;
    constexpr int kElementCount = 100000;

    for (int i = 0; i < kIterNum; ++i) {
        std::cout << '\r' << "iter " << i + 1 << "/" << kIterNum << " ..." << std::flush;

        // 1. spawn 2 threads to insert
        std::vector<std::thread> insert_threads;
        for (int i = 0; i < 2; ++i) {
            insert_threads.emplace_back([&btree](int id) {
                for (int i = 0; i < kElementCount; ++i) {
                    bool ret;
                    KeyType k{i};
                    Value v = btree.insert(k, i, &ret);
                    if (ret) {
                        ASSERT_EQ(v.getValue(), i);
                    } else {
                        delete[] k.transfer();
                    }
                }
            }, i);
        }
        // 2. then spawn 2 threads to delete
        std::vector<std::thread> delete_threads;
        for (int i = 0; i < 2; ++i) {
            delete_threads.emplace_back([&btree](int id) {
                for (int i = 0; i < kElementCount; ++i) {
                    KeyType k{i};
                    btree.remove(k);
                    delete[] k.transfer();
                }
            }, i);
        }
        // 3. then spawn 2 threads to query
        std::vector<std::thread> query_threads;
        for (int i = 0; i < 2; ++i) {
            query_threads.emplace_back([&btree](int id) {
                for (int i = 0; i < kElementCount; ++i) {
                    Value res;
                    KeyType k{i};
                    if (btree.lookup(k, res)) {
                        ASSERT_EQ(res.getValue(), i);
                    } else {
                        // std::cout << "#" << i << ": Not found.\n";
                    }
                    delete[] k.transfer();
                    spin_n_us(2);
                }
            }, i);
        }
    
        for (int i = 0; i < 2; ++i) {
            insert_threads[i].join();
            delete_threads[i].join();
            query_threads[i].join();
        }

        // 4. delete all 
        for (int i = 0; i < kElementCount; ++i) {
            KeyType k{i};
            btree.remove(k);
            delete[] k.transfer();
        }
    }
    std::cout << '\n';
}


#define ConcurrencyTrivialKeyTest(TestType, type, page_size)                     \
    TEST(BTreeConcurrencyTest, CONCAT4(TestType, TrivialKey, type, page_size)) \
    {                                                                            \
        ConcurrentTest_##TestType<TrivialKey<type>, page_size>();                \
    }

#define ConcurrencyNonTrivialKeyTest(TestType, type, page_size)                     \
    TEST(BTreeConcurrencyTest, CONCAT4(TestType, NonTrivialKey, type, page_size)) \
    {                                                                               \
        ConcurrentTest_##TestType<NonTrivialKey<type>, page_size>();                \
    }

ConcurrencyTrivialKeyTest(QueryDelete, int, 1024);

ConcurrencyTrivialKeyTest(InsertDeleteScanForUpdate, int, 4096);

//ConcurrencyNonTrivialKeyTest(QueryDelete, int, 1024);

ConcurrencyTrivialKeyTest(QueryInsert, int, 1024);
ConcurrencyTrivialKeyTest(QueryInsert, int, 4096);
// ConcurrencyNonTrivialKeyTest(QueryInsert, int, 1024);
// ConcurrencyNonTrivialKeyTest(QueryInsert, int, 4096);

ConcurrencyTrivialKeyTest(DeleteDelete, int, 1024);
ConcurrencyTrivialKeyTest(DeleteDelete, int, 4096);
// ConcurrencyNonTrivialKeyTest(DeleteDelete, int, 1024);
// ConcurrencyNonTrivialKeyTest(DeleteDelete, int, 4096);

ConcurrencyTrivialKeyTest(DeleteDeletePredicate, int, 1024);
ConcurrencyTrivialKeyTest(DeleteDeletePredicate, int, 4096);
// ConcurrencyNonTrivialKeyTest(DeleteDeletePredicate, int, 1024);
// ConcurrencyNonTrivialKeyTest(DeleteDeletePredicate, int, 4096);

ConcurrencyTrivialKeyTest(InsertInsert, int, 1024);
ConcurrencyTrivialKeyTest(InsertInsert, int, 4096);
// ConcurrencyNonTrivialKeyTest(InsertInsert, int, 1024);
// ConcurrencyNonTrivialKeyTest(InsertInsert, int, 4096);

ConcurrencyTrivialKeyTest(InsertDelete, int, 1024);
ConcurrencyTrivialKeyTest(InsertDelete, int, 4096);
// ConcurrencyNonTrivialKeyTest(InsertDelete, int, 1024);
// ConcurrencyNonTrivialKeyTest(InsertDelete, int, 4096);

ConcurrencyTrivialKeyTest(InsertDeleteQuery, int, 1024);
ConcurrencyTrivialKeyTest(InsertDeleteQuery, int, 4096);
// ConcurrencyNonTrivialKeyTest(InsertDeleteQuery, int, 1024);
// ConcurrencyNonTrivialKeyTest(InsertDeleteQuery, int, 4096);


TEST(NoopTest, DISABLED_Dummy) {
    for (int i = 1; i <= 20; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        spin_n_us(i);
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "expire: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << " us\n";
    }
}


int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
 	auto ret = RUN_ALL_TESTS();

    return ret;
}
	
