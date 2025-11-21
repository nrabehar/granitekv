#include "bitcask.h"
#include <iostream>
#include <cassert>

using namespace granite::storage;

void test_basic_operations() {
    std::cout << "\n=== Test 1: Basic SET/GET/DEL ===\n";
    
    BitcaskStore store("test_data.dat");
    
    // Test SET and GET
    store.set("hello", "world");
    store.set("foo", "bar");
    store.set("key123", "value456");
    
    auto val1 = store.get("hello");
    assert(val1.has_value() && val1.value() == "world");
    std::cout << "✓ GET hello = " << val1.value() << "\n";
    
    auto val2 = store.get("foo");
    assert(val2.has_value() && val2.value() == "bar");
    std::cout << "✓ GET foo = " << val2.value() << "\n";
    
    // Test key not found
    auto val3 = store.get("nonexistent");
    assert(!val3.has_value());
    std::cout << "✓ GET nonexistent = (not found)\n";
    
    // Test DEL
    store.del("foo");
    auto val4 = store.get("foo");
    assert(!val4.has_value());
    std::cout << "✓ DELETE foo successful\n";
    
    std::cout << "Keys in store: " << store.key_count() << "\n";
}

void test_recovery() {
    std::cout << "\n=== Test 2: Recovery (Persistence) ===\n";
    
    {
        BitcaskStore store("test_recovery.dat");
        store.set("persistent", "data");
        store.set("key1", "value1");
        store.set("key2", "value2");
        std::cout << "Written 3 keys, closing store...\n";
    } // Store destructor called, file closed
    
    // Reopen - should recover data
    {
        BitcaskStore store("test_recovery.dat");
        std::cout << "Store reopened.\n";
        
        auto val = store.get("persistent");
        assert(val.has_value() && val.value() == "data");
        std::cout << "✓ Recovered: persistent = " << val.value() << "\n";
        
        assert(store.key_count() == 3);
        std::cout << "✓ Recovered " << store.key_count() << " keys\n";
    }
}

void test_overwrites() {
    std::cout << "\n=== Test 3: Overwriting Values ===\n";
    
    BitcaskStore store("test_overwrite.dat");
    
    store.set("counter", "1");
    store.set("counter", "2");
    store.set("counter", "3");
    
    auto val = store.get("counter");
    assert(val.has_value() && val.value() == "3");
    std::cout << "✓ After 3 overwrites, counter = " << val.value() << "\n";
    
    // Should only have 1 key in index
    assert(store.key_count() == 1);
    std::cout << "✓ Index contains " << store.key_count() << " key (correct)\n";
}

void test_large_values() {
    std::cout << "\n=== Test 4: Large Values ===\n";
    
    BitcaskStore store("test_large.dat");
    
    // Create a 1MB value
    std::string large_value(1024 * 1024, 'X');
    store.set("large", large_value);
    
    auto val = store.get("large");
    assert(val.has_value() && val.value().size() == 1024 * 1024);
    std::cout << "✓ Stored and retrieved 1MB value\n";
}

int main() {
    std::cout << "=== Bitcask Storage Engine - Phase 1 Tests ===\n";
    
    try {
        test_basic_operations();
        test_recovery();
        test_overwrites();
        test_large_values();
        
        std::cout << "\n✅ All tests passed!\n";
        std::cout << "\nPhase 1 Complete: Storage engine with persistence and recovery working.\n";
        
    } catch (const std::exception& e) {
        std::cerr << "\n❌ Test failed: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}
