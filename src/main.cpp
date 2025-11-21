#include "bitcask.h"
#include <iostream>

using namespace granite::storage;

int main() {
    std::cout << "GraniteKV - Bitcask Storage Engine\n";
    std::cout << "===================================\n\n";
    
    try {
        BitcaskStore store("granitekv.dat");
        
        // Simple demonstration
        store.set("name", "GraniteKV");
        store.set("version", "1.0");
        store.set("type", "key-value store");
        
        std::cout << "Stored 3 keys.\n";
        std::cout << "name: " << store.get("name").value_or("N/A") << "\n";
        std::cout << "version: " << store.get("version").value_or("N/A") << "\n";
        std::cout << "type: " << store.get("type").value_or("N/A") << "\n";
        std::cout << "\nTotal keys: " << store.key_count() << "\n";
        
        std::cout << "\nStorage engine ready. Run tests with './test_bitcask'\n";
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}
