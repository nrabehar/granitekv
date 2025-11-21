#ifndef GRANITE_BITCASK_H
#define GRANITE_BITCASK_H

#include <string>
#include <string_view>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include <memory>

namespace granite {
namespace storage {

// Binary log entry header (stored on disk)
struct __attribute__((packed)) LogHeader {
    uint32_t crc;        // CRC32 checksum of entire entry (excluding crc field)
    uint32_t timestamp;  // Unix timestamp
    uint32_t key_size;   // Key length in bytes
    uint32_t value_size; // Value length in bytes
};

// In-memory index entry (KeyDir)
struct KeyDirEntry {
    uint32_t file_id;    // File identifier (for future multi-file support)
    uint32_t value_size; // Size of value in bytes
    uint64_t value_pos;  // File offset where value begins
    uint32_t timestamp;  // Timestamp for conflict resolution
};

// Main Bitcask storage engine
class BitcaskStore {
private:
    int fd_;                                              // File descriptor for active data file
    std::string filepath_;                                // Path to data file
    std::unordered_map<std::string, KeyDirEntry> key_dir_; // In-memory index
    
public:
    explicit BitcaskStore(const std::string& filepath);
    ~BitcaskStore();
    
    // Core operations
    void set(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    void del(const std::string& key);
    
    // Recovery and maintenance
    void recover();  // Rebuild index from log file
    void sync();     // Force flush to disk (fsync)
    
    // Statistics
    size_t key_count() const { return key_dir_.size(); }
    
private:
    // Internal helpers
    uint64_t append_entry(const std::string& key, const std::string& value);
    std::optional<std::string> read_value(uint64_t offset, uint32_t size);
    bool verify_entry(uint64_t offset, const LogHeader& header, 
                     const std::string& key, const std::string& value);
    
    // Prevent copying
    BitcaskStore(const BitcaskStore&) = delete;
    BitcaskStore& operator=(const BitcaskStore&) = delete;
};

} // namespace storage
} // namespace granite

#endif // GRANITE_BITCASK_H
