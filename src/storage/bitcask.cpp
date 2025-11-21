#include "bitcask.h"
#include "crc32.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctime>
#include <cstring>
#include <vector>
#include <stdexcept>
#include <iostream>

namespace granite {
namespace storage {

BitcaskStore::BitcaskStore(const std::string& filepath) 
    : filepath_(filepath), fd_(-1) {
    
    // Open or create the data file
    fd_ = open(filepath_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open data file: " + filepath_);
    }
    
    // Rebuild index from existing data
    recover();
}

BitcaskStore::~BitcaskStore() {
    if (fd_ != -1) {
        sync();  // Ensure all data is flushed
        close(fd_);
    }
}

void BitcaskStore::set(const std::string& key, const std::string& value) {
    if (key.empty()) {
        throw std::invalid_argument("Key cannot be empty");
    }
    
    // Append to log file
    uint64_t offset = append_entry(key, value);
    
    // Update in-memory index
    KeyDirEntry entry;
    entry.file_id = 0;  // Single file for now
    entry.value_size = static_cast<uint32_t>(value.size());
    entry.value_pos = offset + sizeof(LogHeader) + key.size();
    entry.timestamp = static_cast<uint32_t>(std::time(nullptr));
    
    key_dir_[key] = entry;
    
    // Force write to disk for durability
    sync();
}

std::optional<std::string> BitcaskStore::get(const std::string& key) {
    // Look up key in index
    auto it = key_dir_.find(key);
    if (it == key_dir_.end()) {
        return std::nullopt;  // Key not found
    }
    
    const KeyDirEntry& entry = it->second;
    
    // Read value from disk
    return read_value(entry.value_pos, entry.value_size);
}

void BitcaskStore::del(const std::string& key) {
    // Remove from in-memory index
    key_dir_.erase(key);
    
    // Write a tombstone (empty value) to log
    // This ensures the deletion persists across restarts
    append_entry(key, "");
    sync();
}

void BitcaskStore::recover() {
    // Clear existing index
    key_dir_.clear();
    
    // Seek to beginning of file
    off_t file_size = lseek(fd_, 0, SEEK_END);
    if (file_size == -1) {
        throw std::runtime_error("Failed to get file size");
    }
    
    if (file_size == 0) {
        return;  // Empty file, nothing to recover
    }
    
    off_t offset = 0;
    
    std::cout << "Recovering from log file (" << file_size << " bytes)...\n";
    
    // Replay entire log file
    while (offset < file_size) {
        LogHeader header;
        
        // Read header
        ssize_t bytes_read = pread(fd_, &header, sizeof(LogHeader), offset);
        if (bytes_read != sizeof(LogHeader)) {
            std::cerr << "Warning: Corrupted entry at offset " << offset << "\n";
            break;
        }
        
        // Read key
        std::string key(header.key_size, '\0');
        pread(fd_, key.data(), header.key_size, offset + sizeof(LogHeader));
        
        // Read value
        std::string value(header.value_size, '\0');
        pread(fd_, value.data(), header.value_size, 
              offset + sizeof(LogHeader) + header.key_size);
        
        // Verify CRC
        if (!verify_entry(offset, header, key, value)) {
            std::cerr << "Warning: CRC mismatch at offset " << offset << "\n";
            break;
        }
        
        // Update index (later entries overwrite earlier ones)
        if (header.value_size > 0) {
            // Non-empty value: add/update key
            KeyDirEntry entry;
            entry.file_id = 0;
            entry.value_size = header.value_size;
            entry.value_pos = offset + sizeof(LogHeader) + header.key_size;
            entry.timestamp = header.timestamp;
            key_dir_[key] = entry;
        } else {
            // Empty value: tombstone (deletion)
            key_dir_.erase(key);
        }
        
        // Move to next entry
        offset += sizeof(LogHeader) + header.key_size + header.value_size;
    }
    
    std::cout << "Recovery complete. Loaded " << key_dir_.size() << " keys.\n";
}

void BitcaskStore::sync() {
    if (fd_ != -1) {
        fsync(fd_);  // Force write to physical disk
    }
}

uint64_t BitcaskStore::append_entry(const std::string& key, const std::string& value) {
    // Get current end of file
    off_t offset = lseek(fd_, 0, SEEK_END);
    if (offset == -1) {
        throw std::runtime_error("Failed to seek to end of file");
    }
    
    // Prepare header
    LogHeader header;
    header.timestamp = static_cast<uint32_t>(std::time(nullptr));
    header.key_size = static_cast<uint32_t>(key.size());
    header.value_size = static_cast<uint32_t>(value.size());
    
    // Compute CRC32 over timestamp + sizes + key + value
    // (excluding the crc field itself)
    std::vector<uint8_t> crc_data;
    crc_data.reserve(sizeof(uint32_t) * 3 + key.size() + value.size());
    
    // Add timestamp, key_size, value_size
    crc_data.insert(crc_data.end(), 
                    reinterpret_cast<uint8_t*>(&header.timestamp),
                    reinterpret_cast<uint8_t*>(&header.timestamp) + sizeof(uint32_t));
    crc_data.insert(crc_data.end(), 
                    reinterpret_cast<uint8_t*>(&header.key_size),
                    reinterpret_cast<uint8_t*>(&header.key_size) + sizeof(uint32_t));
    crc_data.insert(crc_data.end(), 
                    reinterpret_cast<uint8_t*>(&header.value_size),
                    reinterpret_cast<uint8_t*>(&header.value_size) + sizeof(uint32_t));
    
    // Add key and value
    crc_data.insert(crc_data.end(), key.begin(), key.end());
    crc_data.insert(crc_data.end(), value.begin(), value.end());
    
    header.crc = utils::crc32(crc_data.data(), crc_data.size());
    
    // Write header
    if (write(fd_, &header, sizeof(LogHeader)) != sizeof(LogHeader)) {
        throw std::runtime_error("Failed to write header");
    }
    
    // Write key
    if (write(fd_, key.data(), key.size()) != static_cast<ssize_t>(key.size())) {
        throw std::runtime_error("Failed to write key");
    }
    
    // Write value
    if (write(fd_, value.data(), value.size()) != static_cast<ssize_t>(value.size())) {
        throw std::runtime_error("Failed to write value");
    }
    
    return static_cast<uint64_t>(offset);
}

std::optional<std::string> BitcaskStore::read_value(uint64_t offset, uint32_t size) {
    std::string value(size, '\0');
    
    ssize_t bytes_read = pread(fd_, value.data(), size, offset);
    if (bytes_read != static_cast<ssize_t>(size)) {
        return std::nullopt;
    }
    
    return value;
}

bool BitcaskStore::verify_entry(uint64_t offset, const LogHeader& header,
                                 const std::string& key, const std::string& value) {
    // Reconstruct CRC data
    std::vector<uint8_t> crc_data;
    crc_data.reserve(sizeof(uint32_t) * 3 + key.size() + value.size());
    
    crc_data.insert(crc_data.end(), 
                    reinterpret_cast<const uint8_t*>(&header.timestamp),
                    reinterpret_cast<const uint8_t*>(&header.timestamp) + sizeof(uint32_t));
    crc_data.insert(crc_data.end(), 
                    reinterpret_cast<const uint8_t*>(&header.key_size),
                    reinterpret_cast<const uint8_t*>(&header.key_size) + sizeof(uint32_t));
    crc_data.insert(crc_data.end(), 
                    reinterpret_cast<const uint8_t*>(&header.value_size),
                    reinterpret_cast<const uint8_t*>(&header.value_size) + sizeof(uint32_t));
    crc_data.insert(crc_data.end(), key.begin(), key.end());
    crc_data.insert(crc_data.end(), value.begin(), value.end());
    
    uint32_t computed_crc = utils::crc32(crc_data.data(), crc_data.size());
    
    return computed_crc == header.crc;
}

} // namespace storage
} // namespace granite
