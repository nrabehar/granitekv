#ifndef GRANITE_CRC32_H
#define GRANITE_CRC32_H

#include <cstdint>
#include <cstddef>

namespace granite {
namespace utils {

// Compute CRC32 checksum for data integrity
uint32_t crc32(const void* data, size_t length);

} // namespace utils
} // namespace granite

#endif // GRANITE_CRC32_H
