cmake_minimum_required(VERSION 3.10)
project(granitekv)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(
	inc
	src/storage
	src/network
	src/utils
)

# Library files (exclude main.cpp)
file(GLOB LIB_FILES
	src/storage/*.cpp
	src/network/*.cpp
	src/utils/*.cpp
)

# Create a library from the core functionality
if(LIB_FILES)
	add_library(granitekv_lib STATIC ${LIB_FILES})
endif()

# Main executable
add_executable(granitekv src/main.cpp)
if(LIB_FILES)
	target_link_libraries(granitekv PRIVATE granitekv_lib)
endif()

# Tests
file(GLOB TEST_FILES tests/*.cpp)
if(TEST_FILES)
	enable_testing()
	add_executable(test_bitcask ${TEST_FILES})
	if(LIB_FILES)
		target_link_libraries(test_bitcask PRIVATE granitekv_lib)
	endif()
	add_test(NAME bitcask_tests COMMAND test_bitcask)
endif()

# Old test framework files (if they exist)
file(GLOB OLD_TEST_FILES tests/test_*.cpp)
if(OLD_TEST_FILES AND NOT TEST_FILES)
	enable_testing()
	foreach(test_file ${OLD_TEST_FILES})
		get_filename_component(test_name ${test_file} NAME_WE)
		add_executable(${test_name} ${test_file})
		if(LIB_FILES)
			target_link_libraries(${test_name} PRIVATE granitekv_lib)
		endif()
		add_test(NAME ${test_name} COMMAND ${test_name})
	endforeach()
endif()
