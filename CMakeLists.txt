cmake_minimum_required(VERSION 3.16)

project(hardware_telemetry)

set(CMAKE_CXX_STANDARD 20)

find_path(POSTGRESQL_INCLUDE_DIR libpq-fe.h
	PATHS /usr/include/postgresql /usr/local/include/postgresql)

find_library(POSTGRESQL_LIBRARY pq
	PATHS /usr/lib /usr/lib64 /usr/lib/x86_64-linux-gnu /usr/local/lib)

# Common socket library
add_library(socket common/socket/TCPSocket.cpp)
target_include_directories(socket PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Regional Collector (multi-city, topology-driven)
add_executable(regional_collector collectors/regional/main.cpp)
target_include_directories(regional_collector PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(regional_collector socket)

# Hierarchical Aggregator (regional/continent/global, topology-driven)
add_executable(hierarchical_aggregator aggregator/hierarchical/main.cpp)
target_include_directories(hierarchical_aggregator PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(hierarchical_aggregator socket)

# Timescale writer (socket-fed prototype writer)
add_executable(timescale_writer timescale_writer/main.cpp)
target_include_directories(timescale_writer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(timescale_writer socket)

if (POSTGRESQL_INCLUDE_DIR AND POSTGRESQL_LIBRARY)
	target_include_directories(timescale_writer PRIVATE ${POSTGRESQL_INCLUDE_DIR})
	target_link_libraries(timescale_writer ${POSTGRESQL_LIBRARY})
	target_compile_definitions(timescale_writer PRIVATE HARDWARE_TELEMETRY_HAS_LIBPQ=1)
endif()

# Simulator
add_executable(simulator simulator/main.cpp)