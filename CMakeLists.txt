cmake_minimum_required(VERSION 3.20)
project(distributed_event_processing LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGET rdkafka++ rdkafka)
pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)

add_library(event_core src/common/event.cpp)
target_include_directories(event_core PUBLIC src/common)

add_library(kafka_client src/kafka/kafka_client.cpp)
target_include_directories(kafka_client PUBLIC src/kafka)
target_link_libraries(kafka_client PUBLIC PkgConfig::RDKAFKA)

add_library(database src/db/database.cpp)
target_include_directories(database PUBLIC src/db)
target_link_libraries(database PUBLIC PkgConfig::PQXX)

add_executable(event_producer src/producer/main.cpp)
target_link_libraries(event_producer PRIVATE event_core kafka_client Threads::Threads)

add_executable(event_worker src/worker/main.cpp)
target_link_libraries(event_worker PRIVATE event_core kafka_client database Threads::Threads)

enable_testing()
add_executable(test_event tests/test_event.cpp)
target_link_libraries(test_event PRIVATE event_core)
add_test(NAME event_serialization COMMAND test_event)

add_executable(event_serialization_benchmark benchmarks/event_serialization_benchmark.cpp)
target_link_libraries(event_serialization_benchmark PRIVATE event_core)
