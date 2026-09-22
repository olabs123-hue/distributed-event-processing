#include "../src/common/event.h"

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

long long nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv)
{
    const int iterations = (argc > 1) ? std::atoi(argv[1]) : 1000000;
    if (iterations <= 0)
    {
        std::cerr << "iterations must be positive\n";
        return 1;
    }

    Event event(42, "MARKET_DATA", "BENCHMARK", "AAPL", 187.32, 100, 1710000000000LL);

    std::string json;
    json.reserve(256);

    const long long serializeStart = nowNs();
    for (int i = 0; i < iterations; ++i)
    {
        json = event.toJson();
    }
    const long long serializeEnd = nowNs();

    volatile long long checksum = 0;
    const long long roundTripStart = nowNs();
    for (int i = 0; i < iterations; ++i)
    {
        Event decoded = Event::fromJson(json);
        checksum += decoded.id + decoded.quantity;
    }
    const long long roundTripEnd = nowNs();

    const double serializeSeconds = (serializeEnd - serializeStart) / 1e9;
    const double roundTripSeconds = (roundTripEnd - roundTripStart) / 1e9;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "iterations=" << iterations << '\n';
    std::cout << "payload_bytes=" << json.size() << '\n';
    std::cout << "serialization_seconds=" << serializeSeconds << '\n';
    std::cout << "serialization_ops_per_second=" << (iterations / serializeSeconds) << '\n';
    std::cout << "round_trip_seconds=" << roundTripSeconds << '\n';
    std::cout << "round_trip_ops_per_second=" << (iterations / roundTripSeconds) << '\n';
    std::cout << "checksum=" << checksum << '\n';

    return 0;
}
