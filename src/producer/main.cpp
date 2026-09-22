#include "../common/event.h"
#include "../kafka/kafka_client.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>

long long nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

int main()
{
    const std::string brokers = std::getenv("KAFKA_BROKERS") ? std::getenv("KAFKA_BROKERS") : "localhost:9092";
    const std::string topic = std::getenv("KAFKA_TOPIC") ? std::getenv("KAFKA_TOPIC") : "market-events";
    const int numberOfEvents = std::getenv("EVENT_COUNT") ? std::atoi(std::getenv("EVENT_COUNT")) : 100;

    KafkaProducerClient producer(brokers, "event-producer");
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> price(100.0, 500.0);
    std::uniform_int_distribution<int> quantity(1, 1000);
    const char* symbols[] = {"AAPL", "MSFT", "NVDA", "AMZN"};

    for (int i = 1; i <= numberOfEvents; ++i)
    {
        Event event(i, "MARKET_DATA", "SIMULATOR", symbols[(i - 1) % 4], price(rng), quantity(rng), nowMs());
        if (!producer.send(topic, std::to_string(event.id), event.toJson())) return 1;
    }

    producer.flush(10000);
    std::cout << "Produced " << numberOfEvents << " events." << std::endl;
    return 0;
}
