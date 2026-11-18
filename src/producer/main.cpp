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
    const long long startId = std::getenv("START_ID") ? std::atoll(std::getenv("START_ID")) : 1;
    if (numberOfEvents < 1 || numberOfEvents > 1000000 || startId < 1) {
        std::cerr << "Invalid synthetic event range." << std::endl;
        return 2;
    }

    KafkaProducerClient producer(brokers, "event-producer");
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> price(100.0, 500.0);
    std::uniform_int_distribution<int> quantity(1, 1000);
    const char* symbols[] = {"AAPL", "MSFT", "NVDA", "AMZN"};

    for (int i = 0; i < numberOfEvents; ++i)
    {
        long long id = startId + i;
        Event event(id, "MARKET_DATA", "SIMULATOR", symbols[(id-1) % 4],
                    price(rng), quantity(rng), nowMs());
        if (!producer.send(topic, std::to_string(event.id), event.toJson())) return 1;
    }
    producer.flush(30000);
    std::cout << "Enqueued " << numberOfEvents << " synthetic events from ID "
              << startId << "." << std::endl;
    return 0;
}
