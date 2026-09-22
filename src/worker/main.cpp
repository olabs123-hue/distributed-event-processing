#include "../common/event.h"
#include "../db/database.h"
#include "../kafka/kafka_client.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

int main()
{
    const std::string brokers = std::getenv("KAFKA_BROKERS") ? std::getenv("KAFKA_BROKERS") : "localhost:9092";
    const std::string topic = std::getenv("KAFKA_TOPIC") ? std::getenv("KAFKA_TOPIC") : "market-events";
    const std::string group = std::getenv("KAFKA_GROUP") ? std::getenv("KAFKA_GROUP") : "event-workers";
    const std::string workerId = std::getenv("WORKER_ID") ? std::getenv("WORKER_ID") : "worker-local";
    const std::string db = std::getenv("DATABASE_URL") ? std::getenv("DATABASE_URL") : "postgresql://event_user:event_password@localhost:5432/events";

    Database database(db);
    database.initialize();
    KafkaConsumerClient consumer(brokers, group, workerId);
    if (!consumer.subscribe(topic)) return 1;

    std::cout << "[" << workerId << "] consuming " << topic << std::endl;

    while (true)
    {
        std::string key;
        std::string payload;
        long long offset = -1;
        if (!consumer.consume(key, payload, offset, 1000)) continue;

        try
        {
            Event event = Event::fromJson(payload);
            bool inserted = database.insertIfNew(event, offset, workerId);
            if (inserted)
                std::cout << "[" << workerId << "] processed event " << event.id << std::endl;
            else
                std::cout << "[" << workerId << "] duplicate ignored: " << event.id << std::endl;

            // Commit only after the database transaction succeeds.
            if (!consumer.commit())
            {
                std::cerr << "Commit failed; event will be redelivered." << std::endl;
            }
        }
        catch (const std::exception& ex)
        {
            std::cerr << "Processing failure: " << ex.what() << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
    }
}
