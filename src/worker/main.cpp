#include "../common/event.h"
#include "../db/database.h"
#include "../kafka/kafka_client.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

int main()
{
    const std::string brokers = std::getenv("KAFKA_BROKERS") ? std::getenv("KAFKA_BROKERS") : "localhost:9092";
    const std::string topic = std::getenv("KAFKA_TOPIC") ? std::getenv("KAFKA_TOPIC") : "market-events";
    const std::string group = std::getenv("KAFKA_GROUP") ? std::getenv("KAFKA_GROUP") : "event-workers";
    const std::string workerId = std::getenv("WORKER_ID") ? std::getenv("WORKER_ID") : "worker-local";
    const std::string db = std::getenv("DATABASE_URL") ? std::getenv("DATABASE_URL") : "postgresql://event_user:event_password@localhost:5432/events";

    std::unique_ptr<Database> database(new Database(db));
    database->initialize();
    KafkaConsumerClient consumer(brokers, group, workerId);
    if (!consumer.subscribe(topic)) return 1;

    std::cout << "[" << workerId << "] consuming " << topic << std::endl;

    while (true)
    {
        std::string key;
        std::string payload;
        long long offset = -1;
        if (!consumer.consume(key, payload, offset, 1000)) continue;

        // Retry the *same consumed record* on DB failure instead of consuming
        // the next offset and accidentally committing past a missing event.
        bool persisted = false;
        while (!persisted)
        {
            try
            {
                if (!database)
                {
                    database.reset(new Database(db));
                    database->initialize();
                }
                Event event = Event::fromJson(payload);
                const bool inserted = database->insertIfNew(event, offset, workerId);
                // Keep normal operational logs, but avoid benchmark-only per-event
                // stdout overhead that can dominate small synthetic throughput runs.
                if (std::getenv("BENCHMARK_QUIET") == nullptr)
                {
                    if (inserted)
                        std::cout << "[" << workerId << "] processed event " << event.id << std::endl;
                    else
                        std::cout << "[" << workerId << "] duplicate ignored: " << event.id << std::endl;
                }
                persisted = true;
            }
            catch (const std::exception& ex)
            {
                database.reset(); // Reconnect after Postgres becomes available.
                std::cerr << "[" << workerId << "] persistence failure: " << ex.what()
                          << "; retrying same event" << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }

        // Commit only after successful database insertion or idempotent conflict.
        // If commit fails, the message may replay (safe via event primary key).
        if (!consumer.commit())
            std::cerr << "[" << workerId << "] Kafka commit failed; replay possible" << std::endl;

    }
}
