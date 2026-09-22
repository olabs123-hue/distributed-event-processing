#include "database.h"

#include <pqxx/pqxx>
#include <stdexcept>

Database::Database(const std::string& connectionString)
    : connection_(std::make_unique<pqxx::connection>(connectionString))
{
    if (!connection_->is_open()) throw std::runtime_error("Could not open PostgreSQL connection");
}

Database::~Database() = default;

void Database::initialize()
{
    pqxx::work tx(*connection_);
    tx.exec(R"SQL(
        CREATE TABLE IF NOT EXISTS processed_events (
            event_id BIGINT PRIMARY KEY,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            quantity INTEGER NOT NULL,
            created_at_ms BIGINT NOT NULL,
            processed_at_ms BIGINT NOT NULL,
            worker_id TEXT NOT NULL,
            kafka_offset BIGINT NOT NULL
        )
    )SQL");
    tx.commit();
}

bool Database::insertIfNew(const Event& event, long long kafkaOffset, const std::string& workerId)
{
    pqxx::work tx(*connection_);
    pqxx::result result = tx.exec_params(R"SQL(
        INSERT INTO processed_events
        (event_id, event_type, source, symbol, price, quantity, created_at_ms, processed_at_ms, worker_id, kafka_offset)
        VALUES ($1,$2,$3,$4,$5,$6,$7,(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,$8,$9)
        ON CONFLICT (event_id) DO NOTHING
    )SQL",
        event.id, event.type, event.source, event.symbol, event.price,
        event.quantity, event.created_at_ms, workerId, kafkaOffset);
    tx.commit();
    return result.affected_rows() == 1;
}

long long Database::countProcessed()
{
    pqxx::work tx(*connection_);
    pqxx::result result = tx.exec("SELECT COUNT(*) FROM processed_events");
    long long count = result[0][0].as<long long>();
    tx.commit();
    return count;
}
