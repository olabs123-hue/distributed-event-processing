#ifndef DATABASE_H
#define DATABASE_H

#include "../common/event.h"

#include <memory>
#include <string>

namespace pqxx { class connection; }

class Database
{
public:
    explicit Database(const std::string& connectionString);
    ~Database();

    void initialize();
    bool insertIfNew(const Event& event, long long kafkaOffset, const std::string& workerId);
    long long countProcessed();

private:
    std::unique_ptr<pqxx::connection> connection_;
};

#endif
