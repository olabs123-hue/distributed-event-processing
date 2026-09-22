#ifndef KAFKA_CLIENT_H
#define KAFKA_CLIENT_H

#include <memory>
#include <string>

namespace RdKafka {
class Producer;
class KafkaConsumer;
class Topic;
}

class KafkaProducerClient
{
public:
    KafkaProducerClient(const std::string& brokers, const std::string& clientId);
    ~KafkaProducerClient();
    bool send(const std::string& topic, const std::string& key, const std::string& payload);
    void flush(int timeoutMs);

private:
    RdKafka::Producer* producer_;
};

class KafkaConsumerClient
{
public:
    KafkaConsumerClient(const std::string& brokers,
                        const std::string& groupId,
                        const std::string& clientId);
    ~KafkaConsumerClient();
    bool subscribe(const std::string& topic);
    bool consume(std::string& key, std::string& payload, long long& offset, int timeoutMs);
    bool commit();

private:
    RdKafka::KafkaConsumer* consumer_;
};

#endif
