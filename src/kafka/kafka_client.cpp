#include "kafka_client.h"

#include <iostream>
#include <stdexcept>

#include <rdkafkacpp.h>

namespace
{
class ErrorCb : public RdKafka::EventCb
{
public:
    void event_cb(RdKafka::Event& event) override
    {
        if (event.severity() == RdKafka::Event::EVENT_ERROR)
        {
            std::cerr << "[Kafka] " << event.str() << std::endl;
        }
    }
};
}

KafkaProducerClient::KafkaProducerClient(const std::string& brokers, const std::string& clientId)
    : producer_(nullptr)
{
    std::string error;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", brokers, error);
    conf->set("client.id", clientId, error);
    conf->set("acks", "all", error);
    conf->set("enable.idempotence", "true", error);
    conf->set("compression.type", "lz4", error);

    if (!error.empty()) throw std::runtime_error(error);
    producer_ = RdKafka::Producer::create(conf.get(), error);
    if (!producer_) throw std::runtime_error("Kafka producer creation failed: " + error);
}

KafkaProducerClient::~KafkaProducerClient()
{
    if (producer_) producer_->flush(5000);
    delete producer_;
}

bool KafkaProducerClient::send(const std::string& topic,
                               const std::string& key,
                               const std::string& payload)
{
    RdKafka::ErrorCode result = producer_->produce(
        topic,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(payload.data()), payload.size(),
        &key, nullptr);

    if (result != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "[Kafka] produce failed: " << RdKafka::err2str(result) << std::endl;
        return false;
    }
    producer_->poll(0);
    return true;
}

void KafkaProducerClient::flush(int timeoutMs)
{
    producer_->flush(timeoutMs);
}

KafkaConsumerClient::KafkaConsumerClient(const std::string& brokers,
                                         const std::string& groupId,
                                         const std::string& clientId)
    : consumer_(nullptr)
{
    std::string error;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", brokers, error);
    conf->set("group.id", groupId, error);
    conf->set("client.id", clientId, error);
    conf->set("enable.auto.commit", "false", error);
    conf->set("auto.offset.reset", "earliest", error);
    conf->set("enable.partition.eof", "false", error);
    if (!error.empty()) throw std::runtime_error(error);

    consumer_ = RdKafka::KafkaConsumer::create(conf.get(), error);
    if (!consumer_) throw std::runtime_error("Kafka consumer creation failed: " + error);
}

KafkaConsumerClient::~KafkaConsumerClient()
{
    if (consumer_) consumer_->close();
    delete consumer_;
}

bool KafkaConsumerClient::subscribe(const std::string& topic)
{
    std::vector<std::string> topics{topic};
    RdKafka::ErrorCode result = consumer_->subscribe(topics);
    if (result != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "[Kafka] subscribe failed: " << result << std::endl;
        return false;
    }
    return true;
}

bool KafkaConsumerClient::consume(std::string& key,
                                  std::string& payload,
                                  long long& offset,
                                  int timeoutMs)
{
    std::unique_ptr<RdKafka::Message> message(consumer_->consume(timeoutMs));
    if (message->err() == RdKafka::ERR__TIMED_OUT) return false;
    if (message->err() != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "[Kafka] consume failed: " << message->errstr() << std::endl;
        return false;
    }

    payload.assign(static_cast<const char*>(message->payload()), message->len());
    if (message->key()) key = *message->key(); else key.clear();
    offset = message->offset();
    return true;
}

bool KafkaConsumerClient::commit()
{
    RdKafka::ErrorCode result = consumer_->commitSync();
    if (result != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "[Kafka] commit failed: " << RdKafka::err2str(result) << std::endl;
        return false;
    }
    return true;
}
