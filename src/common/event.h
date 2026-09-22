#ifndef EVENT_H
#define EVENT_H

#include <string>

struct Event
{
    long long id;
    std::string type;
    std::string source;
    std::string symbol;
    double price;
    int quantity;
    long long created_at_ms;

    Event();
    Event(long long id,
          const std::string& type,
          const std::string& source,
          const std::string& symbol,
          double price,
          int quantity,
          long long created_at_ms);

    std::string toJson() const;
    static Event fromJson(const std::string& json);
};

#endif
