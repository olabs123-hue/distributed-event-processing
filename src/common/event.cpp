#include "event.h"

#include <iomanip>
#include <sstream>
#include <stdexcept>

Event::Event()
    : id(0), type(""), source(""), symbol(""), price(0.0), quantity(0), created_at_ms(0)
{
}

Event::Event(long long id,
             const std::string& type,
             const std::string& source,
             const std::string& symbol,
             double price,
             int quantity,
             long long created_at_ms)
    : id(id), type(type), source(source), symbol(symbol), price(price), quantity(quantity), created_at_ms(created_at_ms)
{
}

std::string Event::toJson() const
{
    std::ostringstream out;
    out << std::fixed << std::setprecision(4);
    out << "{"
        << "\"id\":" << id << ","
        << "\"type\":\"" << type << "\","
        << "\"source\":\"" << source << "\","
        << "\"symbol\":\"" << symbol << "\","
        << "\"price\":" << price << ","
        << "\"quantity\":" << quantity << ","
        << "\"created_at_ms\":" << created_at_ms
        << "}";
    return out.str();
}

Event Event::fromJson(const std::string& json)
{
    // Minimal parser for the controlled event schema used by this project.
    // The project deliberately keeps the wire format simple for the first Kafka phase.
    Event event;
    auto extractString = [&json](const std::string& key) -> std::string {
        const std::string token = "\"" + key + "\":\"";
        std::size_t start = json.find(token);
        if (start == std::string::npos) throw std::runtime_error("Missing field: " + key);
        start += token.size();
        std::size_t end = json.find('"', start);
        if (end == std::string::npos) throw std::runtime_error("Invalid string field: " + key);
        return json.substr(start, end - start);
    };
    auto extractNumber = [&json](const std::string& key) -> std::string {
        const std::string token = "\"" + key + "\":";
        std::size_t start = json.find(token);
        if (start == std::string::npos) throw std::runtime_error("Missing field: " + key);
        start += token.size();
        std::size_t end = json.find_first_of(",}", start);
        if (end == std::string::npos) throw std::runtime_error("Invalid number field: " + key);
        return json.substr(start, end - start);
    };

    event.id = std::stoll(extractNumber("id"));
    event.type = extractString("type");
    event.source = extractString("source");
    event.symbol = extractString("symbol");
    event.price = std::stod(extractNumber("price"));
    event.quantity = std::stoi(extractNumber("quantity"));
    event.created_at_ms = std::stoll(extractNumber("created_at_ms"));
    return event;
}
