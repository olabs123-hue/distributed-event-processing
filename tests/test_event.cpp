#include "../src/common/event.h"

#include <cassert>
#include <iostream>

int main()
{
    Event original(42, "MARKET_DATA", "TEST", "AAPL", 123.45, 50, 1000);
    Event decoded = Event::fromJson(original.toJson());

    assert(decoded.id == original.id);
    assert(decoded.type == original.type);
    assert(decoded.source == original.source);
    assert(decoded.symbol == original.symbol);
    assert(decoded.quantity == original.quantity);
    assert(decoded.created_at_ms == original.created_at_ms);

    std::cout << "Event serialization test passed." << std::endl;
    return 0;
}
