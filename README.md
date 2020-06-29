# template_db
A database written in Modern C++ from the ground up.

## Getting Started

```cpp
#include "tbd/tdb.hpp"

using namespace tdb;

//...

using R = AsyncMap<>;
using IndexedOneToManyStringSearch = DatabaseBuilder < R, StringSearch<R> >;
enum Tables { Lookup };

IndexedOneToManyStringSearch db("otmsdb.dat");
auto search = db.Table<Lookup>();

size_t v = 0; //Using this as a table element placeholder
search.Insert("this is a string", gsl::span<uint8_t>((uint8_t*)&v,sizeof(size_t)));

v++;
search.Insert("different string this this", gsl::span<uint8_t>((uint8_t*)&v, sizeof(size_t)));

assert(search.Find<size_t>("this").size() == 2);
assert(search.Find<size_t>("string").size() == 2);
assert(search.Find<size_t>("different").size() == 1);
assert(search.Find<size_t>(" is").size() == 1);

//...

using SEGLOOKUP = BTree< R, OrderedSegmentPointer<uint64_t> >;
using Database = DatabaseBuilder < R, SEGLOOKUP >;
using Segment = std::pair<uint64_t, uint64_t>;

enum Tables { Segments };

{
    Database db("db.dat");
    auto segments = db.Table<Segments>();

    segments.Insert(Segment(15,5), uint64_t(1));

    assert(*segments.Find(15) == 1);
    assert(*segments.Find(Segment(14,2)) == 1);
    assert(!segments.Find(14));
    assert(*segments.Find(19) == 1);
    assert(*segments.Find(Segment(18, 2)) == 1);
    assert(!segments.Find(20));
}

//...

struct Element
{
    Element() {}

    Element(uint64_t _id, const char* first, const char* last, const char* _address) :id(_id)
    {
        strncpy_s(first_name, first, 11);
        strncpy_s(last_name, last, 11);
        strncpy_s(address, _address, 31);
    }

    auto Keys(uint64_t n)
    {
        return std::make_tuple(n + 8, n + 20, n + 32);
    }

    uint64_t id = 0;
    char first_name[12] = {};
    char last_name[12] = {};
    char address[32] = {};
};

using E = SimpleTableElementBuilder <Element>;
using SIDX = BTree< R, MultiSurrogateStringPointer<R> >;

using Database = DatabaseBuilder < R, FixedTable<R, E, SIDX, SIDX, SIDX> >;

enum Tables { Table };
enum Indexes { FirstName, LastName, Address };

{
    Database db("db.dat");
    auto element_table = db.Table<Table>();
        
    element_table.Emplace(0, "John", "Doe", "777 Brook Way");
    element_table.Emplace(1, "Joe", "Dohn", "666 Other Way");
    element_table.Emplace(2, "Little", "Timmy", "69420 Pog Champ");
    element_table.Emplace(3, "John", "Again", "5497 W 123 E");

    element_table.MultiFindSurrogate< FirstName >([&](auto& element)
    {
        assert (element.id == 0 || element.id == 3);
    },"John");

    element_table.MultiFindSurrogate< LastName >([&](auto& element)
    {
        assert(element.id == 2);
    }, "Timmy");
}

```
