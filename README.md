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

```
