# template_db
A template database written Modern C++ from the ground up.

##Getting Started

```cpp
#include "tbd/database.h[["


    constexpr auto lim = 10 * 1000;

    LargeIndex dx("db.dat");

    std::array<RandomKeyT<Key32>, lim> keys;

    for (size_t i = 0; i < lim; i++)
    {
        assert(nullptr != dx.Insert(keys[i], uint64_t(i)).first);
    }

    for (size_t i = 0; i < lim; i++)
    {
        auto res = dx.Find(keys[i]);
        assert((nullptr != res && *res == i));
    }

```

