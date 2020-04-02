/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "../catch.hpp"

#include <filesystem>

#include "builder.hpp"
#include "bucket.hpp"
#include "btree.hpp"
#include "pages.hpp"
#include "table.hpp"

#include "d8u/random.hpp"
#include "d8u/buffer.hpp"

using namespace tdb;


TEST_CASE("Network Layer", "[tdb::]")
{
    //NETWORK LAYER TODO

    /*There is a little bit of question about how to bridge the template layer to a runtime layer :( :( :( */

    /*Short of implementing a common database this might be a problem*/

    /*Or a list of common database types... this is always the limitation of templates*/

    /*The network server is implemented for indexes, client needs implemented*/

    /*There is a deeper question holding up the show here, it is that of protocol and format*/

    /*It would be fun to implement a SQL compatability layer*/
}

TEST_CASE("Streaming Bucket", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    using R = AsyncMap<>;
    using Database = DatabaseBuilder < R, Stream< R, BTree< R, FuzzyHashPointer> >, ReserveTables<R,7> >;

    enum Tables { Streams, Reserved };



    constexpr size_t key_c = 10 * 1000, rep = 5;
    auto& keys = singleton<std::array<RandomKeyT<Key32>, key_c>>();
    auto& sizes = singleton<std::array<size_t, key_c>>();

    {
        Database db("db.dat");
        auto streams = db.Table<Streams>();

        for (size_t j = 0; j < rep; j++)
        {
            for (size_t i = 0; i < key_c; i++)
            {
                std::vector<size_t> tmp(d8u::random::Integer() % 300,i);
                sizes[i] += tmp.size() * sizeof(size_t);

                streams.Write(keys[i], d8u::byte_buffer(tmp));
            }
        }

        for (size_t i = 0; i < key_c; i++)
        {
            size_t total = 0;
            bool valid = true;
            streams.Stream(keys[i], [&](auto seg)
            {
                total += seg.size();
                auto buf = d8u::t_buffer<size_t>(seg);

                for (auto& e : buf)
                    if (e != i)
                        valid = false;

                return valid;
            });

            CHECK(total == sizes[i]);
            CHECK(valid);

            valid = true;
            auto stream = streams.Read(keys[i]);
            auto buf = d8u::t_buffer<size_t>(stream);

            for (auto& e : buf)
                if (e != i)
                    valid = false;

            CHECK(stream.size() == sizes[i]);
            CHECK(valid);

        }
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Fixed Bucket", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");



    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Endless Table", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    {

    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Fixed Table", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    struct Element
    {
        Element() {}

        Element(uint64_t si, const char* txt) :some_integer(si)
        {
            strncpy_s(indexed_string, txt, 23);
            indexed_string[23] = '\0';
        }

        auto Keys(uint64_t n)
        {
            return std::make_tuple(n + sizeof(uint64_t));
        }

        uint64_t some_integer;
        char indexed_string[24];
    };

    using R = AsyncMap<>;
    using E = SimpleTableElementBuilder <Element>;      

    using Database = DatabaseBuilder < R, FixedTable<R, E, BTree< R, OrderedSurrogateStringPointer<R> > >, ReserveTables<R, 7> >;

    enum Tables { Streams, Reserved };

    {

    }

    std::filesystem::remove_all("db.dat");
}