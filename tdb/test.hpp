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

TEST_CASE("Fixed Table", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

#pragma pack(push, 1)
    struct Element
    {
        Element() {}

        Element(uint64_t _id, const char* first, const char* last, const char* _address) :id(_id)
        {
            strncpy_s(first_name, first, 31);
            strncpy_s(last_name, last, 31);
            strncpy_s(address, _address, 31);
        }

        auto Keys(uint64_t n)
        {
            return std::make_tuple(n + 8, n + 40, n + 72);
        }

        uint64_t id = 0;
        char first_name[32] = {};
        char last_name[32] = {};
        char address[32] = {};
    };
#pragma pack(pop)

    using R = AsyncMap<>;
    using E = SimpleTableElementBuilder <Element>;
    using SIDX = BTree< R, OrderedSurrogateStringPointer<R> >;

    using Database = DatabaseBuilder < R, FixedTable<R, E, SIDX, SIDX, SIDX> >;

    constexpr size_t lim = 10 * 1000;
    enum Tables { Table, Reserved };
    enum Indexes { FirstName, LastName, Address };

    {
        Database db("db.dat");
        auto element_table = db.Table<Table>();
        std::list<std::tuple<std::string, std::string, std::string>> check_work;

        for (size_t i = 0; i < lim; i++)
        {
            check_work.emplace_back(d8u::random::Word(20) +std::to_string(i), d8u::random::Word(20) + std::to_string(i), d8u::random::Word(20) + std::to_string(i));
            element_table.Emplace(i, std::get<FirstName>(check_work.back()).c_str(), std::get<LastName>(check_work.back()).c_str(), std::get<Address>(check_work.back()).c_str());
        }

        size_t j = 0;
        for (auto& i : check_work)
        {
            auto element = element_table.FindSurrogate< FirstName >(std::get< FirstName >(i).c_str());
            CHECK(element->id == j);

            element = element_table.FindSurrogate< LastName >(std::get< LastName >(i).c_str());
            CHECK(element->id == j);

            element = element_table.FindSurrogate< Address >(std::get< Address >(i).c_str());
            CHECK(element->id == j);

            j++;
        }
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Streaming Bucket", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    using R = AsyncMap<>;
    using Database = DatabaseBuilder < R, Stream< R, BTree< R, FuzzyHashPointer> > >;

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