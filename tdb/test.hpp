/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <execution>
#include <numeric>

#include "../catch.hpp"
#include "d8u/util.hpp"

#include "database.hpp"

using namespace tdb;
using namespace d8u::util;



TEST_CASE("Network Layer", "[tdb::]")
{
    //NETWORK LAYER TODO
}

struct StringIndexKeyIndexedTableElement
{
    static const size_t max_pages = 8190;
    static const size_t page_elements = 1024;
    static const size_t lookup_padding = 0;
    static const size_t page_padding = 0;

    using Int = uint64_t;
    using Link = uint64_t;

    StringIndexKeyIndexedTableElement() {}

    StringIndexKeyIndexedTableElement(uint64_t si, const char* txt, const Key32 & _key) 
        : key(_key)
        , some_integer(si)
    {
        strncpy_s(indexed_string, txt, 23);
        indexed_string[23] = '\0';
    }

    auto Keys()
    {
        return std::make_tuple(indexed_string,key);
    }

    uint64_t some_integer;
    char indexed_string[24];
    Key32 key;
};

TEST_CASE("String and Key Indexed Table", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    {
        Table<StringIndexKeyIndexedTableElement,1024*1024, _StringIndexKeyIndexTable<StringIndexKeyIndexedTableElement, 1024 * 1024> > dx("db.dat");

        std::array<RandomKeyT<Key32>, 4> keys;

        dx.Emplace(0, "help",keys[0]);
        dx.Emplace(1, "something", keys[1]);
        dx.Emplace(2, "else", keys[2]);
        dx.Emplace(3, "qqzzz", keys[3]);

        CHECK((0 == dx.Find<0>(string_viewz("help"))->some_integer));
        CHECK((1 == dx.Find<0>(string_viewz("something"))->some_integer));
        CHECK((2 == dx.Find<0>(string_viewz("else"))->some_integer));
        CHECK((3 == dx.Find<0>(string_viewz("qqzzz"))->some_integer));

        CHECK((0 == dx.Find<1>(keys[0])->some_integer));
        CHECK((1 == dx.Find<1>(keys[1])->some_integer));
        CHECK((2 == dx.Find<1>(keys[2])->some_integer));
        CHECK((3 == dx.Find<1>(keys[3])->some_integer));
    }

    std::filesystem::remove_all("db.dat");
}



struct StringIndexedTableElement
{
    static const size_t max_pages = 8190;
    static const size_t page_elements = 2048;
    static const size_t lookup_padding = 0;
    static const size_t page_padding = 0;

    using Int = uint64_t;
    using Link = uint64_t;

    StringIndexedTableElement() {}

    StringIndexedTableElement(uint64_t si, const char* txt) :some_integer(si)
    {
        strncpy_s(indexed_string, txt, 23);
        indexed_string[23] = '\0';
    }

    auto Keys() 
    { 
        return std::make_tuple(indexed_string);
    }

    uint64_t some_integer;
    char indexed_string[24];
};

TEST_CASE("String Indexed Table", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    {
        Table<StringIndexedTableElement> dx("db.dat");

        dx.Emplace(0, "help");
        dx.Emplace(1, "something");
        dx.Emplace(2, "else");
        dx.Emplace(3, "qqzzz");

        CHECK((0 == dx.Find<0>(string_viewz("help"))->some_integer));
        CHECK((1 == dx.Find<0>(string_viewz("something"))->some_integer));
        CHECK((2 == dx.Find<0>(string_viewz("else"))->some_integer));
        CHECK((3 == dx.Find<0>(string_viewz("qqzzz"))->some_integer));
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Surrogate String Simple", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    {
        SmallStringIndex dx("db.dat");

        dx.Insert(dx.SetObject(string_viewz("Surrogate String")).second,uint64_t(0));
        dx.Insert(dx.SetObject(string_viewz("Surrogate WString")).second, uint64_t(1));
        dx.Insert(dx.SetObject(string_viewz("abcdefg")).second, uint64_t(2));
        dx.Insert(dx.SetObject(string_viewz("zyxwvu")).second, uint64_t(3));

        CHECK((0 == *dx.Find(string_viewz("Surrogate String"))));
        CHECK((1 == *dx.Find(string_viewz("Surrogate WString"))));
        CHECK((2 == *dx.Find(string_viewz("abcdefg"))));
        CHECK((3 == *dx.Find(string_viewz("zyxwvu"))));
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Persistent Incidental Simple", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    auto k = Key32(string_view("Test"));
    auto k2 = Key32(string_view("Test2"));

    {
        SmallIndex dx("db.dat");  

        CHECK(true == dx.InsertObject(k, k));
        CHECK(false == dx.InsertObject(k, k));
        CHECK(nullptr != dx.FindObject(k));
    }

    {
        SmallIndex dx("db.dat");

        CHECK(true == dx.InsertObject(k2, k2));
        CHECK(false == dx.InsertObject(k2, k2));
        CHECK(nullptr != dx.FindObject(k2));

        CHECK(dx.FindObject(k) != dx.FindObject(k2));
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Simple Index", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    {
        SmallIndex dx("db.dat");

        auto k = Key32(string_view("Test"));

        CHECK(nullptr != dx.Insert(k, uint64_t(99)).first);
        CHECK(nullptr != dx.Find(k));
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Simple Hashmap", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    {
        SmallHashmap dx("db.dat");

        auto k = Key32(string_view("Test"));

        CHECK(nullptr != dx.Insert(k, uint64_t(99)).first);
        CHECK(nullptr != dx.Find(k));
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("10,000 Inserts", "[tdb::]")
{
    constexpr auto lim = 10 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        SmallIndex dx("db.dat");

        std::array<RandomKeyT<Key32>, lim> keys;

        for (size_t i = 0; i < lim; i++)
        {
            CHECK(nullptr != dx.Insert(keys[i], uint64_t(i)).first);
        }

        for (size_t i = 0; i < lim; i++)
        {
            auto res = dx.Find(keys[i]);

            CHECK((nullptr != res && *res == i));
        }
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("10,000 Hashmap Inserts", "[tdb::]")
{
    constexpr auto lim = 10 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        SmallHashmap dx("db.dat");

        std::array<RandomKeyT<Key32>, lim> keys;

        for (size_t i = 0; i < lim; i++)
        {
            CHECK(nullptr != dx.Insert(keys[i], uint64_t(i)).first);
        }

        for (size_t i = 0; i < lim; i++)
        {
            auto res = dx.Find(keys[i]);
            CHECK((nullptr != res && *res == i));
        }
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("100,000 Inserts", "[tdb::]")
{
    constexpr auto lim = 100 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        LargeIndex dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        for (size_t i = 0; i < lim; i++)
        {
            CHECK(nullptr != dx.Insert(keys[i], uint64_t(i)).first);
        }

        for (size_t i = 0; i < lim; i++)
        {
            auto res = dx.Find(keys[i]);
            CHECK((nullptr != res && *res == i));
        }
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("1,000,000 Hashmap Threaded", "[tdb::]")
{
    constexpr auto lim = 1000 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        LargeHashmapSafe dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        std::atomic<size_t> insert = 0;

        std::for_each(std::execution::par, keys.begin(), keys.end(), [&](auto& k)
        {
            if (dx.InsertLock(k, uint64_t(99)).first)
                insert++;
        });

        CHECK(insert == lim);

        std::atomic<size_t> find = 0;

        std::for_each(std::execution::par, keys.begin(), keys.end(), [&](auto& k)
        {
            auto res = dx.FindLock(k);
            if ((nullptr != res && *res == 99))
                find++;
        });

        CHECK(find == lim);
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("1,000,000 Inserts Threaded", "[tdb::]")
{
    constexpr auto lim = 1000 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        LargeIndexS dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        std::atomic<size_t> insert = 0;

        std::for_each(std::execution::par, keys.begin(), keys.end(), [&](auto &k)
        {
            if (dx.InsertLock(k, uint64_t(99)).first) 
                insert++;
        });

        CHECK(insert == lim);

        std::atomic<size_t> find = 0;

        std::for_each(std::execution::par, keys.begin(), keys.end(), [&](auto& k)
        {
            auto res = dx.FindLock(k);
            if ((nullptr != res && *res == 99))
                find++;
        });

        CHECK(find == lim);
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("100,000 Inserts Hashmap", "[tdb::]")
{
    constexpr auto lim = 100 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        LargeHashmap dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        for (size_t i = 0; i < lim; i++)
        {
            CHECK(nullptr != dx.Insert(keys[i], uint64_t(i)).first);
        }

        for (size_t i = 0; i < lim; i++)
        {
            auto res = dx.Find(keys[i]);
            CHECK((nullptr != res && *res == i));
        }
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("1,000000 Inserts Hashmap", "[tdb::]")
{
    constexpr auto lim = 1000 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        LargeHashmap dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        for (size_t i = 0; i < lim; i++)
        {
            CHECK(nullptr != dx.Insert(keys[i], uint64_t(i)).first);
        }

        for (size_t i = 0; i < lim; i++)
        {
            auto res = dx.Find(keys[i]);
            CHECK((nullptr != res && *res == i));
        }
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("1,000,000 MapReduceT<8> Threaded Inserts", "[tdb::]")
{
    constexpr auto M = 8;
    constexpr auto lim = 1000 * 1000;

    std::filesystem::remove_all("db.dat0");
    std::filesystem::remove_all("db.dat1");
    std::filesystem::remove_all("db.dat2");
    std::filesystem::remove_all("db.dat3");
    std::filesystem::remove_all("db.dat4");
    std::filesystem::remove_all("db.dat5");
    std::filesystem::remove_all("db.dat6");
    std::filesystem::remove_all("db.dat7");

    {
        MapReduceT<M> dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        std::atomic<size_t> identity = 0;
        std::atomic<size_t> done = 0;
        std::array<size_t, M> inserts = { 0,0,0,0,0,0,0,0 };
        std::array<size_t, M> finds = { 0,0,0,0,0,0,0,0 };

        std::for_each_n(std::execution::par_unseq, keys.data(), M, [&](auto v)
        {
            auto d = identity++;
            size_t li = 0, lf = 0;

            for (size_t i = 0; i < lim; i++)
            {
                if (dx.Insert(keys[i], uint64_t(i), d).first)
                {
                    inserts[d]++;
                    li++;
                }
            }         

            for (size_t i = 0; i < lim; i++)
            {
                if (dx.Find(keys[i], d))
                {
                    finds[d]++;
                    lf++;
                }
            } 

            done++;
        });

        auto total_insert = std::accumulate(inserts.begin(), inserts.end(),0);
        auto total_find = std::accumulate(finds.begin(), finds.end(), 0);

        CHECK(lim == total_find);
        CHECK(lim == total_insert);
    }

    std::filesystem::remove_all("db.dat0");
    std::filesystem::remove_all("db.dat1");
    std::filesystem::remove_all("db.dat2");
    std::filesystem::remove_all("db.dat3");
    std::filesystem::remove_all("db.dat4");
    std::filesystem::remove_all("db.dat5");
    std::filesystem::remove_all("db.dat6");
    std::filesystem::remove_all("db.dat7");
}

TEST_CASE("Multi-Threaded Access", "[tdb::]")
{
    constexpr auto lim = 6 * 1000;
    constexpr auto readers = 16;

    {
        std::filesystem::remove_all("db.dat");

        SmallIndex dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        std::thread wr([&]()
        {
            for (size_t i = 0; i < lim; i++)
            {
                dx.Insert(keys[i], uint64_t(i));
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        //We should be able to read from any number of reader threads before the data is there and while updates are being made.
        //Eventually we will be consistent. Thrash the MMAP IO!
        //

        std::list<std::thread> tl;

        for (size_t i = 0; i < readers; i++)
            tl.emplace_back([&]()
            {
                size_t i;
                do
                {
                    //We can open as many of these as many const handles as we want, they will just share the map.
                    SmallIndexReadOnly ldx("db.dat");

                    i = 0;
                    for (; i < lim; i++)
                    {
                        if (!ldx.Find(keys[i]))
                            break;
                    }
                } while (i != lim);
            });

        wr.join();

        for (auto& t : tl)
            t.join();

        CHECK(true);    //If we are here then it worked.
    }

    std::filesystem::remove_all("db.dat");
}