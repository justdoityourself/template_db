/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <execution>
#include <numeric>

#include "../catch.hpp"

#include "database.hpp"

using namespace tdb;


TEST_CASE("Simple Index", "[tdb::]")
{
    std::filesystem::remove_all("db.dat");

    {
        Simple i("db.dat");

        auto dx = i.Index();

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
        Simple i("db.dat");
        auto dx = i.Index();

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

template <typename T> T& singleton()
{
    static T t;

    return t;
}

TEST_CASE("100,000 Inserts", "[tdb::]")
{
    constexpr auto lim = 100 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        SimpleLarge i("db.dat");
        auto dx = i.Index();

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

TEST_CASE("1,000,000 MapReduce8 Threaded Inserts", "[tdb::]")
{
    //Note about this test, there is not currently a mutex protecting the growth and allocation of blocks.
    //This test correctly identifies that this causes trouble about 10% of the time.
    //This object works well with read IO but needs to be fixed for writes.
    //

    constexpr auto lim = 1000 * 1000;

    std::filesystem::remove_all("db.dat");

    {
        MapReduce8 dx("db.dat");

        auto& keys = singleton<std::array<RandomKeyT<Key32>, lim>>(); // Heap

        std::atomic<size_t> identity = 0;
        std::atomic<size_t> done = 0;
        std::array<size_t, 8> inserts = { 0,0,0,0,0,0,0,0 };
        std::array<size_t, 8> finds = { 0,0,0,0,0,0,0,0 };

        std::for_each_n(std::execution::par_unseq, keys.data(), 8, [&](auto v)
        {
            auto d = identity++;
            size_t li = 0, lf = 0;

            for (size_t i = 0; i < lim; i++)
            {
                auto res = dx.Insert(keys[i], uint64_t(i), d);
                if (res.first)
                {
                    inserts[d]++;
                    li++;
                }
                if (res.second)
                {
                    std::cout << "OVERWRITE" << std::endl;
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

            if (li != lf)
                std::cout << "SANITY CHECK FAILED" << std::endl;

            done++;
        });

        //In some cases the finds && inserts are not flushed. Need to actually atomically load from those memory addresses. This also seems to work.
        while(done != 8) std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        auto total_insert = std::accumulate(inserts.begin(), inserts.end(),0);
        auto total_find = std::accumulate(finds.begin(), finds.end(), 0);

        CHECK(lim == total_find);
        CHECK(lim == total_insert);
    }

    std::filesystem::remove_all("db.dat");
}

TEST_CASE("Multi-Threaded Access", "[tdb::]")
{
    constexpr auto lim = 6 * 1000;
    constexpr auto readers = 4;

    {
        std::filesystem::remove_all("db.dat");

        Simple i("db.dat");

        auto dx = i.Index();

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
                    Simple li("db.dat");

                    auto ldx = li.Index();

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