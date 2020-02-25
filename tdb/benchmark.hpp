/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "../picobench.hpp"
#include "../ProgressBar.hpp"

#include "database.hpp"
#include "d8u/util.hpp"

namespace tdb
{
    using namespace d8u::util;

    namespace benchmark
    {
        static const int test_count = 10;
        ProgressBar progressBar(test_count * 25744, 70);
       
        template <typename T, size_t S> void insert(picobench::state& s)
        {
            auto& keys = singleton<std::array<RandomKeyT<Key32>, S>>(); // Heap and common to all tests

            T dx("db.dat");
            size_t total = 0;

            {
                picobench::scope scope(s);

                for (auto _ : s)
                {
                    for (size_t i = 0; i < S; i++)
                        if (dx.Insert(keys[i], uint64_t(i)).first)
                            total++;
                }
            }

            progressBar += s.iterations();  progressBar.display();

            if (total != S * s.iterations()) std::cout << total << std::endl;
        }

        template <typename T, size_t S> void find(picobench::state& s)
        {
            auto& keys = singleton<std::array<RandomKeyT<Key32>, S>>(); // Heap and common to all tests

            std::filesystem::remove_all("db.dat");
            T dx("db.dat");

            for (size_t i = 0; i < S; i++)
                dx.Insert(keys[i], uint64_t(i));

            size_t total = 0;

            {
                picobench::scope scope(s);

                for (auto _ : s)
                {
                    for (size_t i = 0; i < S; i++)
                        if (dx.Find(keys[i])) total++;
                }
            }

            progressBar += s.iterations();  progressBar.display();

            if(total != S * s.iterations()) std::cout << total << std::endl;
        }
    

     
       auto hashi100k = insert<LargeHashmap, 8000>;
       auto hashf100k = find<LargeHashmap, 8000>;

       auto hashi100ks = insert<LargeHashmapS, 8000>;
       auto hashf100ks = find<LargeHashmapS, 8000>;

       auto hashi100km = insert<LargeHashmapM, 8000>;
       auto hashf100km = find<LargeHashmapM, 8000>;

       auto hashi100kl = insert<LargeHashmapL, 8000>;
       auto hashf100kl = find<LargeHashmapL, 8000>;

       auto bsi100k = insert<LargeIndex, 8000>;
       auto bsf100k = find<LargeIndex, 8000>;

        PICOBENCH_SUITE("Fuzzy hashmap vs binary tree insert");


        PICOBENCH(hashi100k);
        PICOBENCH(hashi100ks);
        PICOBENCH(hashi100km);
        PICOBENCH(hashi100kl);
        PICOBENCH(bsi100k);

        PICOBENCH_SUITE("Fuzzy hashmap vs binary tree finds");

        PICOBENCH(hashf100k);
        PICOBENCH(hashf100ks);
        PICOBENCH(hashf100km);
        PICOBENCH(hashf100kl);
        PICOBENCH(bsf100k);
        



       
    }

}


