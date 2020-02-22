/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#ifdef TEST_RUNNER


#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "tdb/test.hpp"

int main(int argc, char* argv[])
{
    return Catch::Session().run(argc, argv);
}


#endif //TEST_RUNNER

#ifdef BENCHMARK_RUNNER


#define PICOBENCH_IMPLEMENT
#include "picobench.hpp"
#include "tdb/benchmark.hpp"

int main(int argc, char* argv[])
{
    picobench::runner runner;
    return runner.run();
}


#endif //BENCHMARK_RUNNER



#if ! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER) && ! defined(OTHER)


#include "clipp.h"

#include <string>
#include <array>
#include <iostream>

//#include "tdb/api.hpp"

using namespace std;
using namespace clipp;

int main(int argc, char* argv[])
{
    bool encrypt = false;
    string out_file = "";

    auto cli = (
        option("-e", "--encrypt").set(encrypt).doc("Encrypt File"),
        option("-o", "--output") & value("Output File", out_file)
        );

    if (!parse(argc, argv, cli)) cout << make_man_page(cli, argv[0]);
    else
    {
    }

    return 0;
}


#endif //! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER)


