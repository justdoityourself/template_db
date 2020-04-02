/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#ifdef TEST_RUNNER


#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "tdb/test.hpp"
#include "tdb/test_legacy.hpp"

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

#include "tdb/net.hpp"

#include "clipp.h"

#include <string>
#include <array>
#include <iostream>
#include <string_view>

#include "tdb/database.hpp"
#include "d8u/string_switch.hpp"

using namespace clipp;
using namespace d8u;

int main(int argc, char* argv[])
{
    std::string path = "default.db";

    auto cli = (
        option("-p", "--path").doc("Database path") & value("path", path)
        );

    if (!parse(argc, argv, cli)) std::cout << make_man_page(cli, argv[0]);
    else
    {
        //TODO client network interface

        tdb::LargeHashmapSafe db(path);
        tdb::NetworkIndex< tdb::LargeHashmapSafe > net(db);

        

        bool running = true;
        gsl::span<uint8_t> find;

        while (running)
        {
            std::cout << "TDB CLI 1.0: ";
            std::string command, key, value;
            std::cin >> command;

            switch (switch_t(command))
            {
            default:
                std::cout << "Invalid Command" << std::endl;
                break;
            case switch_t("h"):
            case switch_t("help"):
            case switch_t("Help"):
            case switch_t("HELP"):
                std::cout << "Commands: help, insert, find, flush, quit" << std::endl;
                break;
            case switch_t("i"):
            case switch_t("insert"):
            case switch_t("Insert"):
            case switch_t("INSERT"):
                std::cout << "Key: ";
                std::cin >> key;
                std::cout << "Value: ";
                std::cin >> value;

                if (db.InsertSizedObjectLock(key, value))
                    std::cout << "Success" << std::endl;
                else
                    std::cout << "Object Already Exists" << std::endl;

                break;
            case switch_t("f"):
            case switch_t("find"):
            case switch_t("Find"):
            case switch_t("FIND"):
                std::cout << "Key: ";
                std::cin >> key;

                find = db.FindSizedObjectLock(key);

                if(!find.size())
                    std::cout << "Object doesn't exists" << std::endl;
                else
                    std::cout << std::string_view((char*)find.data(), find.size()) << std::endl;
                
                break;
            case switch_t("fl"):
            case switch_t("flush"):
            case switch_t("Flush"):
            case switch_t("FLUSH"):
                std::cout << "Flushing...";

                db.Flush();

                std::cout << " Done." << std::endl;

                break;
            case switch_t("q"):
            case switch_t("quit"):
            case switch_t("Quit"):
            case switch_t("QUIT"):
                std::cout << "Shutdown..." << std::endl;
                running = false;
                break;
            }
        }
    }

    return 0;
}


#endif //! defined(BENCHMARK_RUNNER) && ! defined(TEST_RUNNER)


