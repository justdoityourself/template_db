/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <cstdint>
#include <string_view>

#include "mhttp/http.hpp"
#include "mhttp/tcpserver.hpp"
#include "keys.hpp"

#include "d8u/string_switch.hpp"

namespace tdb
{
	using namespace mhttp;
    using namespace std;
    using namespace d8u;

    template <typename STORE> class NetworkIndex
    {
        TcpServer insert;
        TcpServer find;
        HttpServer http;

        STORE& store;
    public:

        void Join()
        {
            insert.Join();
            find.Join();
            http.Join();
        }

        void Shutdown()
        {
            insert.Shutdown();
            find.Shutdown();
            http.Shutdown();
        }

        ~NetworkIndex()
        {
            Shutdown();
        }

        NetworkIndex(STORE& _store, string_view http_port = "8045", string_view find_port = "1717", string_view insert_port = "1818", size_t threads = 1)
            : store(_store)
            , http((uint16_t)stoi(http_port.data()),
                [&](auto& c, auto&& req, auto queue)
                {
                    switch (switch_t(req.type))
                    {
                    default:
                        c.Http400A(queue);
                        break;
                    case switch_t("GET"):
                    case switch_t("Get"):
                    case switch_t("get"):
                    {
                        auto ptr = store.FindObjectLock(req.path.substr(1));

                        if (!ptr)
                            c.Http404A(queue);
                        else
                        {
                            auto sz = (uint16_t*)ptr;

                            std::vector<uint8_t> buffer(*sz);
                            std::copy(ptr + 2, ptr + *sz + 2, buffer.data());

                            c.ResponseA(queue, "200 OK", std::move(buffer));
                        }

                        break;
                    }
                    case switch_t("POST"):
                    case switch_t("Post"):
                    case switch_t("post"):
                    {
                        if (req.body.size() > 64 * 1024 - 2)
                        {
                            c.Http400A(queue);
                            return;
                        }

                        auto [ptr, status] = store.InsertLock(req.path.substr(1),uint64_t(0));

                        if (status)
                            c.Http400A(queue);
                        else
                        {
                            auto [dest,offset] = store.Incidental(req.body.size()+2);
                            *ptr = offset;
                            auto sz = (uint16_t*)dest;
                            *sz = (uint16_t)req.body.size();

                            std::copy(req.body.begin(), req.body.end(), dest + 2);

                            c.Http200A(queue);
                        }
                    }
                        break;
                    }

                }, true, { threads })
            , find((uint16_t)stoi(find_port.data()), ConnectionType::message,
                [&](auto* pc, auto req, auto body, void* reply)
                {
                    if (req.size() != 32)
                        throw std::runtime_error("Invalid Block size");

                    auto ptr = store.FindObjectLock( *( (Key32*)req.data() ) );

                    if (!ptr)
                    {
                        pc->ActivateWrite(reply, std::vector<uint8_t>());
                        return;
                    }

                    auto sz = (uint16_t*)ptr;

                    std::vector<uint8_t> buffer(*sz);
                    std::copy(ptr + 2, ptr + *sz + 2, buffer.data());

                    pc->ActivateWrite(reply, std::move(buffer));

                }, true, TcpServer::Options{ threads })
            , insert((uint16_t)stoi(insert_port.data()),ConnectionType::message ,
                [&](auto* pc, auto req, auto body, void* reply)
                {
                    if(req.size() <= 32 || req.size() > 64*1024-2+32)
                        throw std::runtime_error("Invalid Block size");
                        
                    auto [ptr, status] = store.InsertLock(*((Key32*)req.data()),uint64_t(0));

                    std::vector<uint8_t> buffer(4);

                    if(status)
                        *((uint32_t*)buffer.data()) = 0;
                    else
                    {
                        auto [dest,offset] = store.Incidental(req.size() - 32 + 2);
                        *ptr = offset;

                        auto sz = (uint16_t*)dest;
                        *sz = (uint16_t)(req.size() - 32);

                        std::copy(req.begin() + 32, req.end(), dest + 2);

                        *((uint32_t*)buffer.data()) = *sz;
                    }              

                    pc->ActivateWrite(reply, std::move(buffer));

                }, true, { threads })
        {
        }
    };

    //TODO CLIENT INTERFACE
}