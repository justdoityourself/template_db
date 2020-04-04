/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "bucket.hpp"
#include "btree.hpp"

#include <set>
#include <string_view>
#include <vector>

#include "d8u/buffer.hpp"

namespace tdb
{
	using namespace std;

	template < typename R, typename int_t, typename bucket_t > class _StringBucket
	{
		bucket_t bucket;

		R* io = nullptr;

	public:

		_StringBucket() {}

		void Validate() {} //TODO

		void Open(R* _io, size_t& _n)
		{
			io = _io;

			bucket.Open(io, _n);
		}

		template <typename V> void Insert(std::string_view k, const V& v)
		{
			if (k.size() < sizeof(int_t))
			{
				int_t kk = 0;

				std::memcpy(&kk, k.data(), k.size());
				bucket.write(*pc, v);
			}
			else
			{
				std::set<int_t> inserted;

				for (size_t i = 0; i < k.size() - sizeof(int_t) + 1; i++)
				{
					auto pc = (int_t*)(k.data() + i);

					if (inserted.find(*pc) == inserted.end())
					{
						inserted.insert(*pc);
						bucket.write(*pc, v);
					}
				}
			}
		}

		template <typename V> void Find(uint64_t k, const V& v)
		{
			auto p = (char*)io->GetObject(k);
			return Insert(std::string_view(p, strlen(p)),v);
		}

		template <typename V> auto Find(std::string_view k)
		{
			std::vector<V> result;

			auto on_result = [&](auto seg)
			{
				auto buf = d8u::t_buffer<V>(seg);
				result.insert(result.end(), buf.begin(), buf.end());
			};

			if (k.size() < sizeof(int_t))
			{
				int_t kk = 0;

				std::memcpy(&kk, k.data(), k.size());
				bucket.Stream(kk, on_result);
			}
			else
			{
				std::set<int_t> found;

				for (size_t i = 0; i < k.size() - sizeof(int_t) + 1; i++)
				{
					auto pc = (int_t*)(k.data() + i);

					if (found.find(*pc) == found.end())
					{
						found.insert(*pc);
						bucket.Stream(*pc, on_result);
					}
				}
			}

			return result;
		}

		template <typename V> auto Find(uint64_t k)
		{
			auto p = (char *)io->GetObject(k);
			return Find(std::string_view(p,strlen(p)));
		}
	};

	template <typename R> using StringBucket = _StringBucket<R, uint32_t, _StreamBucket<R, uint64_t, uint64_t, 1024, BTree< R, SimpleOrderedListBuilder<64 * 1024, uint64_t, uint32_t > > > >;
}