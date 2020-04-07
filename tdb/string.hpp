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

	/*
		One to many lookup, string to pointer.
	*/

	template < typename R, typename int_t, size_t lim_c, typename bucket_t > class _StringSearch
	{
		bucket_t bucket;

		R* io = nullptr;

	public:

		_StringSearch() {}

		void Validate() {} //TODO

		void Open(R* _io, size_t& _n)
		{
			io = _io;

			bucket.Open(io, _n);
		}

		template <typename V> void Insert(std::string_view k, const V& v)
		{
			if (k.size() < lim_c)
			{
				int_t kk = 0;

				std::memcpy(&kk, k.data(), k.size());
				bucket.Write(kk, v);
			}
			else
			{
				std::set<typename int_t::Key> inserted;

				for (size_t i = 0; i < k.size() - lim_c + 1; i++)
				{
					int_t kk = 0;
					std::memcpy(&kk, k.data()+i, lim_c);

					if (inserted.find(kk.key) == inserted.end())
					{
						inserted.insert(kk.key);
						bucket.Write(kk, v);
					}
				}
			}
		}

		template <typename V> void Insert(uint64_t k, const V& v)
		{
			auto p = (char*)io->GetObject(k);
			return Insert(std::string_view(p, strlen(p)),v);
		}

		template <typename V> auto Find(std::string_view k)
		{
			std::set<V> _result;

			auto on_result = [&](auto seg)
			{
				auto buf = d8u::t_buffer<V>(seg);

				for (auto& e : buf)
					_result.insert(e);

				return true;
			};

			if (k.size() < lim_c)
			{
				int_t kk = 0;

				std::memcpy(&kk, k.data(), k.size());
				bucket.Stream(kk, on_result);
			}
			else
			{
				std::set<typename int_t::Key> found;

				for (size_t i = 0; i < k.size() - lim_c + 1; i++)
				{
					int_t kk = 0;
					std::memcpy(&kk, k.data()+i, lim_c);

					if (found.find(kk.key) == found.end())
					{
						found.insert(kk.key);
						bucket.Stream(kk, on_result);
					}
				}
			}

			std::vector<V> result(_result.size());
			std::copy(_result.begin(), _result.end(), result.begin());

			return result;
		}

		template <typename V> auto Find(uint64_t k)
		{
			auto p = (char *)io->GetObject(k);
			return Find(std::string_view(p,strlen(p)));
		}
	};

	template <typename R> using StringSearch = _StringSearch<R, _IntWrapper<uint32_t>, 3, _StreamBucket<R, uint64_t, uint64_t, 1024, BTree< R, SimpleOrderedListBuilder<64 * 1024, uint64_t, _IntWrapper<uint32_t> > > > >;
}