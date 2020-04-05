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



	template < typename R, typename json_t, typename index_t, size_t page_c > class _TransactedJson
	{
		index_t index;

		R* io = nullptr;

	public:

		_TransactedJson() {}

		void Validate() {} //TODO

		void Open(R* _io, size_t& _n)
		{
			io = _io;

			index.Open(io, _n);
		}

		template <typename V> void Insert(std::string_view k, const V& v)
		{

		}

		template <typename V> void Find(uint64_t k, const V& v)
		{

		}

		template <typename V> auto Find(std::string_view k)
		{

		}

		template <typename V> auto Find(uint64_t k)
		{

		}
	};

}