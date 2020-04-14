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

	template < typename R, typename element_t, typename index_t > class _Document
	{
		index_t index;
		size_t root_n;

		R* io = nullptr;

		using link_t = typename element_t::Link;
		using int_t = typename element_t::Int;
		using pointer_t = typename element_t::Pointer;
		static const size_t max_pages = element_t::max_pages;
		static const size_t max_elements = element_t::max_elements;
		static const size_t padding = element_t::padding;

#pragma pack(push,1)

		struct lookup_t
		{
			lookup_t() {}

			link_t pages[max_pages] = {};
			pointer_t lookup[max_pages] = {};

			int_t page_count = 0;
			int_t lookup_count = 0;

			uint8_t padding[lookup_padding];
		};

		static_assert(sizeof(lookup_t) == R::UnitSize);

#pragma pack(pop)

		void Open(R* _io, size_t& _n)
		{
			root_n = _n++;
			io = _io;

			if (io->size() <= root_n)
				io->template Allocate<lookup_t>();

			_Open(_n);
		}		
		auto Root() const
		{
			return &io->template Lookup<lookup_t>(root_n);
		}

	public:

		_Document() {}

		void Validate() {} //TODO

		void Open(R* _io, size_t& _n)
		{
			root_n = _n++;
			io = _io;

			if (io->size() <= root_n)
				io->template Allocate<lookup_t>();

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