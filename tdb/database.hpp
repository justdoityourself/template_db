/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <array>

#include "host.hpp"
#include "btree.hpp"
#include "recycling.hpp"
#include "mapping.hpp"

namespace tdb
{
	template <size_t S> using _R = _Recycling< _MapFile<S>, 64 * 1024 >;
	template <size_t S> using _T = _BTree<_R<S>, OrderedListPointer >;
	template <size_t S> using _Index = _Database< _R<S>, _T<S> >;

	template < size_t G = 1024 * 1024 > class Index
	{
		_Index<G> db;

	public:
		Index(string_view name)
		{
			db.Open(name);
		}

		template <typename K, typename V> auto Insert(const K& k, const V& v)
		{
			return db.Table<0>().Insert(k, v);
		}

		template <typename K> auto Find(const K& k) const
		{
			return db.Table<0>().Find(k);
		}
	};

	using SmallIndex = Index<>;
	using SmallIndexReadOnly = const Index<>;

	using LargeIndex = Index<>;
	using LargeIndexReadOnly = const Index<64*1024*1024>;

	template < size_t C = 8, size_t G = 8*1024*1024 > class MapReduceT
	{
		std::array<_Index<G>, C> dbr;

	public:
		MapReduceT(string_view name) 
		{
			auto idx = 0;
			for (auto& e : dbr)
				e.Open(std::string(name) + std::to_string(idx++));
		}

		template <typename K, typename V> auto Insert(const K& k, const V& v, size_t thread = -1)
		{
			auto map = *k.data() % C;

			if (thread != -1 && map != thread)
				return std::make_pair((uint64_t*)nullptr, false);

			return dbr[map].Table<0>().Insert(k, v);
		}

		template <typename K> auto Find(const K& k, size_t thread = -1) const
		{
			auto map = *k.data() % C;

			if (thread != -1 && map != thread)
				return (uint64_t*)nullptr;

			return dbr[map].Table<0>().Find(k);
		}
	};
}