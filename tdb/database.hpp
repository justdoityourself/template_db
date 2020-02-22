/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "host.hpp"
#include "btree.hpp"
#include "recycling.hpp"
#include "mapping.hpp"

namespace tdb
{
	using R = _Recycling< _MapFile<1024*1024>, 64 * 1024 >;
	using T = _BTree<R, OrderedListPointer >;
	using Simple = _Database< R, T >;

	using RL = _Recycling< _MapFile<64*1024*1024>, 64 * 1024 >;
	using TL = _BTree<RL, OrderedListPointer >;
	using SimpleLarge = _Database< RL, TL >;

	using _MapReduce8 = _Database< RL, TL, TL, TL, TL, TL, TL, TL, TL >;

	class MapReduce8
	{
		_MapReduce8 db;

	public:
		MapReduce8(string_view name) : db(name) {}

		template <typename K, typename V> auto Insert(const K& k, const V& v, size_t thread = -1)
		{
			auto map = *k.data() % 8;

			if (thread != -1 && map != thread)
				return std::make_pair((uint64_t*)nullptr, false);

			switch (map)
			{
			default:
			case 0: return db.Table<0>().Insert(k, v);
			case 1: return db.Table<1>().Insert(k, v);
			case 2: return db.Table<2>().Insert(k, v);
			case 3: return db.Table<3>().Insert(k, v);
			case 4: return db.Table<4>().Insert(k, v);
			case 5: return db.Table<5>().Insert(k, v);
			case 6: return db.Table<6>().Insert(k, v);
			case 7: return db.Table<7>().Insert(k, v);
			}
		}

		template <typename K> auto Find(const K& k, size_t thread = -1)
		{
			auto map = *k.data() % 8;

			if (thread != -1 && map != thread)
				return (uint64_t*)nullptr;

			switch (map)
			{
			default:
			case 0: return db.Table<0>().Find(k);
			case 1: return db.Table<1>().Find(k);
			case 2: return db.Table<2>().Find(k);
			case 3: return db.Table<3>().Find(k);
			case 4: return db.Table<4>().Find(k);
			case 5: return db.Table<5>().Find(k);
			case 6: return db.Table<6>().Find(k);
			case 7: return db.Table<7>().Find(k);
			}
		}
	};
}