/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "tdb.hpp"
#include "poly_keys.hpp"

namespace tdb
{
	namespace search_engine
	{
		struct SearchRecord
		{
			SearchRecord() {}

			SearchRecord(const Key16& fh) : file_hash(fh) { }

			auto Keys(uint64_t n)
			{
				return std::make_tuple(file_hash);
			}

			Key16 file_hash;
			//todo when, who, what, name and whatever else we need to know.
		};

		using R = AsyncMap<32*1024*1024,512*1024>;
		using N = SimpleFuzzyHashBuilder<512 * 1024, uint32_t, polynomial::F1B4C1_Key, 64,2>;

		using E = SimpleTableElementBuilder <SearchRecord,512*1024,uint32_t>;

		using I = SimpleFuzzyHashBuilder<512 * 1024, uint32_t, Key16, 64,2>;
		using LeanIndexStream = DatabaseBuilder < R, _StreamBucket<R, uint32_t, uint32_t, 64 - 20, 1024 - 12, BTree< R, N>>, FixedTable<R, E, BTree< R, I>>  >;
	}
}