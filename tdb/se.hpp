/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "tdb.hpp"
#include "poly_keys.hpp"

#include "d8u/time.hpp"

namespace tdb
{
	namespace search_engine
	{
		struct SearchRecord
		{
			SearchRecord() {}

			SearchRecord(const Key16& fh, uint32_t _modified, std::string_view _name, std::string_view _domain) 
				: file_hash(fh) 
				, inserted(d8u::time::epoch_seconds())
				, modified(_modified) 
			{ 
				strncpy_s(name, _name.data(), (_name.size() <= 47) ? _name.size() : 47);
				strncpy_s(domain, _domain.data(), (_domain.size() <= 15) ? _domain.size() : 15);
			}

			auto Keys(uint64_t n)
			{
				return std::make_tuple(file_hash);
			}

			Key16 file_hash;
			uint64_t rank = 1;
			uint32_t modified;
			uint32_t inserted;
			char name[48] = {};
			char domain[16] = {};
		};

		using R = AsyncMap<32*1024*1024,512*1024>;
		using N = SimpleFuzzyHashBuilder<512 * 1024, uint32_t, polynomial::F1B4C1_Key, 24,2>;

		using E = SimpleTableElementBuilder <SearchRecord,512*1024,uint32_t>;

		using I = SimpleFuzzyHashBuilder<512 * 1024, uint32_t, Key16, 8,2>;
		using LeanIndexStream = DatabaseBuilder < R, _StreamBucket<R, uint32_t, uint32_t, 64 - 20, 1024 - 12, BTree< R, N, 3 >>, FixedTable<R, E, BTree< R, I>>  >;
	}
}