/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "tdb.hpp"
#include "poly_keys.hpp"

namespace tdb
{
	namespace search_engine
	{
		using R = AsyncMap<32*1024*1024,512*1024>;
		using N = SimpleFuzzyHashBuilder<512 * 1024, uint64_t, polynomial::F1B4C1_Key, 8,2>;
		using I = SimpleFuzzyHashBuilder<512 * 1024, uint32_t, Key16, 8,2>;
		using LeanIndexStream = DatabaseBuilder < R, Stream< R, BTree< R, N> >, BTree< R, I>  >;
	}
}