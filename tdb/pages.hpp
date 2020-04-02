/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "btree.hpp"

namespace tdb
{
	/*
		Pages implements nodes of a specific size.

		Nodes must match the allocation unit of the recycler, as this is the minimum addressable unit.

		This is done by carefully with key/value counts, pointer counts and padding.

		Use a static assert to check your work please.

		TODO:
		
		1.	Naming scheme for the intended page size of the node.
		2.	Some kind of builder herer would be awesome. This might be possible but potentially complicated.
			Needing to take the input of the node parent, key and value type and then output the right numbers to match page size...
	*/

	template <typename R> using OrderedSurrogateStringPointer = _OrderedListNode<uint64_t, _OrderedSurrogateString<R, uint64_t>, uint64_t, uint64_t, 4092, 4, 8>;
	static_assert(sizeof(OrderedSurrogateStringPointer<void>) == 64 * 1024);

	template <typename R> using SurrogateKeyPointer = _OrderedListNode<uint64_t, _SurrogateKey<R, uint64_t, Key32>, uint64_t, uint64_t, 4092, 4, 8>;
	static_assert(sizeof(SurrogateKeyPointer<void>) == 64 * 1024);

	//8 + 40 * N(1637) + 16 + 32 + 0
	using OrderedListPointer = _OrderedListNode<uint64_t, Key32, uint64_t, uint64_t, 1637, 4, 0>;
	static_assert(sizeof(OrderedListPointer) == 64 * 1024);

	template <size_t F> using FuzzyHashPointerT = _FuzzyHashNode<uint64_t, Key32, uint64_t, uint64_t, 1637, 4, F, 0>;
	static_assert(sizeof(FuzzyHashPointerT<1>) == 64 * 1024);

	template <size_t F> using BigFuzzyHashPointerT = _FuzzyHashNode<uint64_t, Key32, uint64_t, uint64_t, 6552, 4, F, 8>;
	static_assert(sizeof(BigFuzzyHashPointerT<1>) == 256 * 1024);

	using FuzzyHashPointer = _FuzzyHashNode<uint64_t, Key32, uint64_t, uint64_t, 1637, 4, 4, 0>;
	static_assert(sizeof(FuzzyHashPointer) == 64 * 1024);

	//8 + 64 * N(1023) + 16 + 32 + 8
	using OrderedListKey = _OrderedListNode<uint64_t, Key32, Key32, uint64_t, 1023, 4, 8>;
	static_assert(sizeof(OrderedListKey) == 64 * 1024);

	//8 + 64 * N(1023) + 16 + 32 + 8
	using OrderedListKP = _OrderedListNode<uint64_t, Key32, KeyP, uint64_t, 1023, 4, 8>;
	static_assert(sizeof(OrderedListKP) == 64 * 1024);

	//8 + 64 * N(1023) + 16 + 32 + 8
	using OrderedListKP2 = _OrderedListNode<uint64_t, Key32, KeyP2, uint64_t, 1023, 4, 8>;
	static_assert(sizeof(OrderedListKP2) == 64 * 1024);
}