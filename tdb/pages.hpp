/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "btree.hpp"

namespace tdb
{
	/*
		Pages implements nodes of a specific size.

		Nodes must match the allocation unit of the recycler, as this is the minimum addressable unit.

		This is done by carefully with key/value counts, pointer counts and padding. UPDATED, USE BUILDERS!

		Use a static assert to check your work please.

		TODO:
		
		1.	Naming scheme for the intended page size of the node.
		X 2.	Some kind of builder herer would be awesome. This might be possible but potentially complicated.
			Needing to take the input of the node parent, key and value type and then output the right numbers to match page size... X
	*/



	//Pointer nodes, map a key to an object:
	//

	template <typename int_t> 	using OrderedSegmentPointer =			SimpleMultiListBuilder<64 * 1024, uint64_t, _Segment<int_t> >;
								using OrderedSegmentPointer32 =			SimpleMultiListBuilder<64 * 1024, uint32_t, _Segment<uint32_t> >;
	template <typename int_t>	using OrderedIntKey =					MultiListBuilder <64 * 1024, int_t, _IntWrapper<int_t>, Key32, int_t>;
	template <typename R>		using MultiSurrogateStringPointer =		SimpleMultiListBuilder<64 * 1024, uint64_t, _OrderedSurrogateString<R, uint64_t> >;
	template <typename R>		using MultiSurrogateKeyPointer32 =		SimpleMultiListBuilder<64 * 1024, uint32_t, _SurrogateKey<R, uint32_t, Key32> >;
	template <typename R>		using MultiSurrogateKeyPointer32v =		SimpleMultiListBuilder<64 * 1024, uint32_t, _SurrogateKey<R, uint32_t, Key32>,4,true >;
	template <typename R>		using OrderedSurrogateStringPointer =	SimpleOrderedListBuilder<64 * 1024, uint64_t, _OrderedSurrogateString<R, uint64_t> >;
	template <typename R>		using SurrogateKeyPointer =				SimpleOrderedListBuilder<64 * 1024, uint64_t, _SurrogateKey<R, uint64_t, Key32> >;
	template <typename R>		using SurrogateKeyPointer32 =			SimpleOrderedListBuilder<64 * 1024, uint32_t, _SurrogateKey<R, uint32_t, Key32> >;
								using OrderedListPointer =				SimpleOrderedListBuilder<64 * 1024, uint64_t, Key32 >;


	template <size_t fuzzy_c> using FuzzyHashPointerT =		SimpleFuzzyHashBuilder<64 * 1024 , uint64_t, Key32, fuzzy_c>;
	template <size_t fuzzy_c> using BigFuzzyHashPointerT =	SimpleFuzzyHashBuilder<256 * 1024, uint64_t, Key32, fuzzy_c>;
							  using FuzzyHashPointer =		SimpleFuzzyHashBuilder<64 * 1024 , uint64_t, Key32, 4>;
							  using FuzzyHashPointer32 =	SimpleFuzzyHashBuilder<64 * 1024, uint32_t, Key32, 4>;


	//Other node types can map a key to keys, or keys and pointers:
	//

	using OrderedListKey = OrderedListBuilder<64 * 1024,uint64_t, Key32, Key32, uint64_t, 4>;
	using OrderedListKP = OrderedListBuilder<64 * 1024,uint64_t, Key32, KeyP, uint64_t, 4>;
	using OrderedListKP2 = OrderedListBuilder<64 * 1024,uint64_t, Key32, KeyP2, uint64_t, 4>;



	static_assert(	sizeof(OrderedSurrogateStringPointer<void>) ==	64 * 1024);
	static_assert(	sizeof(SurrogateKeyPointer<void>) ==			64 * 1024);
	static_assert(	sizeof(OrderedListPointer) ==					64 * 1024);
	static_assert(	sizeof(FuzzyHashPointerT<1>) ==					64 * 1024);
	static_assert(	sizeof(FuzzyHashPointer) ==						64 * 1024);
	static_assert(	sizeof(OrderedListKey) ==						64 * 1024);
	static_assert(	sizeof(OrderedListKP) ==						64 * 1024);
	static_assert(	sizeof(OrderedListKP2) ==						64 * 1024);



	static_assert(sizeof(BigFuzzyHashPointerT<1>) == 256 * 1024);
}