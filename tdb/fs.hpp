/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "tdb.hpp"

namespace tdb
{
	namespace filesystem
	{
		//16TB with 4kb cluster
		using Segment32 = _Segment<uint32_t>;

#pragma pack(push, 1)
		template < typename SEG, typename KEY = Key32 > struct FileT
		{
			FileT() {}

			template < typename RUNS> static size_t Size(uint64_t size, uint64_t time, const char* names, std::vector<uint32_t> parents, const std::vector<uint8_t>& keys, const RUNS& runs)
			{
				return sizeof(FileT) + strlen(names) + 1 + parents.size() * sizeof(uint32_t) + keys.size() + sizeof(SEG) * runs.size();
			}

			size_t Size() const
			{
				return ((size_t)seg_offset) + seg_count * sizeof(SEG);
			}

			template < typename RUNS> FileT(uint64_t _size, uint64_t _time, const char* names, std::vector<uint32_t> parents, const std::vector<uint8_t>& keys, const RUNS& runs)
				: size(_size)
				, time(_time)
			{
				size_t l1 = strlen(names) + 1;
				std::copy(names, names + l1, (char*)(this + 1));

				parent_offset = sizeof(FileT) + l1;
				parent_count = parents.size();

				std::copy(parents.begin(), parents.end(), (uint32_t*)((uint8_t*)this) + parent_offset);

				key_offset = parent_offset + parents.size()*sizeof(uint32_t);
				key_count = keys.size() / sizeof(KEY);

				std::copy(keys.begin(), keys.end(), ((uint8_t*)this) + key_offset);

				seg_offset = key_offset + keys.size();
				seg_count = runs.size();

				auto sp = (SEG*)(((uint8_t*)this) + seg_offset);
				for(auto & r : runs)
					*sp++ = SEG(r);
			}

			template < size_t value_c > auto Value() { return 0; }
			template < > auto Value<0> () { return size; }
			template < > auto Value<1>() { return time; }
			template < > auto Value<2>() { return (const char *) (this+1); }
			template < > auto Value<3>() { return gsl::span<uint32_t>((uint32_t*)(((uint8_t*)(this)) + parent_offset), parent_count); }
			template < > auto Value<4>() { return gsl::span<KEY>((KEY*)(((uint8_t*)(this)) + key_offset), key_count); }
			template < > auto Value<5>() { return gsl::span<SEG>((SEG*)(((uint8_t*)(this)) + seg_offset), seg_count); }

			auto Keys(uint64_t n)
			{
				return std::make_tuple(n + sizeof(FileT),
					n + key_offset + (key_count - 1) * sizeof(KEY),
					gsl::span<SEG>((SEG*)(((uint8_t*)(this)) + seg_offset), seg_count));
			}

			uint64_t size = 0;
			uint64_t time = 0;

			//[Dynamic]
			uint16_t parent_offset;
			uint16_t parent_count;
			uint16_t key_offset;
			uint16_t key_count;
			uint16_t seg_offset;
			uint16_t seg_count;
		};

#pragma pack(pop)

		using R = AsyncMap<128 * 1024 * 1024>;
		using E = SimpleSurrogateTableBuilder32 <FileT<Segment32>>;
		using NameSearch = StringSearch32<R>;
		using HashSearch = BTree< R, MultiSurrogateKeyPointer32<R> >;
		using MountSearch = BTree< R, OrderedSegmentPointer32<uint32_t> >;

		using NameNull = NullStringSearch32<R>;
		using HashNull = NullIndex< R, MultiSurrogateKeyPointer32<R> >;
		using MountNull = NullIndex< R, OrderedSegmentPointer32<uint32_t> >;

		using FullIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameSearch, HashSearch, MountSearch > >; //This is too expensive ATM, todo optimize.
		using NoIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameNull, HashNull, MountNull > >;
		using HalfIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameNull, HashSearch, MountSearch > >;
		using MinimalIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameNull, HashSearch, MountNull > >;

		enum Tables { Files };
		enum Indexes { Names, Hash, Disk };
		enum Values { Size, Time, Name,Parent,Keys,Runs };
	}
}