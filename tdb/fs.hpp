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

			template < typename RUNS> static size_t Size(uint16_t type,uint16_t flags,uint64_t size, uint64_t time, const char* names, std::vector<uint32_t> parents, const std::vector<uint8_t>& keys, const RUNS& runs)
			{
				return sizeof(FileT) + strlen(names) + 1 + parents.size() * sizeof(uint32_t) + keys.size() + sizeof(SEG) * runs.size();
			}

			size_t Size() const
			{
				return ((size_t)seg_offset) + seg_count * sizeof(SEG);
			}

			template < typename RUNS> FileT(uint16_t _type, uint16_t _flags, uint64_t _size, uint64_t _time, const char* names, std::vector<uint32_t> parents, const std::vector<uint8_t>& keys, const RUNS& runs)
				: size(_size)
				, time(_time)
				, type(_type)
				, flags(_flags)
			{
				size_t l1 = strlen(names) + 1;
				std::copy(names, names + l1, (char*)(this + 1));

				parent_offset = (uint16_t)(sizeof(FileT) + l1);
				parent_count = (uint16_t)parents.size();

				std::copy(parents.begin(), parents.end(), (uint32_t*)(((uint8_t*)this) + parent_offset));

				key_offset = (uint16_t)(parent_offset + parents.size()*sizeof(uint32_t));
				key_count = (uint16_t)keys.size() / sizeof(KEY);

				std::copy(keys.begin(), keys.end(), ((uint8_t*)this) + key_offset);

				seg_offset = (uint16_t)(key_offset + keys.size());
				seg_count = (uint16_t)runs.size();

				auto sp = (SEG*)(((uint8_t*)this) + seg_offset);
				for(auto & r : runs)
					*sp++ = SEG(r);

				//Debug Assert
				//if (Size() != Size(_size, _time, names, parents, keys, runs))
				//	std::cout << "Problem";
			}

			template < size_t value_c > auto Value() const { return 0; }
			template < > auto Value<0> () const { return size; }
			template < > auto Value<1>() const  { return time; }

			template < > auto Value<2>() const
			{ 
				std::vector<std::string_view> result;
				size_t len = parent_offset - sizeof(FileT) - 1;

				if (len > 0xffff)
					return std::vector<std::string_view>();

				std::string_view view((const char*)(this + 1), len);

				size_t cur = 0;
				for (size_t i = 0; i < len; i++)
				{
					if (view[i] == '|')
					{
						result.push_back(std::string_view(view.data() + cur, i - cur - 1));
						cur = i;
					}
				}

				result.push_back(std::string_view(view.data() + cur, len - cur));

				return result; 
			}

			template < > auto Value<3>() const  { return gsl::span<uint32_t>((uint32_t*)(((uint8_t*)(this)) + parent_offset), parent_count); }
			template < > auto Value<4>() const { return gsl::span<KEY>((KEY*)(((uint8_t*)(this)) + key_offset), key_count); }
			template < > auto Value<5>() const { return gsl::span<SEG>((SEG*)(((uint8_t*)(this)) + seg_offset), seg_count); }
			template < > auto Value<6>() const { return (const char*)(this + 1); }

			auto Filesize() const { return size; }
			auto Time() const { return time; }
			auto Parents() const { return Value<3>(); }
			auto Names() const { return Value<2>(); }
			auto Name() const { return Value<6>(); }

			auto Descriptor() { return Value<4>(); }

			template < typename T > auto DescriptorT() 
			{ 
				return gsl::span<T>((T*)(((uint8_t*)(this)) + key_offset), key_count);
			}

			auto FirstName() const 
			{
				auto _n = Names();

				if (!_n.size())
					return std::string_view("");

				return _n[0];
			}

			auto Type() const { return type; }
			auto Flags() const { return flags; }

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
			uint16_t type;
			uint16_t flags;
		};

#pragma pack(pop)

		//Table/Row:
		//

		using Row = FileT<Segment32>;
		using E = SimpleSurrogateTableBuilder32 <Row>;



		//Mempry Mapped:
		//

		using R = AsyncMap<128 * 1024 * 1024>;
		using NameSearch = StringSearch32<R>;
		using HashSearch = BTree< R, MultiSurrogateKeyPointer32v<R> >;
		using MountSearch = BTree< R, OrderedSegmentPointer32<uint32_t> >;

		using NameNull = NullStringSearch32<R>;
		using HashNull = NullIndex< R, MultiSurrogateKeyPointer32v<R> >;
		using MountNull = NullIndex< R, OrderedSegmentPointer32<uint32_t> >;

		using FullIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameSearch, HashSearch, MountSearch > >; //This is too expensive ATM, todo optimize.
		using NoIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameNull, HashNull, MountNull > >;
		using HalfIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameNull, HashSearch, MountSearch > >;
		using MinimalIndex32 = DatabaseBuilder < R, SurrogateTable<R, E, NameNull, HashSearch, MountNull > >;



		//Read Only Memory:
		//

		using MRO = AsyncMemoryView<>;
		using NameSearchM = StringSearch32<MRO>;
		using HashSearchM = BTree< MRO, MultiSurrogateKeyPointer32v<R> >;
		using MountSearchM = BTree< MRO, OrderedSegmentPointer32<uint32_t> >;

		using NameNullM = NullStringSearch32<MRO>;
		using HashNullM = NullIndex< MRO, MultiSurrogateKeyPointer32v<MRO> >;
		using MountNullM = NullIndex< MRO, OrderedSegmentPointer32<uint32_t> >;

		using FullIndex32M = DatabaseBuilder < MRO, SurrogateTable<MRO, E, NameSearchM, HashSearchM, MountSearchM > >; //This is too expensive ATM, todo optimize.
		using NoIndex32M = DatabaseBuilder < MRO, SurrogateTable<MRO, E, NameNullM, HashNullM, MountNullM > >;
		using HalfIndex32M = DatabaseBuilder < MRO, SurrogateTable<MRO, E, NameNullM, HashSearchM, MountSearchM > >;
		using MinimalIndex32M = DatabaseBuilder < MRO, SurrogateTable<MRO, E, NameNullM, HashSearchM, MountNullM > >;

		enum Tables { Files };
		enum Indexes { Names, Hash, Disk };
		enum Values { Size, Time, NameList,Parent,Keys,Runs,Name };
		enum Types { File, Folder };
		enum Flags {  };
	}
}