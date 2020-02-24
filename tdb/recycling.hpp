/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <array>
#include <stdexcept>

namespace tdb
{
	using namespace std;

	template < typename M, uint64_t unit_t> class _Recycling : M
	{
		static const uint64_t null_t = (uint64_t)-1;
		struct _Header
		{
			uint64_t count = 0;
			uint64_t free = null_t;
			uint64_t inuse = 0;
			uint64_t time = 0;

			uint64_t _align[4] = { 0 };
		};

	public:

		using M::Stale;

		static uint64_t AvailableSpace(uint64_t used)
		{
			return sizeof(Unit) - used % sizeof(Unit);
		}

		struct Unit : array<uint8_t, unit_t>
		{
			uint64_t & Pointer() { return *((uint64_t*)this); }
		};

		uint64_t size() { return Header().count; }

		_Header & Header()
		{
			return *((_Header*)M::data());
		}

		_Recycling() {}

		template <typename ... t_args> _Recycling(t_args ... args)
		{
			Open(args...);
		}

		template <typename ... t_args> void Open(t_args ... args)
		{
			M::Open(args...);

			if (M::size() == 0)
				M::template Allocate<_Header>();
		}

		Unit & Root()
		{
			return *((Unit*)(M::data() + sizeof(_Header)));
		}

		bool InvalidIndex(uint64_t idx)
		{
			return idx > Header().count;
		}

		Unit & LookupUnit(uint64_t idx)
		{
			if (InvalidIndex(idx))
				throw out_of_range("The specified index exceeds current range.");

			return Root()[idx];
		}

		Unit & AllocateUnit()
		{
			if (Header().free == null_t)
			{
				Header().count++;
				Header().inuse++;

				return *M::template Allocate<Unit>().first;
			}

			Unit & result = LookupUnit(Header().free);

			if (InvalidIndex(result.Pointer()))
				throw domain_error("Recycler metadata is corrupt.");

			Header().free = result.Pointer();

			Header().inuse++;

			return result;
		}

		uint64_t IndexUnit(Unit & u)
		{
			return (uint64_t)(&u - &Root());
		}

		void FreeUnit(Unit & u)
		{
			u.Pointer() = Header().free;
			Header().free = IndexUnit(u);

			Header().inuse--;
		}

		void FreeIndex(uint64_t idx)
		{
			if(InvalidIndex(idx))
				throw out_of_range("The specified index exceeds current range.");

			FreeUnit(LookupUnit(idx));
		}

		Unit * AllocateSpan(uint64_t c)
		{
			Header().count += c;
			Header().inuse += c;

			return (Unit*)M::Allocate(sizeof(Unit)*c).first;
		}

		uint64_t MapLength(uint64_t l)
		{
			return l / sizeof(Unit) + ((l % sizeof(Unit)) ? 1 : 0);
		}

		uint64_t MapBytes(uint64_t l)
		{
			return MapLength(l) * sizeof(Unit);
		}

		uint8_t* Allocate(uint64_t l)
		{
			return (uint8_t*)AllocateSpan(MapLength(l));
		}

		void Free(uint8_t * p, const uint64_t l)
		{
			Unit * u = (Unit*)p;

			for (uint64_t i = 0; i < MapLength(l); i++, u++)
				FreeUnit(*u);
		}

		template < typename T > void Free(T & t)
		{
			Free((uint8_t*)&t, sizeof(T));
		}

		template < typename T > T & Allocate()
		{
			uint8_t * p = Allocate(sizeof(T));
			new(p) T();

			return *((T*)p);
		}

		template < typename T > T & Lookup(uint64_t idx)
		{
			//With multiple readers and a single writter the map can be undersized.
			//

			if (idx * sizeof(Unit) + sizeof(_Header) > M::size())
				return *((T*) nullptr);

			return *((T*)&(((Unit*)(M::data() + sizeof(_Header)))[idx]));
		}

		template < typename T > uint64_t Index(const T & t)
		{
			return IndexUnit(*((Unit*)&t));
		}

		template <typename J, typename ... t_args> J& Construct(t_args ... args)
		{
			auto r = Allocate(sizeof(J));
			new(r) J(args...);

			return *((J*)r);
		}
	};
}