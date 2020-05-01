/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <cstdint>
#include <time.h>
#include <random>

#include "hash.hpp"
#include "types.hpp"

namespace tdb
{
	using namespace std;

	template <typename T1, typename T2 = T1> struct KeyT
	{
		static const uint8_t mode = KeyMode::key_mode_direct;

		KeyT() {}

		template < typename T > KeyT(const T & t)
		{
			HashT(*this, t);
		}

		void Zero()
		{
			auto p = (uint64_t*)this;

			for (size_t i = 0; i < sizeof(*this); i += 8, p++)
				(*p) = 0;
		}

		bool IsZero()
		{
			auto p = (uint64_t*)this;

			for (size_t i = 0; i < sizeof(*this); i += 8, p++)
				if (*p)
					return false;

			return true;
		}

		bool Equal(const KeyT& k, void* ref_page = nullptr, void* ref_page2 = nullptr)
		{
			auto p1 = (uint64_t*)this;
			auto p2 = (uint64_t*)&k;

			for (size_t i = 0; i < sizeof(*this); i += 8, p1++, p2++)
				if (*p1 != *p2)
					return false;

			return true;
		}

		void Random()
		{
			auto p = (uint64_t*)this;

			static default_random_engine e((unsigned int)time(0));

			for (size_t i = 0; i < sizeof(*this); i+=8, p++)
			{
				(*p) = e();
				(*p) <<= 32; //e() doesn't fill top dword of qword
				(*p) += e();
			}

			HashT(*this, *this);
		}

		int Compare(const KeyT & p,void* ref_page=nullptr, void* ref_page2 = nullptr)
		{
			if (first == p.first)
			{
				if (second == p.second)
					return 0;

				if (second > p.second)
					return 1;
				return -1;
			}
			else
			{
				if (first > p.first)
					return 1;
				return -1;
			}
		}

		bool operator==(const KeyT & p)
		{
			return Compare(p) == 0;
		}

		bool operator>(const KeyT & p)
		{
			return Compare(p) == 1;
		}

		uint8_t& operator[] (size_t dx)
		{
			return *(data() + dx);
		}

		const uint8_t& operator[] (size_t dx) const
		{
			return *(data() + dx);
		}

		const uint8_t* data() const { return (uint8_t*)this; }
		size_t size() const { return sizeof(*this); }

		const uint8_t* begin() const { return data(); }
		const uint8_t* end() const { return data() + size(); }

		T1 first;
		T2 second;
	};

	template < typename K, typename P > struct KeyPointerT : public K
	{
		KeyPointerT() {}

		template < typename T > KeyPointerT(const T & t) : K(t) {}

		using K::operator==;
		using K::operator>;
		P pointer = 0;
	};

	template < typename K, typename P > struct KeyPointerT2 : public K
	{
		KeyPointerT2() {}

		template < typename T > KeyPointerT2(const T & t) : K(t) {}

		using K::operator==;
		using K::operator>;
		P p1 = 0;
		P p2 = 0;
	};

	template <typename K> struct RandomKeyT : K
	{
		RandomKeyT() { K::Random(); }

		using K::operator==;
		using K::operator>;
	};

	using Key16 = KeyT<uint64_t>;
	using Key24 = KeyT<Key16, uint64_t>;
	using Key32 = KeyT<Key16>;
	using KeyP = KeyPointerT<Key24, uint64_t>;
	using KeyP2 = KeyPointerT2<Key24, uint32_t>;
	using Key64 = KeyT<Key32>;

#pragma pack(push,1)
	template <typename int_t> struct _IntWrapper
	{
		static const uint8_t mode = KeyMode::key_mode_direct;

		_IntWrapper() {}
		_IntWrapper(int_t t) : key(t) {}

		using Key = int_t;
		int_t key = 0;

		int Compare(const _IntWrapper& rs, void* _ref_page, void* direct_page)
		{
			//NO SURROGATE INTS! todo surrogate bigint wrapper

			if (key > rs.key)
				return 1;
			if (key < rs.key)
				return -1;
			return 0;
		}

		//No int wrapper hashmaps ( equals function )
	};

	template <typename int_t> struct _Segment
	{
		static const uint8_t mode = KeyMode::key_mode_direct;

		_Segment() {}
		_Segment(const std::pair<int_t, int_t>& p) : start(p.first), length(p.second) {}
		template < typename T > explicit  _Segment(const T& t) : start((int_t)t.start), length((int_t)t.length) {}
		_Segment(int_t o, int_t l) : start(o), length(l) {}

		int_t start = 0;
		int_t length = 0;

		int Compare(const _Segment& rs, void* _ref_page, void* direct_page)
		{
			if (start >= rs.start + rs.length)
				return 1;
			if (start + length <= rs.start)
				return -1;
			return 0;
		}
	};

	template <typename R, typename int_t> struct _OrderedSurrogateString
	{
		static const uint8_t mode = KeyMode::key_mode_surrogate;

		_OrderedSurrogateString() {}
		_OrderedSurrogateString(int_t t) : sz_offset(t) {}

		int_t sz_offset = 0;

		int Compare(const _OrderedSurrogateString& rs, void* _ref_page, void* direct_page)
		{
			auto ref_page = (R*)_ref_page;

			auto l = (const char*)ref_page->GetObject(sz_offset);
			auto r = (const char*)((!rs.sz_offset) ? (uint8_t*)direct_page : ref_page->GetObject(rs.sz_offset));

			return strcmp(l, r);
		}
	};

	template <typename R, typename int_t, typename key_t> struct _SurrogateKey
	{
		static const uint8_t mode = KeyMode::key_mode_surrogate;

		_SurrogateKey() {}
		_SurrogateKey(int_t t) : sz_offset(t) {}

		int_t sz_offset = 0;

		int Compare(const _SurrogateKey& rs, void* _ref_page, void* direct_page)
		{
			auto ref_page = (R*)_ref_page;

			auto l = (key_t*)ref_page->GetObject(sz_offset);
			auto r = (key_t*)((!rs.sz_offset) ? (uint8_t*)direct_page : ref_page->GetObject(rs.sz_offset));

			return l->Compare(*r);
		}

		bool Equal(const _SurrogateKey& rs, void* _ref_page, void* direct_page)
		{
			auto ref_page = (R*)_ref_page;

			auto l = (key_t*)ref_page->GetObject(sz_offset);
			auto r = (key_t*)((!rs.sz_offset) ? (uint8_t*)direct_page : ref_page->GetObject(rs.sz_offset));

			return l->Equal(*r);
		}
	};

	template < size_t L > std::string_view string_viewz(const char(&t)[L])
	{
		return std::string_view(t, L);
	}

	void* string_voidz(const char* t)
	{
		return (void*)t;
	}

	template < typename T >void* key_void(T& t)
	{
		return (void*)&t;
	}
#pragma pack(pop)
}