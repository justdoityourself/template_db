/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <cstdint>
#include <time.h>
#include <random>

#include "hash.hpp"

namespace tdb
{
	using namespace std;

	template <typename T1, typename T2 = T1> struct KeyT
	{
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

		bool Equal(const KeyT& k)
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

		int Compare(const KeyT & p)
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
}