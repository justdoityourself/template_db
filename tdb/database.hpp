/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <utility>
#include <array>
#include <list>
#include <mutex>

#include "host.hpp"
#include "btree.hpp"
#include "table.hpp"
#include "recycling.hpp"
#include "mapping.hpp"

namespace tdb
{
	/*
		With great configuration comes huge blocks of template routing:
	*/

	template <size_t S> using _R = _Recycling< _MapFile<S>, 64 * 1024 >;
	template <size_t S> using _SGR = _Recycling< _MapList<S>, 64 * 1024 >;

	template <size_t S> using _T = _BTree<_R<S>, OrderedListPointer >;
	template <size_t S> using _SGT = _BTree<_SGR<S>, OrderedListPointer >;

	template <size_t S, size_t F> using _F = _BTree<_R<S>, FuzzyHashPointerT<F> >;
	template <size_t S, size_t F> using _SGF = _BTree<_SGR<S>, FuzzyHashPointerT<F> >;

	template <size_t S> using _SS = _BTree<_R<S>, OrderedSurrogateStringPointer<_R<S>> >;
	template <size_t S> using _SGSS = _BTree<_SGR<S>, OrderedSurrogateStringPointer<_SGR<S>> >;

	template <size_t S> using _SK = _BTree<_R<S>, SurrogateKeyPointer<_R<S>> >;
	template <size_t S> using _SGSK = _BTree<_SGR<S>, SurrogateKeyPointer<_SGR<S>> >;



	template <size_t S> using _R256 = _Recycling< _MapFile<S>, 256 * 1024 >;
	template <size_t S> using _SGR256 = _Recycling< _MapList<S>, 256 * 1024 >;

	template <size_t S> using _T256 = _BTree<_R256<S>, OrderedListPointer >;
	template <size_t S> using _SGT256 = _BTree<_SGR256<S>, OrderedListPointer >;

	template <size_t S, size_t F> using _F256 = _BTree<_R256<S>, BigFuzzyHashPointerT<F> >;
	template <size_t S, size_t F> using _SGF256 = _BTree<_SGR256<S>, BigFuzzyHashPointerT<F> >;



	template <size_t S> using _IndexSortedList = _Database< _R<S>, _T<S> >;
	template <size_t S> using _IndexSortedSurrogateString = _Database< _R<S>, _SS<S> >;
	template <size_t S> using _IndexSortedSurrogateKey = _Database< _R<S>, _SK<S> >;
	template <size_t S, size_t F = 4> using _IndexFuzzyHash = _Database< _R<S>, _F<S,F> >;
	template <size_t S, size_t F = 4> using _BigIndexFuzzyHash = _Database< _R256<S>, _F256<S, F> >;



	template <size_t S> using _IndexSortedListSafe = _Database< _SGR<S>, _SGT<S> >;
	template <size_t S> using _IndexSortedSurrogateStringSafe = _Database< _SGR<S>, _SGSS<S> >;
	template <size_t S> using _IndexSortedSurrogateKeySafe = _Database< _SGR<S>, _SGSK<S> >;
	template <size_t S, size_t F = 4> using _IndexFuzzyHashSafe = _Database< _SGR<S>, _SGF<S, F> >;
	template <size_t S, size_t F = 4> using _BigIndexFuzzyHashSafe = _Database< _SGR256<S>, _SGF256<S, F> >;



	template <size_t S> using _RM = _Recycling< _ReadMemoryList<S>, 64 * 1024 >;
	template <size_t S, size_t F> using _FM = _BTree<_RM<S>, FuzzyHashPointerT<F> >;
	template <size_t S, size_t F = 4> using _MemoryIndexFuzzyHash = _Database< _RM<S>, _FM<S, F> >;





	template <typename element_t, size_t S> using _StringIndexTable = _Database< _SGR<S>,
																				_Table< _SGR<S>, element_t, _SGSS<S> >
																			   >;

	template <typename element_t, size_t S> using _StringIndexKeyIndexTable = _Database< _SGR<S>,
																				_Table< _SGR<S>, element_t, _SGSS<S>, _SGSK<S> >
																			   >;

	template < typename element_t, size_t G = 1024 * 1024, typename TABLE = _StringIndexTable<element_t,G> > class Table
	{
		TABLE db;

	public:

		Table() {}

		template < typename T > Table(T& stream)
		{
			db.Open(stream);
		}

		template < typename T > Table(T&& stream)
		{
			db.Open(stream);
		}

		template < typename T >  void Open(T& stream)
		{
			db.Open(stream);
		}

		template < typename T >  void Open(T&& stream)
		{
			db.Open(stream);
		}

		template <typename ... t_args> element_t& Emplace(t_args ... args)
		{
			return db.Table<0>().Emplace(args...);
		}

		template <size_t I, typename K> auto Find(const K& k, void* ref = nullptr) const
		{
			return db.Table<0>().Find<I>(k,ref);
		}
	};


	template < size_t G = 1024 * 1024, typename INDEX = _IndexSortedList<G> > class Index
	{
		INDEX db;

	public:
		Index() {}

		template < typename T > Index(T & stream)
		{
			db.Open(stream);
		}

		template < typename T > Index(T&& stream)
		{
			db.Open(stream);
		}

		template < typename T >  void Open(T& stream)
		{
			db.Open(stream);
		}

		template < typename T >  void Open(T&& stream)
		{
			db.Open(stream);
		}

		void Flush()
		{
			db.Flush();
		}

		void Close()
		{
			db.Close();
		}

		uint8_t* GetObject(uint64_t off) const
		{
			return db.GetObject(off);
		}

		bool Stale(uint64_t size = 0) const
		{
			return db.Stale(size);
		}

		auto Incidental(size_t s)
		{
			return db.Incidental(s);
		}

		template< typename T> auto SetObject(const T & t)
		{
			auto segment = Incidental(t.size());

			std::copy(t.begin(), t.end(), segment.first);

			return segment;
		}

		template <typename K, typename V> auto Insert(const K& k, const V& v)
		{
			return db.Table<0>().Insert(k, v);
		}

		template <typename F> auto Iterate(F &&f) const
		{
			return db.Table<0>().Iterate(std::move(f));
		}

		template <typename K> auto Find(const K& k,void* ref=nullptr) const
		{
			return db.Table<0>().Find(k,ref);
		}

		template <typename K, typename V> auto InsertLock(const K& k, const V& v)
		{
			return db.Table<0>().InsertLock(k, v);
		}

		template <typename K> auto FindLock(const K& k, void* ref = nullptr) const
		{
			return db.Table<0>().FindLock(k,ref);
		}

		template <typename K, typename V> bool InsertObject(const K& k, const V& v)
		{
			auto [ptr,status] = db.Table<0>().Insert(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = Incidental(v.size());

			if (!iptr) 
				return false;

			std::copy(v.begin(), v.end(), iptr);
			*ptr = offset;

			return true;
		}

		template <typename K, typename V> bool InsertObjectLock(const K& k, const V& v)
		{
			auto [ptr, status] = db.Table<0>().InsertLock(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = Incidental(v.size());

			if (!iptr)
				return false;

			std::copy(v.begin(), v.end(), iptr);
			*ptr = offset;

			return true;
		}

		template <typename K, typename V, typename SZ = uint16_t> bool InsertSizedObject(const K& k, const V& v)
		{
			auto [ptr, status] = db.Table<0>().Insert(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = Incidental(v.size() + 2);

			if (!iptr)
				return false;

			auto sz = (SZ*)iptr;
			*sz = (SZ)v.size();
			std::copy(v.begin(), v.end(), iptr+sizeof(SZ));
			*ptr = offset;

			return true;
		}

		template <typename K, typename V, typename SZ = uint16_t> bool InsertSizedObjectLock(const K& k, const V& v)
		{
			auto [ptr, status] = db.Table<0>().InsertLock(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = Incidental(v.size() + 2);

			if (!iptr)
				return false;

			auto sz = (SZ*)iptr;
			*sz = (SZ)v.size();
			std::copy(v.begin(), v.end(), iptr + sizeof(SZ));
			*ptr = offset;

			return true;
		}

		template <typename K> uint8_t * FindObject(const K& k, void* ref = nullptr)
		{
			auto ptr = db.Table<0>().Find(k,ref);

			if (!ptr)
				return nullptr;

			return GetObject(*ptr);
		}

		template <typename K> uint8_t* FindObjectLock(const K& k, void* ref = nullptr)
		{
			auto ptr = db.Table<0>().FindLock(k,ref);

			if (!ptr)
				return nullptr;

			return GetObject(*ptr);
		}

		template <typename K, typename SZ = uint16_t> gsl::span<uint8_t> FindSizedObject(const K& k, void* ref = nullptr)
		{
			auto ptr = db.Table<0>().Find(k,ref);

			if (!ptr)
				return nullptr;

			auto obj = GetObject(*ptr);

			if (!obj)
				return gsl::span<uint8_t>();

			auto sz = (SZ*)obj;

			return gsl::span<uint8_t>(obj + sizeof(SZ), (size_t)*sz);
		}

		template <typename K, typename SZ = uint16_t> gsl::span<uint8_t> FindSizedObjectLock(const K& k, void* ref = nullptr)
		{
			auto ptr = db.Table<0>().FindLock(k,ref);

			if (!ptr)
				return gsl::span<uint8_t>();

			auto obj = GetObject(*ptr);

			if (!obj)
				return gsl::span<uint8_t>();

			auto sz = (SZ*)obj;

			return gsl::span<uint8_t>(obj + sizeof(SZ), (size_t)*sz);
		}
	};


	/*
		More template routing!
	*/

	using MemoryHashmap = Index<1024 * 1024, _MemoryIndexFuzzyHash<1024 * 1024>>;

	using SmallStringIndex = Index<1024*1024, _IndexSortedSurrogateStringSafe<1024*1024>>;

	using SmallIndex = Index<>;
	using SmallIndexReadOnly = const Index<>;

	using MediumIndex = Index<8*1024*1024>;
	using MediumIndexReadOnly = const Index<8 * 1024 * 1024>;

	using LargeIndex = Index<64 * 1024 * 1024>;
	using LargeIndexReadOnly = const Index<64*1024*1024>;

	using LargeIndexS = Index<64 * 1024 * 1024, _IndexSortedListSafe<64 * 1024 * 1024>>;
	using LargeIndexReadOnlyS = const Index<64 * 1024 * 1024, _IndexSortedListSafe<64 * 1024 * 1024>>;

	using TinyHashmapSafe = Index<960 * 1024, _IndexFuzzyHashSafe<960 * 1024>>;
	using TinyHashmapReadOnlySafe = const Index<960 * 1024, _IndexFuzzyHashSafe<960 * 1024>>;

	using SmallHashmap = Index<1024 * 1024, _IndexFuzzyHash<1024 * 1024>>;
	using SmallHashmapReadOnly = const Index<1024 * 1024, _IndexFuzzyHash<1024 * 1024>>;

	using SmallHashmapSafe = Index<1024 * 1024, _IndexFuzzyHashSafe<1024 * 1024>>;
	using SmallHashmapReadOnlySafe = const Index<1024 * 1024, _IndexFuzzyHashSafe<1024 * 1024>>;

	using MediumHashmap = Index<8 * 1024 * 1024, _IndexFuzzyHash<8 * 1024 * 1024>>;
	using MediumHashmapReadOnly = const Index<8 * 1024 * 1024, _IndexFuzzyHash<8 * 1024 * 1024>>;

	using MediumHashmapSafe = Index<8 * 1024 * 1024, _IndexFuzzyHashSafe<8 * 1024 * 1024>>;
	using MediumHashmapReadOnlySafe = const Index<8 * 1024 * 1024, _IndexFuzzyHashSafe<8 * 1024 * 1024>>;

	using LargeHashmap = Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024>>;
	using LargeHashmapReadOnly = const Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024>>;

	using LargeHashmapSafe = Index<64 * 1024 * 1024, _IndexFuzzyHashSafe<64 * 1024 * 1024>>;
	using LargeHashmapReadOnlySafe = const Index<64 * 1024 * 1024, _IndexFuzzyHashSafe<64 * 1024 * 1024>>;

	using LargeHashmapS = Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024, 1>>;
	using LargeHashmapReadOnlyS = const Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024,1>>;

	using LargeHashmapM = Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024, 2>>;
	using LargeHashmapReadOnlyM = const Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024, 2>>;

	using LargeHashmapL = Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024,8>>;
	using LargeHashmapReadOnlyL = const Index<64 * 1024 * 1024, _IndexFuzzyHash<64 * 1024 * 1024,8>>;

	template < size_t C = 8, size_t G = 8*1024*1024, typename INDEX = _IndexSortedList<G> > class MapReduceT
	{
		std::array<INDEX, C> dbr;

	public:
		MapReduceT(string_view name) 
		{
			auto idx = 0;
			for (auto& e : dbr)
				e.Open(std::string(name) + std::to_string(idx++));
		}

		template <typename K, typename V> auto Insert(const K& k, const V& v, size_t thread = -1)
		{
			auto map = *k.data() % C;

			if (thread != -1 && map != thread)
				return std::make_pair((uint64_t*)nullptr, false);

			return dbr[map].Table<0>().Insert(k, v);
		}

		template <typename K> auto Find(const K& k, size_t thread = -1) const
		{
			auto map = *k.data() % C;

			if (thread != -1 && map != thread)
				return (uint64_t*)nullptr;

			return dbr[map].Table<0>().Find(k);
		}
	};

}