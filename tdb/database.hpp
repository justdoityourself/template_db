/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <utility>
#include <array>
#include <list>
#include <mutex>

#include "host.hpp"
#include "btree.hpp"
#include "recycling.hpp"
#include "mapping.hpp"

namespace tdb
{
	template <size_t S> using _R = _Recycling< _MapFile<S>, 64 * 1024 >;
	template <size_t S> using _SGR = _Recycling< _MapList<S>, 64 * 1024 >;
	template <size_t S> using _T = _BTree<_R<S>, OrderedListPointer >;
	template <size_t S> using _SGT = _BTree<_SGR<S>, OrderedListPointer >;
	template <size_t S, size_t F> using _F = _BTree<_R<S>, FuzzyHashPointerT<F> >;
	template <size_t S, size_t F> using _SGF = _BTree<_SGR<S>, FuzzyHashPointerT<F> >;

	template <size_t S> using _R256 = _Recycling< _MapFile<S>, 256 * 1024 >;
	template <size_t S> using _SGR256 = _Recycling< _MapList<S>, 256 * 1024 >;
	template <size_t S> using _T256 = _BTree<_R256<S>, OrderedListPointer >;
	template <size_t S> using _SGT256 = _BTree<_SGR256<S>, OrderedListPointer >;
	template <size_t S, size_t F> using _F256 = _BTree<_R256<S>, BigFuzzyHashPointerT<F> >;
	template <size_t S, size_t F> using _SGF256 = _BTree<_SGR256<S>, BigFuzzyHashPointerT<F> >;

	template <size_t S> using _IndexSortedList = _Database< _R<S>, _T<S> >;
	template <size_t S, size_t F = 4> using _IndexFuzzyHash = _Database< _R<S>, _F<S,F> >;
	template <size_t S, size_t F = 4> using _BigIndexFuzzyHash = _Database< _R256<S>, _F256<S, F> >;

	template <size_t S> using _IndexSortedListSafe = _Database< _SGR<S>, _SGT<S> >;
	template <size_t S, size_t F = 4> using _IndexFuzzyHashSafe = _Database< _SGR<S>, _SGF<S, F> >;
	template <size_t S, size_t F = 4> using _BigIndexFuzzyHashSafe = _Database< _SGR256<S>, _SGF256<S, F> >;

	template <size_t S> using _RM = _Recycling< _ReadMemoryList<S>, 64 * 1024 >;
	template <size_t S, size_t F> using _FM = _BTree<_RM<S>, FuzzyHashPointerT<F> >;
	template <size_t S, size_t F = 4> using _MemoryIndexFuzzyHash = _Database< _RM<S>, _FM<S, F> >;

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

		template <typename K, typename V> auto Insert(const K& k, const V& v)
		{
			return db.Table<0>().Insert(k, v);
		}

		template <typename F> auto Iterate(F &&f) const
		{
			return db.Table<0>().Iterate(std::move(f));
		}

		template <typename K> auto Find(const K& k) const
		{
			return db.Table<0>().Find(k);
		}

		template <typename K, typename V> auto InsertLock(const K& k, const V& v)
		{
			return db.Table<0>().InsertLock(k, v);
		}

		template <typename K> auto FindLock(const K& k) const
		{
			return db.Table<0>().FindLock(k);
		}

		template <typename K, typename V> bool InsertObject(const K& k, const V& v)
		{
			auto [ptr,status] = db.Table<0>().Insert(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = Incidental(v.size());

			std::copy(v.begin(), v.end(), iptr);
			*ptr = offset;

			return true;
		}

		template <typename K, typename V> bool InsertObjectLock(const K& k, const V& v)
		{
			auto [ptr, status] = db.Table<0>().InsertLock(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = Incidental(v.size());

			std::copy(v.begin(), v.end(), iptr);
			*ptr = offset;

			return true;
		}

		template <typename K> uint8_t * FindObject(const K& k)
		{
			auto ptr = db.Table<0>().Find(k);

			if (!ptr)
				return nullptr;

			return GetObject(*ptr);
		}

		template <typename K> uint8_t* FindObjectLock(const K& k)
		{
			auto ptr = db.Table<0>().FindLock(k);

			if (!ptr)
				return nullptr;

			return GetObject(*ptr);
		}
	};

	using MemoryHashmap = Index<1024 * 1024, _MemoryIndexFuzzyHash<1024 * 1024>>;

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


	/*
		These constructs are mostly not useful with the implementation of per-node locking and mapping lists.
		Might be removed at some point.
	*/

	template <typename T> class SafeWriter
	{
		std::mutex ml;
		T writer;

		struct Lifetime
		{
			SafeWriter<T> & parent;

			Lifetime(SafeWriter<T>& _parent) : parent(_parent)
			{
				parent.ml.lock();
			}

			~Lifetime()
			{
				parent.ml.unlock();
			}

			T& GetWriter () { return parent.writer; }
		};

	public:
		SafeWriter(std::string_view name) : writer(name) {}

		Lifetime GetLock() { return Lifetime(*this); }
	};

	template <typename T, size_t MIN=3> class ViewManager
	{
		std::mutex ml;
		std::list<std::pair<std::atomic<size_t>,T>> maps;
		std::string name;

		struct Lifetime
		{
			std::pair<std::atomic<size_t>, T>& parent;

			Lifetime(std::pair<std::atomic<size_t>, T>& _parent) : parent(_parent) 
			{
				parent.first++;
			}

			~Lifetime() { parent.first--; }

			T& Map () { return parent.second; }
		};

	public:
		ViewManager(std::string_view _name) : name(_name)
		{
			maps.emplace_back(0, name);
		}

		Lifetime View(uint64_t size=0)
		{
			std::lock_guard<std::mutex> l(ml);

			if (maps.back().second.Stale(size))
			{
				maps.emplace_back(0, name);

				//House Keeping:
				//

				while (maps.size() > MIN && maps.front().first.load() == 0)
					maps.pop_front();
			}

			//Lockless safety conditions:
			//

			//MIN views, growth / stale rate, test stale before atomic inc.
			//If all three of these conditions can happen in a few micro seconds then this breaks. Can't happen.
			//

			return Lifetime(maps.back());
		}

		//Target offset instead of total size:
		//

		Lifetime Fixed(uint64_t offset)
		{
			std::lock_guard<std::mutex> l(ml);

			if (maps.back().second.size() < offset) 
			{
				maps.emplace_back(0, name);

				while (maps.size() > MIN&& maps.front().first.load() == 0)
					maps.pop_front();
			}

			return Lifetime(maps.back());
		}

		auto Alloc(uint64_t size = 0)
		{
			std::lock_guard<std::mutex> l(ml);

			if (maps.back().second.Stale(size))
			{
				maps.emplace_back(0, name);

				while (maps.size() > MIN&& maps.front().first.load() == 0)
					maps.pop_front();
			}

			//Lockless safety conditions:
			//

			//MIN views, growth / stale rate, test stale before atomic inc.
			//If all three of these conditions can happen in a few micro seconds then this breaks. Can't happen.
			//

			return std::make_pair(maps.back().second.Allocate(size),Lifetime(maps.back()));
		}
	};


	template <typename T> class MakeHeader
	{
	public:
		MakeHeader(std::string_view name)
		{
			if (!std::filesystem::exists(name))
				ofstream(name, ios::out | ios::binary).write((const char*)&T(), sizeof(T));
		}
	};
}