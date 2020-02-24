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
	template <size_t S> using _T = _BTree<_R<S>, OrderedListPointer >;
	template <size_t S> using _Index = _Database< _R<S>, _T<S> >;

	template < size_t G = 1024 * 1024 > class Index
	{
		_Index<G> db;

	public:
		Index(string_view name)
		{
			db.Open(name);
		}

		bool Stale(uint64_t size = 0) const
		{
			return db.Stale(size);
		}

		template <typename K, typename V> auto Insert(const K& k, const V& v)
		{
			return db.Table<0>().Insert(k, v);
		}

		template <typename K> auto Find(const K& k) const
		{
			return db.Table<0>().Find(k);
		}
	};

	using SmallIndex = Index<>;
	using SmallIndexReadOnly = const Index<>;

	using MediumIndex = Index<8*1024*1024>;
	using MediumIndexReadOnly = const Index<8 * 1024 * 1024>;

	using LargeIndex = Index<>;
	using LargeIndexReadOnly = const Index<64*1024*1024>;

	template < size_t C = 8, size_t G = 8*1024*1024 > class MapReduceT
	{
		std::array<_Index<G>, C> dbr;

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