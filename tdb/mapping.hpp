/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "../mio.hpp"
#include "../gsl-lite.hpp"

#include <string_view>
#include <filesystem>
#include <fstream>
#include <utility>

#include "d8u/util.hpp"

namespace tdb
{
	using namespace std;
	using namespace gsl;

	namespace fs = std::filesystem;
	using namespace d8u::util;


	template <uint64_t growsize_t = 1024*1024, size_t page_t = 64*1024> class _MapFile
	{
		mio::mmap_sink map;
		string name;

		struct _Header
		{
			uint64_t size = 0;
			uint64_t version = 1;

			uint64_t _align[6] = { 0 };
		};

		_Header & Header() const
		{
			return *((_Header*)map.data());
		}

		void _Open()
		{
			std::error_code error;

			map.map(name, error);

			if (error)
				throw std::runtime_error(string("Failed to map ") + error.message());
		}

		uint64_t incidental_offset = 0;
		gsl::span<uint8_t> incidental;

	public:
		std::pair<uint8_t*, uint64_t> Incidental(size_t s)
		{
			if (s > page_t)
				return std::make_pair<nullptr, -1>;

			if (s > incidental.size())
			{
				auto [pointer, offset] = Allocate(page_t);
				incidental_offset = offset;
				incidental = gsl::span<uint8_t>(pointer, page_t);
			}

			uint8_t* result = incidental.data();
			uint64_t result_offset = incidental_offset;

			incidental_offset += s;
			incidental = gsl::span<uint8_t>(incidental.data() + s, incidental.size() - s);

			return std::make_pair(result, result_offset);
		}

		void Flush() { map.sync(); }
		string_view Name() { return name; }
		void Close() { map.unmap(); }

		bool Stale(uint64_t size=0) const
		{
			return Header().size + size > map.size();
		}

		void Reopen()
		{
			map.unmap();
			_Open();
		}

		_MapFile() {}
		_MapFile(const string_view file)
		{
			Open(file);
		}

		~_MapFile() {}

		void Open(const string_view file)
		{
			name = file;

			if (!fs::exists(file))
			{
				fs::create_directories(fs::path(file).remove_filename());
				empty_file1(file);
			}

			_Open();

			if (map.size() < sizeof(_Header))
			{
				Resize(sizeof(_Header));

				Header() = { 0,1 };
			}
		}

		void Resize(uint64_t target)
		{
			if (target + sizeof(_Header) >= map.size())
			{
				uint64_t growth = (target + sizeof(_Header)) - map.size();
				if (growth < growsize_t)
					growth = growsize_t;

				Reserve(size() + growth - sizeof(_Header));
			}

			Header().size = target;
		}

		void Reserve(uint64_t target)
		{
			Close();
			fs::resize_file(name, target + sizeof(_Header));
			Reopen();
		}

		void UpdateVersion()
		{
			Header().version++;
		}

		uint8_t * data() { return (uint8_t*)map.data() + sizeof(_Header); }
		uint64_t size() { return Header().size; }
		uint8_t* offset(uint64_t m) const { return (uint8_t*)map.data() + m + sizeof(_Header); }

		uint64_t offset_of(uint8_t* v)
		{
			return (uint64_t)(v - data());
		}

		pair<uint8_t *,uint64_t> Allocate(uint64_t szof)
		{
			auto s = Header().size;
			Resize(s + szof);

			return make_pair(offset(s),s);
		}

		template <typename J> pair<J *,uint64_t> Allocate()
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J();

			return make_pair((J*)r.first,r.second);
		}

		template <typename J, typename ... t_args> pair<J *, uint64_t> Construct(t_args ... args)
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J(args...);

			return make_pair((J*)r.first, r.second);
		}
	};

	template <uint64_t growsize_t = 1024 * 1024, size_t page_t = 64 * 1024> class _ReadMemoryFile
	{
		std::vector<uint8_t> mem;

		struct _Header
		{
			uint64_t size = 0;
			uint64_t version = 1;

			uint64_t _align[6] = { 0 };
		};

		_Header& Header() const
		{
			return *((_Header*)mem.data());
		}

	public:
		std::pair<uint8_t*, uint64_t> Incidental(size_t s)
		{
			return std::make_pair<nullptr, -1>;
		}

		void Flush() {  }
		void Close() { }

		bool Stale(uint64_t size = 0) const
		{
			return Header().size + size > mem.size();
		}

		void Reopen(){}

		_ReadMemoryFile() {}
		_ReadMemoryFile(std::vector<uint8_t>& _m) { Open(_m);  }

		void Open(std::vector<uint8_t>& _m)
		{
			mem = std::move(_m);
		}

		~_ReadMemoryFile() {}

		void Resize(uint64_t target){}

		void Reserve(uint64_t target) { }

		void UpdateVersion()
		{
			Header().version++;
		}

		uint8_t* data() { return (uint8_t*)mem.data() + sizeof(_Header); }
		uint64_t size() { return Header().size; }
		uint8_t* offset(uint64_t m) const { return (uint8_t*)mem.data() + m + sizeof(_Header); }

		uint64_t offset_of(uint8_t* v)
		{
			return (uint64_t)(v - data());
		}

		pair<uint8_t*, uint64_t> Allocate(uint64_t szof)
		{
			return make_pair((uint8_t*)nullptr, uint64_t(0));
		}

		template <typename J> pair<J*, uint64_t> Allocate()
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J();

			return make_pair((J*)r.first, r.second);
		}

		template <typename J, typename ... t_args> pair<J*, uint64_t> Construct(t_args ... args)
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J(args...);

			return make_pair((J*)r.first, r.second);
		}
	};

	template <uint64_t growsize_t = 1024 * 1024, size_t grace_t = 64, size_t page_t = 64 * 1024> class _ReadMemoryList
	{
		std::vector<uint8_t> mem;

		struct _Header
		{
			uint64_t size = 0;
			uint64_t version = 1;

			uint64_t _align[6] = { 0 };

			uint8_t ex[page_t - grace_t - 64];
		};

		_Header& Header() const
		{
			return *((_Header*)mem.data());
		}

	public:
		std::pair<uint8_t*, uint64_t> Incidental(size_t s)
		{
			return std::make_pair<nullptr, -1>;
		}

		void Flush() {  }
		void Close() { }

		bool Stale(uint64_t size = 0) const
		{
			return Header().size + size > mem.size();
		}

		void Reopen() {}

		_ReadMemoryList() {}
		_ReadMemoryList(std::vector<uint8_t>& _m) { Open(_m); }

		void Open(std::vector<uint8_t>& _m)
		{
			mem = std::move(_m);
		}

		~_ReadMemoryList() {}

		void Resize(uint64_t target) {}

		void Reserve(uint64_t target) { }

		void UpdateVersion()
		{
			Header().version++;
		}

		uint8_t* data() { return (uint8_t*)mem.data() + sizeof(_Header); }
		uint64_t size() { return Header().size; }
		uint8_t* offset(uint64_t m) const { return (uint8_t*)mem.data() + m + sizeof(_Header); }

		uint64_t offset_of(uint8_t* v)
		{
			return (uint64_t)(v - data());
		}

		pair<uint8_t*, uint64_t> Allocate(uint64_t szof)
		{
			return make_pair((uint8_t*)nullptr, uint64_t(0));
		}

		template <typename J> pair<J*, uint64_t> Allocate()
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J();

			return make_pair((J*)r.first, r.second);
		}

		template <typename J, typename ... t_args> pair<J*, uint64_t> Construct(t_args ... args)
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J(args...);

			return make_pair((J*)r.first, r.second);
		}
	};

	template <uint64_t growsize_t = 1024 * 1024, size_t grace_t = 64, size_t page_t = 64*1024> class _MapList
	{
		std::recursive_mutex ll;
		std::list<mio::mmap_sink> list;
		string name;
		uint64_t current = 0;

		struct _Header
		{
			uint64_t size = 0;
			uint64_t version = 1;

			uint64_t _align[6] = { 0 };

			uint8_t ex[page_t - grace_t - 64];
		};

		static_assert(sizeof(_Header) + grace_t == page_t);

		_Header& Header() const
		{
			return *((_Header*)list.front().data());
		}

		void _Open()
		{
			list.emplace_back(name);

			current = list.back().size();
		}

		void _Grow()
		{
			list.emplace_back(name,current);

			current += list.back().size();
		}

		uint64_t incidental_offset = 0;
		gsl::span<uint8_t> incidental;

	public:

		//Allocate an unaligned fragment of mapped fil space:
		//

		std::pair<uint8_t*,uint64_t> Incidental(size_t s)
		{
			if (s > page_t)
				return std::make_pair((uint8_t*)nullptr, (uint64_t)-1);

			std::lock_guard<std::recursive_mutex> lock(ll);

			if (s > incidental.size())
			{
				auto [pointer, offset] = Allocate(page_t);
				incidental_offset = offset;
				incidental = gsl::span<uint8_t>(pointer,page_t);
			}

			uint8_t* result = incidental.data();
			uint64_t result_offset = incidental_offset;

			incidental_offset += s;
			incidental = gsl::span<uint8_t>(incidental.data()+s, incidental.size()-s);

			return std::make_pair (result, result_offset);
		}

		void Flush() 
		{ 
			for(auto & map : list)
				map.sync(); 
		}

		string_view Name() { return name; }

		void Close() { list.clear(); }

		bool Stale(uint64_t size = 0) const
		{
			size_t total = 0;

			for (auto& map : list)
				total += map.size();

			return Header().size + size > total;
		}

		void Flatten()
		{
			std::lock_guard<std::recursive_mutex> lock(ll);

			Reopen();
		}

		void Reopen()
		{
			Close();
			_Open();
		}

		_MapList() {}
		_MapList(const string_view file)
		{
			Open(file);
		}

		~_MapList() {}

		void Open(const string_view file)
		{
			name = file;

			if (!fs::exists(file))
			{
				fs::create_directories(fs::path(file).remove_filename());
				empty_file1(file);
				fs::resize_file(name, growsize_t + sizeof(_Header) + grace_t);

				_Open();

				Header() = { 0,1 };
			}
			else
				_Open();
		}

		void Resize(uint64_t target)
		{
			if (target + sizeof(_Header) > current)
				Reserve(current + growsize_t);

			Header().size = target;
		}

		void Reserve(uint64_t size)
		{
			fs::resize_file(name, size );
			_Grow();
		}

		void UpdateVersion()
		{
			Header().version++;
		}

		//This interface is disallowed with the map list.
		//uint8_t* data() { return (uint8_t*)map.data() + sizeof(_Header); }
		uint64_t size() { return Header().size; }

		uint8_t* offset(uint64_t o) const
		{ 
			o += sizeof(_Header);

			uint64_t c = 0;
			for (auto& m : list)
			{
				if (o >= c && o < c + m.size())
					return (uint8_t*)m.data() + o - c;

				c += m.size();
			}

			return nullptr;
		}

		uint64_t offset_of(uint8_t* p)
		{
			uint64_t c = 0;
			for (auto& m : list)
			{
				if (p >= (uint8_t*)m.data() && p < (uint8_t*)m.data() + m.size())
					return (p - (uint8_t*)m.data()) + c - sizeof(_Header);

				c += m.size();
			}

			return 0;
		}

		pair<uint8_t*, uint64_t> AllocateAlign(uint64_t szof)
		{
			auto rem = szof % page_t;

			return Allocate((rem) ? szof + page_t - rem : szof);
		}

		pair<uint8_t*, uint64_t> Allocate(uint64_t szof)
		{
			std::lock_guard<std::recursive_mutex> lock(ll);

			auto s = Header().size;

			auto _start = s + sizeof(_Header);
			auto _final = s + szof + sizeof(_Header);
			if (_start < current && _final > current) //Block doesn't fit into map boundaries.
				Resize(_final); //Move boundary to exact map beginning. Leaves zero block gap, page aligned so still can be iterated.

			Resize(s + szof);

			return make_pair(offset(s), s);
		}

		pair<uint8_t*, uint64_t> AllocateLock(uint64_t szof)
		{
			//All allocates of this object are locked
			//

			return Allocate(szof);
		}

		template <typename J> pair<J*, uint64_t> Allocate()
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J();

			return make_pair((J*)r.first, r.second);
		}

		template <typename J, typename ... t_args> pair<J*, uint64_t> Construct(t_args ... args)
		{
			auto r = Allocate(sizeof(J));
			new(r.first) J(args...);

			return make_pair((J*)r.first, r.second);
		}
	};
}