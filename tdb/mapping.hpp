/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "../mio.hpp"
#include "../gsl-lite.hpp"

#include <string_view>
#include <filesystem>
#include <fstream>
#include <utility>

namespace tdb
{
	using namespace std;
	using namespace gsl;

	namespace fs = std::filesystem;

	void empty_file(const string_view file)
	{
		ofstream fs(file, ios::out);
	}

	template <typename T> uint64_t append_file(const string_view file,const T & m)
	{
		ofstream fs(file, ios::out | fstream::app | ios::binary);
		uint64_t result = fs.tellp();
		fs.write((const char*)&m.length, sizeof(uint32_t));
		fs.write((const char*)m.data(), m.size());

		return result;
	}

	void empty_file1(const string_view file)
	{
		ofstream fout;
		fout.open(file, ios::out);
		fout << 'c';
	}



	template <uint64_t growsize_t = 1024*1024> class _MapFile
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

	public:
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
		uint8_t* offset(uint64_t m) { return (uint8_t*)map.data() + m + sizeof(_Header); }

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

	template <uint64_t growsize_t = 1024 * 1024, size_t grace_t = 64> class _MapList
	{
		std::mutex ll;
		std::list<mio::mmap_sink> list;
		string name;
		uint64_t current = 0;

		struct _Header
		{
			uint64_t size = 0;
			uint64_t version = 1;

			uint64_t _align[6] = { 0 };

			uint8_t ex[64 * 1024 - grace_t - 64];
		};

		static_assert(sizeof(_Header) + grace_t == 64 * 1024);

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

	public:
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

		uint8_t* offset(uint64_t o) 
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

		pair<uint8_t*, uint64_t> Allocate(uint64_t szof)
		{
			std::lock_guard<std::mutex> lock(ll);

			auto s = Header().size;
			Resize(s + szof);

			return make_pair(offset(s), s);
		}

		pair<uint8_t*, uint64_t> AllocateLock(uint64_t szof)
		{
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