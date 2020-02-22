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

		_Header & Header()
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
			if (target + sizeof(_Header) > map.size())
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
}