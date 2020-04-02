/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

namespace tdb
{
	using namespace std;

	template < typename R, typename link_t, typename int_t, size_t min_alloc, typename index_t > class _StreamBucket
	{
		index_t index;

		R* io = nullptr;

		struct link
		{
			int_t size = 0;
			link_t next = 0;
		};

		struct header
		{
			int_t total = 0;
			link_t last = 0;
		};

	public:

		_StreamBucket() {}

		_StreamBucket(R* _io)
		{
			Open(_io);
		}

		void Validate() {} //TODO

		void Open(R* _io, size_t& _n)
		{
			io = _io;

			index.Open(io, n);
		}

		template <typename T, typename V> void Write(const T & k, const V & v)
		{
			auto [ptr, overwrite] = index.Insert(k,link_t(0));

			if (!overwrite)
			{
				size_t size = v.size();

				if (size < min_alloc)
					size = min_alloc;

				auto [bucket, offset] = io->Incidental(sizeof(link) + sizeof(header) + size);

				auto ph = (header*)bucket;
				auto lh = (link*)(bucket + sizeof(header));

				ph->total = v.size();
				ph->last = 0;
				lh->next = 0;
				lh->size = v.size();

				uint8_t* bin = (uint8_t*)(lh + 1);
				std::copy(v.begin(), v.end(), bin);

				ptr = offset;
				return;
			}

			auto _header = io->GetObject(*ptr);
			auto ph = (header*)_header;
			auto lh = (link*)((ph->last) ? io->GetObject(ph->last) : (bucket + sizeof(header)));
			auto rem = v.size(), off = 0;

			uint8_t* bin = (uint8_t*)(lh + 1);
			auto available = min_alloc - lh->size();

			if (available > rem)
				available = rem;
				
			std::copy(v.begin(), v.begin() + available, bin + lh.size);

			off += available;
			rem -= available;
			lh.size += available;

			if (rem)
			{
				size_t size = rem;

				if (size < min_alloc)
					size = min_alloc;

				auto [bucket, offset] = io->Incidental(sizeof(link) + size);

				auto lh2 = (link*)buck;

				lh2->next = 0;
				lh2->size = rem;

				bin = (uint8_t*)(lh2 + 1);

				std::copy(v.begin()+off, v.end(), bin);

				ph->last = lh->next = offset;
			}

			ph->total += v.size();
		}

		template < typename K > std::vector<uint8_t> Read(const K& k)
		{
			auto ptr = index.Find(k);

			if (!ptr)
				return std::vector<uint8_t>();

			auto _header = io->GetObject(*ptr);
			auto ph = (header*)_header;
			auto lh = (link*)(ph + 1);

			size_t off = 0;
			uint8_t* bin = (uint8_t*)(lh + 1);

			std::vector<uint8_t> result(ph->total);

			do
			{
				std::copy(bin,bin+lh.size,result.data()+off);
				off += lh->size;

				lh = (link*)((lh->next) ? io->GetObject(*ptr) : nullptr);
			} while (lh);

			return result;
		}
	};
}