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

			index.Open(io, _n);
		}

		template <typename T, typename V> void Insert(const T& k, const V& v)
		{
			return Write(k, v);
		}

		template <typename T, typename V> auto WriteT(const T& k, const V& v)
		{
			return Write(k,gsl::span<uint8_t>((uint8_t*)&v,sizeof(V)));
		}

		template <typename T, typename V> auto Write(const T & k, const V & v)
		{
			if (!v.size()) return std::make_pair((link_t*)nullptr,false);

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

				*ptr = offset;
				return std::make_pair(ptr, false);
			}

			auto _header = io->GetObject(*ptr);

			auto ph = (header*)_header;
			auto lh = (link*)((ph->last) ? io->GetObject(ph->last) : (_header + sizeof(header)));

			size_t rem = v.size(), off = 0;

			uint8_t* bin = (uint8_t*)(lh + 1);
			auto available = (lh->size > min_alloc) ? 0 : min_alloc - lh->size;

			if (available > rem)
				available = rem;
				
			if (available)
			{
				std::copy(v.begin(), v.begin() + available, bin + lh->size);

				off += available;
				rem -= available;
				lh->size += available;
			}

			if (rem)
			{
				size_t size = rem;

				if (size < min_alloc)
					size = min_alloc;

				auto [bucket, offset] = io->Incidental(sizeof(link) + size);

				auto lh2 = (link*)bucket;

				lh2->next = 0;
				lh2->size = rem;

				bin = (uint8_t*)(lh2 + 1);

				std::copy(v.begin()+off, v.end(), bin);

				ph->last = lh->next = offset;
			}

			ph->total += v.size();

			return std::make_pair(ptr, true);
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

			std::vector<uint8_t> result(ph->total);

			do
			{
				uint8_t* bin = (uint8_t*)(lh + 1);

				std::copy(bin,bin+lh->size,result.data()+off);
				off += lh->size;

				lh = (link*)((lh->next) ? io->GetObject(lh->next) : nullptr);
			} while (lh);

			return result;
		}

		template < typename K, typename F > void Stream(const K& k, F && f)
		{
			auto ptr = index.Find(k);

			if (!ptr) return;

			auto _header = io->GetObject(*ptr);
			auto ph = (header*)_header;
			auto lh = (link*)(ph + 1);

			do
			{
				uint8_t* bin = (uint8_t*)(lh + 1);

				if (!f(gsl::span<uint8_t>(bin, lh->size)))
					break;

				lh = (link*)((lh->next) ? io->GetObject(lh->next) : nullptr);
			} while (lh);
		}
	};

	template < typename R, typename index_t > using Stream = _StreamBucket<R, uint64_t, uint64_t, 1024, index_t>;

	//TODO FIXED BUCKET
	//Better for random IO
	//Is it needed? Will implement when needed.
}