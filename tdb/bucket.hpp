/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "types.hpp"

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

		bool Validate() 
		{
			//TODO VALIDATE BUCKETS
			return index.Validate();
		}

		void Open(R* _io, size_t& _n)
		{
			io = _io;

			index.Open(io, _n);

			auto & desc = io->GetDescriptor(_n - 1);
			desc.standard_index.pointer_mode = PointerMode::pointer_mode_bucket;
		}

		template <typename T, typename V> void Insert(const T& k, const V& v)
		{
			return Write<false>(k, v);
		}

		template <typename T, typename V> auto Write(const T& k, const V& v)
		{
			return _Write<false>(k,v, std::is_integral<V>());
		}

		template <typename T, typename V> auto _Write(const T& k, const V& v, std::true_type)
		{
			return _Write<false>(k,v,std::bool_constant<false>());
		}

		template <typename T, typename V> void InsertLock(const T& k, const V& v)
		{
			return Write<true>(k, v);
		}

		template <typename T, typename V> auto WriteLock(const T& k, const V& v)
		{
			return _WriteLock(k, v, std::is_integral<V>());
		}

		template <typename T, typename V> auto _WriteLock(const T& k, const V& v, std::true_type)
		{
			return _Write<true>(k, v, std::bool_constant<false>());
		}

		template <typename T, typename V> auto _WriteLock(const T& k, const V& v, std::false_type)
		{
			return _Write<true>(k, v, std::bool_constant<false>());
		}

		template <bool lock_v, typename T, typename V> auto _Write(const T & k, const V & v, std::false_type)
		{
			if (!v.size()) return std::make_pair((link_t*)nullptr,false);

			auto do_insert = [&](auto handle)
			{
				auto [ptr, overwrite] = handle;

				if (!ptr)
					std::cout << std::endl;

				if (!overwrite) //The overwrite indicator is atomic
				{
					size_t size = v.size();

					if (size < min_alloc)
						size = min_alloc;

					auto [bucket, offset] = io->Incidental(sizeof(link) + sizeof(header) + size);

					if (!offset)
						offset = 0;

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

				if (!lh)
					std::cout << std::endl;

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

					std::copy(v.begin() + off, v.end(), bin);

					ph->last = lh->next = offset;
				}

				ph->total += v.size();

				return std::make_pair(ptr, true);
			};

			if constexpr (lock_v)
				return index.InsertLockContext(k, link_t(0),do_insert);
			else
				return do_insert(index.Insert(k, link_t(0)));
		}

		template < typename K > std::vector<uint8_t> Read(const K& k) // todo when needed, locking version.
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

		template < typename K, typename F > void Stream(const K& k, F && f) // todo when needed, locking version.
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