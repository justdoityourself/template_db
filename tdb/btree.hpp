/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <atomic>
#include <tuple>
#include <utility>
#include <thread>
#include <chrono>

#include "../gsl-lite.hpp"

#include "keys.hpp"
#include "types.hpp"

namespace tdb
{
	using namespace std;

#pragma pack(push,1)
#pragma warning( push )
#pragma warning( disable : 4200 )

	template <typename T> struct ScopedLock
	{
		bool locked = false;
		T& target;
		ScopedLock(T& t) : target(t) 
		{ 
			if (target.count != T::Bins)
			{
				locked = true;
				target.Lock();
			}
		}

		~ScopedLock() 
		{ 
			if(locked)
				target.Unlock(); 
		}
	};

	template < typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c, bool check_v = false > struct _BaseNode
	{
		static const uint64_t _guard = 0xfe3451dceabc45afull;

		key_t keys[bin_c];

		pointer_t pointers[bin_c];

		link_t links[link_c] = { 0 };

		int_t count = 0;

		int_t checksum = (int_t)0;

		int_t footer_guard = (int_t)_guard;

		void Lock()
		{
			if (footer_guard != 0 && footer_guard != (int_t)_guard)
				throw std::runtime_error("Bad Node");

			static constexpr auto lock_delay = std::chrono::milliseconds(5);

			auto lock = (std::atomic<int_t> * )& footer_guard;

			int_t expected = (int_t)_guard;
			while (!lock->compare_exchange_weak(expected, 0, std::memory_order_acquire))
			{
				expected = (int_t)_guard;
				std::this_thread::sleep_for(lock_delay);
			}
		}

		void Wait()
		{
			static constexpr auto lock_delay = std::chrono::milliseconds(5);

			auto lock = (std::atomic<int_t>*) & footer_guard;

			while (lock->load() == 0)
				std::this_thread::sleep_for(lock_delay);
		}

		void Unlock()
		{
			auto lock = (std::atomic<int_t> * )& footer_guard;

			lock->store((int_t)_guard, std::memory_order_release);
		}

		void Expand(int c)
		{
			for (int i = (int)count - 1; i >= c; i--)
				memcpy(keys + (i + 1), keys + i, sizeof(key_t));

			for (int i = (int)count - 1; i >= c; i--)
				memcpy(pointers + (i + 1), pointers + i, sizeof(pointer_t));

			count++;
		}

		bool Validate()
		{
			if constexpr (check_v)
			{
				Lock();
				int_t _checksum = 0;

				for (int_t i = 0; i < count; i++)
				{
					int_t* ptr = (int_t*)&keys[i];

					for (size_t i = 0; i < sizeof(key_t) / sizeof(int_t); i++, ptr++)
						_checksum ^= *ptr;
				}

				Unlock();

				return _checksum == checksum;
			}
			else
				return true;
		}

		void CheckKey(const key_t&k)
		{
			if constexpr (check_v)
			{
				static_assert(sizeof(key_t) % sizeof(int_t) == 0);
				int_t* ptr = (int_t*)&k;

				for (size_t i = 0; i < sizeof(key_t) / sizeof(int_t); i++, ptr++)
					checksum ^= *ptr;
			}
		}
	};

	//TODO LOCAL SURROGATE, benchmark
	/*template < typename surrogate_t,typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c > struct _LocalSurrogateBaseNode
	{
		static const uint64_t _guard = 0xfe3451dceabc45afull;

		key_t keys[bin_c];

		pointer_t pointers[bin_c];

		surrogate_t list[bin_c];

		link_t links[link_c] = { 0 };

		int_t count = 0;

		int_t checksum = (int_t)0;

		int_t footer_guard = (int_t)_guard;

		void Lock()
		{
			static constexpr auto lock_delay = std::chrono::milliseconds(5);

			auto lock = (std::atomic<int_t>*) & footer_guard;

			int_t expected = (int_t)_guard;
			while (!lock->compare_exchange_weak(expected, 0, std::memory_order_acquire))
			{
				expected = (int_t)_guard;
				std::this_thread::sleep_for(lock_delay);
			}
		}

		void Wait()
		{
			static constexpr auto lock_delay = std::chrono::milliseconds(5);

			auto lock = (std::atomic<int_t>*) & footer_guard;

			while (lock->load() == 0)
				std::this_thread::sleep_for(lock_delay);
		}

		void Unlock()
		{
			auto lock = (std::atomic<int_t>*) & footer_guard;

			lock->store((int_t)_guard, std::memory_order_release);
		}

		void Expand(int c)
		{
			for (int i = (int)count - 1; i >= c; i--)
				memcpy(list + (i + 1), list + i, sizeof(surrogate_t));

			count++;
		}
	};*/

	template < typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c, size_t padding_c = 0, bool check_v = false > struct _OrderedListNode : public _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>
	{
		static const uint32_t type = TableType::btree_sorted_list;

		uint8_t padding[padding_c];

		using Pointer = pointer_t;
		using Key = key_t;
		using Int = int_t;
		using Link = link_t;
		static const int Bins = bin_c;
		static const int Links = link_c;
		static const int Padding = padding_c;

		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::keys;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::pointers;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::links;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::count;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::CheckKey;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::Expand;

		void Init() {}

		size_t max_rec() { return (size_t)-1; }

		int Insert(const key_t& k, const pointer_t& p, pair<pointer_t*, bool>& overwrite, size_t depth,void*ref_pages)
		{
			overwrite = { nullptr,false };
			if (!count)
			{
				CheckKey(k);

				keys[0] = k;
				pointers[0] = p;
				count++;

				overwrite = { pointers,false };

				return 0;
			}

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k,ref_pages,nullptr))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					overwrite = { pointers + middle,true };
					//pointers[middle] = p; // We allow the call to update the existing object if needed / wanted.
					return 0;
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low * link_c / bin_c) + 1;
			}
			else
			{
				CheckKey(k);

				Expand(low);

				keys[low] = k;
				pointers[low] = p;

				overwrite = { pointers + low, false };

				return 0;
			}
		}

		int Find(const key_t& k, pointer_t** pr, size_t depth, void* ref_pages, void * ref_page2)
		{
			*pr = nullptr;

			if (!count)
				return 0;

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k, ref_pages,ref_page2))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					*pr = pointers + middle;
					return 0;
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low * link_c / bin_c) + 1;
			}
			else
				return 0;
		}

		template < typename F > std::pair<int,int> FindRange(F && f, const key_t& low_k, const key_t& high_k, size_t depth, void* ref_pages, void* ref_page2)
		{
			if (!count)
				return std::make_pair(0,0);

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(low_k, ref_pages, ref_page2))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					while (middle - 1 > 0 && keys[middle - 1].Compare(low_k, ref_pages, ref_page2) >= 0) middle--;
					low = middle;
					goto BREAK;
				}
			}
		BREAK:


			if (low == bin_c)
				low--;
			else if (low == -1)
				low++;

			for (high = low; high < count && keys[high].Compare(low_k, ref_pages, ref_page2) >= 0 && keys[high].Compare(high_k, ref_pages, ref_page2) <= 0; high++)
				f(keys[high],pointers[high]);

			if (count == bin_c)
			{
				if (high == bin_c)
					high--;
				else if (high == -1)
					high++;

				return std::make_pair((low * link_c / bin_c) + 1, (high * link_c / bin_c) + 1);
			}
			else
				return std::make_pair(0, 0);			
		}
	};

	//TODO LOCAL SURROGATE, benchmark
	/*template < typename surrogate_t, typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c, size_t padding_c = 0 > struct _LocalSurrogateOrderedListNode : public _LocalSurrogateBaseNode<surrogate_t,int_t, key_t, pointer_t, link_t, bin_c, link_c>
	{
		uint8_t padding[padding_c];

		using Pointer = pointer_t;
		using Key = key_t;
		using Int = int_t;
		using Link = link_t;
		static const int Bins = bin_c;
		static const int Links = link_c;
		static const int Padding = padding_c;

		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::keys;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::pointers;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::links;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::count;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::Expand;

		void Init() {}

		size_t max_rec() { return (size_t)-1; }

		int Insert(const key_t& k, const pointer_t& p, pair<pointer_t*, bool>& overwrite, size_t depth, void* ref_pages)
		{
			overwrite = { nullptr,false };
			if (!count)
			{
				keys[0] = k;
				pointers[0] = p;
				count++;

				overwrite = { pointers,false };

				return 0;
			}

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k, ref_pages, nullptr))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					overwrite = { pointers + middle,true };
					//pointers[middle] = p; // We allow the call to update the existing object if needed / wanted.
					return 0;
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low * link_c / bin_c) + 1;
			}
			else
			{
				Expand(low);

				keys[low] = k;
				pointers[low] = p;

				overwrite = { pointers + low, false };

				return 0;
			}
		}

		int Find(const key_t& k, pointer_t** pr, size_t depth, void* ref_pages, void* ref_page2)
		{
			*pr = nullptr;

			if (!count)
				return 0;

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k, ref_pages, ref_page2))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					*pr = pointers + middle;
					return 0;
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low * link_c / bin_c) + 1;
			}
			else
				return 0;
		}
	};*/


	template < typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c, size_t padding_c = 0, bool check_v = false > struct _OrderedMultiListNode : public _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>
	{
		static const uint32_t type = TableType::btree_sorted_multilist;

		uint8_t padding[padding_c];

		using Pointer = pointer_t;
		using Key = key_t;
		using Int = int_t;
		using Link = link_t;
		static const int Bins = bin_c;
		static const int Links = link_c;
		static const int Padding = padding_c;

		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::keys;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::pointers;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::links;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::count;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::Expand;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::CheckKey;

		void Init() {}

		size_t max_rec() { return (size_t)-1; }

		int Insert(const key_t& k, const pointer_t& p, pair<pointer_t*, bool>& overwrite, size_t depth, void* ref_pages)
		{
			overwrite = { nullptr,false };
			if (!count)
			{
				CheckKey(k);

				keys[0] = k;
				pointers[0] = p;
				count++;

				overwrite = { pointers,false };

				return 0;
			}

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k, ref_pages, nullptr))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:

					if (count == bin_c)
					{
						//To standardize which node pathway we use we must find the lower bound of this match:
						//

						while (middle > 0 && keys[middle - 1].Compare(k, ref_pages, nullptr) == 0)
							middle--;

						return (middle * link_c / bin_c) + 1;
					}
					else
					{
						CheckKey(k);

						Expand(middle);

						keys[middle] = k;
						pointers[middle] = p;

						overwrite = { pointers + low, false };

						return 0;
					}
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low * link_c / bin_c) + 1;
			}
			else
			{
				CheckKey(k);

				Expand(low);

				keys[low] = k;
				pointers[low] = p;

				overwrite = { pointers + low, false };

				return 0;
			}
		}

		template < typename F > int MultiFind(F&& f,const key_t& k, size_t depth, void* ref_pages, void* ref_page2)
		{
			if (!count)
				return 0;

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k, ref_pages, ref_page2))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					if (count == bin_c)
					{
						//To standardize which node pathway we use we must find the lower bound of this match:
						//

						auto upper = middle;

						if (!f(pointers + middle))
							return 0;

						while (upper < bin_c - 1 && keys[upper + 1].Compare(k, ref_pages, ref_page2) == 0)
							if (!f(pointers + ++upper))
								return 0;

						while (middle > 0 && keys[middle - 1].Compare(k, ref_pages, ref_page2) == 0)
							if (!f(pointers + --middle))
								return 0;

						return (middle * link_c / bin_c) + 1;
					}
					else
					{
						auto upper = middle;

						if (!f(pointers + middle))
							return 0;

						while (upper < count - 1 && keys[upper + 1].Compare(k, ref_pages, ref_page2) == 0)
							if (!f(pointers + ++upper))
								return 0;

						while (middle > 0 && keys[middle - 1].Compare(k, ref_pages, ref_page2) == 0)
							if (!f(pointers + --middle))
								return 0;

						return 0;
					}
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low * link_c / bin_c) + 1;
			}
			else
				return 0;
		}

		int Find(const key_t& k, pointer_t** pr, size_t depth, void* ref_pages, void* ref_page2)
		{
			*pr = nullptr;

			if (!count)
				return 0;

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k, ref_pages, ref_page2))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					*pr = pointers + middle;
					return 0;
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low * link_c / bin_c) + 1;
			}
			else
				return 0;
		}

		template < typename F > std::pair<int, int> FindRange(F&& f, const key_t& low_k, const key_t& high_k, size_t depth, void* ref_pages, void* ref_page2)
		{
			if (!count)
				return std::make_pair(0, 0);

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(low_k, ref_pages, ref_page2))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					while (middle-1 > 0 && keys[middle-1].Compare(low_k, ref_pages, ref_page2) >= 0) middle--;
					low = middle;
					goto BREAK;
				}
			}
		BREAK:

			if (low == bin_c)
				low--;
			else if (low == -1)
				low++;

			for (high = low; high < count && keys[high].Compare(low_k, ref_pages, ref_page2) >= 0 && keys[high].Compare(high_k, ref_pages, ref_page2) <= 0; high++)
				f(keys[high],pointers[high]);

			if (count == bin_c)
			{
				if (high == bin_c)
					high--;
				else if (high == -1)
					high++;

				return std::make_pair((low * link_c / bin_c) + 1, (high * link_c / bin_c) + 1);
			}
			else
				return std::make_pair(0, 0);
		}
	};

	template < typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c, size_t fuzz_c, size_t padding_c = 0, bool check_v = false > struct _FuzzyHashNode : public _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>
	{
		static const uint32_t type = TableType::btree_fuzzymap;

		uint8_t padding[padding_c];

		using Pointer = pointer_t;
		using Key = key_t;
		using Int = int_t;
		using Link = link_t;
		static const size_t Fuzz = fuzz_c;
		static const size_t Bins = bin_c;
		static const size_t Links = link_c;
		static const size_t Padding = padding_c;

		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::keys;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::pointers;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::links;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::count;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c, check_v>::CheckKey;

		void Init()
		{
			for (size_t i = 0; i < bin_c; i++)
				pointers[i] = (pointer_t)-1;
		}

		size_t max_rec() { return sizeof(key_t)/2; }

		int Insert(const key_t& k, const pointer_t& p, pair<pointer_t*, bool>& overwrite, size_t depth, void* ref_pages)
		{
			int bin = (*(((uint16_t*)&k) + (depth % max_rec())) % bin_c);

			if (pointers[bin] == (pointer_t)-1)
			{
				CheckKey(k);

				keys[bin] = k;
				pointers[bin] = p;
				count++;

				overwrite = { pointers + bin, false };

				return 0;
			}

			overwrite = { nullptr,false };

			int low = bin - fuzz_c / 2;
			int high = low + fuzz_c;
			int z = -1;

			if (low < 0) low = 0;
			if (high > bin_c) high = bin_c;

			while (low < high)
			{
				if (pointers[low] == (pointer_t)-1)
				{
					z = low++;
					continue;
				}

				if (keys[low].Equal(k, ref_pages,nullptr))
				{
					overwrite = { pointers + low,true };
					//pointers[middle] = p; // We allow the call to update the existing object if needed.
					return 0;
				}

				low++;
			}

			if (z != -1)
			{
				CheckKey(k);

				keys[z] = k;
				pointers[z] = p;
				count++;

				overwrite = { pointers + z, false };

				return 0;
			}
			else
				return ((low-1) * link_c / bin_c) + 1;
		}

		int Find(const key_t& k, pointer_t** pr, size_t depth, void* ref_pages, void* ref_page2)
		{
			*pr = nullptr;

			if (!count)
				return 0;

			int bin = (*(((uint16_t*)&k) + depth) % bin_c);

			if (keys[bin].Equal(k,ref_pages,ref_page2))
			{
				*pr = pointers + bin;
				return 0;
			}

			int low = bin - fuzz_c / 2;
			int high = low + fuzz_c;
			int z = -1;

			if (low < 0) low = 0;
			if (high > bin_c) high = bin_c;

			while (low < high)
			{
				if (pointers[low] == (pointer_t)-1)
				{
					z = low++;
					continue;
				}

				if (keys[low].Equal(k, ref_pages, ref_page2))
				{
					*pr = pointers + low;
					return 0;
				}

				low++;
			}

			if (z != -1)
				return 0;
			else
				return ((low - 1) * link_c / bin_c) + 1;
		}
	};

#pragma warning( pop )
#pragma pack(pop)



	template < typename R, typename node_t, size_t double_stall_s = 1, size_t double_max_s = -1 > class _BTree
	{
		using key_t = typename node_t::Key;
		using pointer_t = typename node_t::Pointer;
		using link_t = typename node_t::Link;
		static const int link_c = node_t::Links;

		R* io = nullptr;
		link_t root_n;

		auto Root() const
		{
			return &io->template Lookup<node_t>(root_n);
		}

	public:
		_BTree() {}

		_BTree(R* _io)
		{
			Open(_io);
		}

		void Open(R* _io, size_t & _n)
		{
			root_n = _n++;
			io = _io;

			if (io->size() <= root_n)
			{
				auto r = &io->template Allocate<node_t>();
				r->Init();

				/*
					Runtime Introspection:
				*/

				auto & desc = io->GetDescriptor(root_n);

				desc.type = node_t::type;

				desc.standard_index.self_balanced = (uint32_t)false;
				desc.standard_index.requires_distributed_key = (uint32_t)true;

				desc.standard_index.key_sz = sizeof(key_t);
				desc.standard_index.link_sz = sizeof(link_t);
				desc.standard_index.pointer_sz = sizeof(pointer_t);
				desc.standard_index.key_mode = key_t::mode;
				desc.standard_index.key_type = key_t::type;

				desc.standard_index.max_capacity = (uint32_t)node_t::Bins;
				desc.standard_index.min_capacity = (uint16_t)-1;

				desc.standard_index.max_page = (uint32_t)sizeof(node_t);
				desc.standard_index.min_page = (uint16_t)-1;

				desc.standard_index.link_count = node_t::Links;
			}
		}

private: 
		
		void _Population(node_t* node, std::pair<uint64_t,uint64_t> & sum) const
		{
			if (!node->count)
				return;

			sum.first += node->count;
			sum.second += node_t::Bins;

			for (int i = 0; i < link_c; i++)
				if (node->links[i])
					_Population(&io->template Lookup<node_t>((uint64_t)node->links[i]), sum);
		}

		template < typename F > void _IterateNodes(node_t* node, F&& f) const
		{
			f(*node);

			for (int i = 0; i < link_c; i++)
				if (node->links[i])
					_IterateNodes(&io->template Lookup<node_t>((uint64_t)node->links[i]), f);
		}

		template < typename F > void IterateNodes(F&& f) const
		{
			if (Root())
				_IterateNodes(Root(), std::move(f));
		}

		template < typename F > int _Iterate(node_t* node, F && f) const
		{
			int count = node->count;

			if (!count)
				return 0;

			for (int i = 0, c = 0; i < (int)node->count; c++)
			{
				if(node->pointers[c] != (pointer_t)-1) // this is designed to filter out unused type in fuzzy map. Doesn't work with non int value types.
				{ 
					i++;
					if (!f(node->pointers[c]))
						return i;
				}
			}

			for (int i = 0; i < link_c; i++)
				if (node->links[i])
					count += _Iterate(&io->template Lookup<node_t>((uint64_t)node->links[i]), f);

			return count;
		}

		template < typename F > int _IterateKV(node_t* node, F&& f) const
		{
			int count = node->count;

			if (!count)
				return 0;

			for (int i = 0, c = 0; i < (int)node->count; c++)
			{
				//if (node->pointers[c] != (pointer_t)-1) //Disabling fuzzy map filtering for K/V version
				{
					i++;
					if (!f(node->keys[c],node->pointers[c]))
						return i;
				}
			}

			for (int i = 0; i < link_c; i++)
				if (node->links[i])
					count += _IterateKV(&io->template Lookup<node_t>((uint64_t)node->links[i]), f);

			return count;
		}

		bool _Validate(node_t* node) const
		{
			if (!node->Validate()) 
				return false;

			for (int i = 0; i < link_c; i++)
			{
				if (node->links[i])
				{
					if (!_Validate(&io->template Lookup<node_t>((uint64_t)node->links[i]))) 
						return false;
				}
			}

			return true;
		}

public: 

		bool Validate() const
		{
			if (!Root())
				return true;
			else
				return _Validate(Root());
		}

		int ResetNodeLocks()
		{
			int result = 0;
			IterateNodes([&](auto & node)
			{
				if (node.footer_guard)
				{
					result++;
					node.footer_guard = 0;
				}
			});

			return result;
		}

		std::pair<uint64_t, uint64_t> Population()
		{
			auto sum = std::make_pair(uint64_t(0), uint64_t(0));

			_Population(Root(), sum);

			return sum;
		}
		
		template < typename F > int Iterate(F &&f) const
		{
			if (!Root())
				return 0;
			else
				return _Iterate(Root(),std::move(f));
		}

		template < typename F > int IterateKV(F&& f) const
		{
			if (!Root())
				return 0;
			else
				return _IterateKV(Root(), std::move(f));
		}

		pointer_t* Find(const key_t& k, void* ref_page=nullptr) const
		{
			node_t* current = Root();

			if (!current)
				return nullptr;

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				pointer_t* pr;
				int result = current->Find(k, &pr, depth++, (void*)io,ref_page);

				if (!result)
					return pr;
				else
				{
					if (depth % double_stall_s != 0 || depth > double_max_s)
						result = 1;

					result--;

					next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

					if (!next)
						return nullptr;
					else
						current = next;
				}
			}

			return nullptr;
		}

		template <typename F> void MultiFind(F && f, const key_t& k, void* ref_page = nullptr) const
		{
			node_t* current = Root();

			if (!current)
				return;

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				int result = current->MultiFind(f, k, depth++, (void*)io, ref_page);

				if (!result)
					return;
				else
				{
					if (depth % double_stall_s != 0 || depth > double_max_s)
						result = 1;

					result--;

					next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

					if (!next)
						return;
					else
						current = next;
				}
			}
		}

		template <typename F> void RangeFind(F&& f, const key_t& low_k, const key_t& high_k, void* ref_page = nullptr, node_t* current = nullptr) const
		{
			if (!current)
				current = Root();

			auto [low,high] = current->FindRange(f, low_k, high_k, 0, (void*)io, ref_page);

			if (!low && !high)
				return;
			else
			{
				low--; high--;

				for (;low <= high;low++)
				{
					auto next = (current->links[low]) ? &io->template Lookup<node_t>(current->links[low]) : nullptr;
					if(next)
						RangeFind(f, low_k, high_k, ref_page, next);
				}

			}
		}

		void Insert(const gsl::span<key_t> & ks, const pointer_t &p)
		{
			for(auto & k: ks)
				Insert(k,p);
		}

		pair<pointer_t*, bool> Insert(const key_t& k, const pointer_t& p)
		{
			node_t* current = Root();
			link_t current_id = root_n;

			if (!current)
				return { nullptr,false };

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				//if(depth >= current->max_rec()) //Recursion should roll around using mod.
				//	return { nullptr,false };

				pair<pointer_t*, bool> overwrite;
				int result = current->Insert(k, p, overwrite,depth++,(void*)io);

				if (!result)
					return overwrite;
				else
				{
					if (depth % double_stall_s != 0 || depth > double_max_s)
						result = 1;

					result--;

					next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

					if (!next)
					{
						next = &io->template Allocate<node_t>();
						next->Init();

						//If the file was remapped, then this must be refreshed
						current = &io->template Lookup<node_t>(current_id);

						current->links[result] = io->template Index<node_t>(*next);
					}

					current_id = current->links[result];
					current = next;
				}
			}

			return { nullptr,false };
		}

		pointer_t* FindLock(const key_t& k, void* ref_page = nullptr) const
		{
			node_t* current = Root();

			if (!current)
				return nullptr;

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				ScopedLock lock(*current); //Only works if no remap is allowed

				pointer_t* pr;
				int result = current->Find(k, &pr,depth++, (void*)io, ref_page);

				if (!result)
					return pr;
				else
				{
					if (depth % double_stall_s != 0 || depth > double_max_s)
						result = 1;

					result--;

					next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

					if (!next)
						return nullptr;
					else
						current = next;
				}
			}

			return nullptr;
		}

		void InsertLock(const gsl::span<key_t>& ks, const pointer_t& p)
		{
			for (auto& k : ks)
				InsertLock(k, p);
		}

		pair<pointer_t*, bool> InsertLock(const key_t& k, const pointer_t& p)
		{
			node_t* current = Root();
			link_t current_id = root_n;

			if (!current)
				return { nullptr,false };

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				//if (depth >= current->max_rec())
				//	return { nullptr,false };

				bool locked = false;
				if (current->count != node_t::Bins) //Must be locked to insert
				{
					current->Lock(); 
					locked = true;
				}

				pair<pointer_t*, bool> overwrite;
				int result = current->Insert(k, p, overwrite,depth++, (void*)io);

				if (!result)
				{
					if(locked) current->Unlock();
					return overwrite;
				}
				else
				{
					if (depth % double_stall_s != 0 || depth > double_max_s)
						result = 1;

					result--;

					next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

					if (!next)
					{ 
						if (!locked) //We must be locked to edit links
						{
							current->Lock();
							locked = true;
						}

						next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

						if (!next)
						{
							next = &io->template AllocateLock<node_t>();
							next->Init();

							//If the file was remapped, then this must be refreshed
							current = &io->template Lookup<node_t>(current_id);

							current->links[result] = (link_t)io->template Index<node_t>(*next);
						}
					}

					current_id = current->links[result];
					if (locked) current->Unlock();

					current = next;
				}
			}

			return { nullptr,false };
		}

		template <typename F> pair<pointer_t*, bool> InsertLockContext(const key_t& k, const pointer_t& p, F && f)
		{
			node_t* current = Root();
			link_t current_id = root_n;

			if (!current)
				return f(pair<pointer_t*, bool>{ nullptr, false });

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				//if (depth >= current->max_rec())
				//	return f(pair<pointer_t*, bool>{ nullptr, false });

				bool locked = false;
				if (current->count != node_t::Bins) //Must be locked to insert
				{
					current->Lock();
					locked = true;
				}

				pair<pointer_t*, bool> overwrite;
				int result = current->Insert(k, p, overwrite, depth++, (void*)io);

				if (!result)
				{
					auto tmp = f(overwrite);
					if (locked) current->Unlock();
					return tmp;
				}
				else
				{
					if (depth % double_stall_s != 0 || depth > double_max_s)
						result = 1;

					result--;

					next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

					if (!next)
					{
						if (!locked) //We must be locked to edit links
						{
							current->Lock();
							locked = true;
						}

						next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

						if (!next)
						{
							next = &io->template AllocateLock<node_t>();
							next->Init();

							//If the file was remapped, then this must be refreshed
							current = &io->template Lookup<node_t>(current_id);

							current->links[result] = io->template Index<node_t>(*next);
						}
					}

					current_id = current->links[result];
					if (locked) current->Unlock();

					current = next;
				}
			}

			return f(pair<pointer_t*, bool>{ nullptr,false });
		}
	};



	template <size_t page_s, typename int_t, typename key_t, typename pointer_t, typename link_t, size_t link_c, bool check_v = false>
	using OrderedListBuilder = _OrderedListNode<	int_t,
													key_t,
													pointer_t,
													link_t,
													(page_s - sizeof(int_t) * 3 - sizeof(link_t) * link_c) / (sizeof(key_t) + sizeof(pointer_t)),
													link_c,
													(page_s - sizeof(int_t) * 3 - sizeof(link_t) * link_c) % (sizeof(key_t) + sizeof(pointer_t)), check_v >;


	template <size_t page_s, typename int_t, typename key_t, size_t link_c = 4, bool check_v = false> using SimpleOrderedListBuilder = OrderedListBuilder<page_s, int_t, key_t, int_t, int_t, link_c, check_v>;



	template <size_t page_s, typename int_t, typename key_t, typename pointer_t, typename link_t, size_t link_c = 4, bool check_v = false>
	using MultiListBuilder = _OrderedMultiListNode<	int_t,
													key_t,
													pointer_t,
													link_t,
													(page_s - sizeof(int_t) * 3 - sizeof(link_t) * link_c) / (sizeof(key_t) + sizeof(pointer_t)),
													link_c,
													(page_s - sizeof(int_t) * 3 - sizeof(link_t) * link_c) % (sizeof(key_t) + sizeof(pointer_t)), check_v >;


	template <size_t page_s, typename int_t, typename key_t, size_t link_c = 4, bool check_v = false> using SimpleMultiListBuilder = MultiListBuilder<page_s, int_t, key_t, int_t, int_t, link_c, check_v>;



	template <size_t page_s, typename int_t, typename key_t, typename pointer_t, typename link_t, size_t link_c, size_t fuzzy_c, bool check_v = false>
	using FuzzyHashBuilder = _FuzzyHashNode <	int_t,
												key_t,
												pointer_t,
												link_t,
												(page_s - sizeof(int_t) * 3 - sizeof(link_t) * link_c) / (sizeof(key_t) + sizeof(pointer_t)),
												link_c,
												fuzzy_c,
												(page_s - sizeof(int_t) * 3 - sizeof(link_t) * link_c) % (sizeof(key_t) + sizeof(pointer_t)), check_v >;

	template <size_t page_s, typename int_t, typename key_t, size_t fuzzy_c = 4, size_t link_c = 4, bool check_v = false> using SimpleFuzzyHashBuilder = FuzzyHashBuilder<page_s, int_t, key_t, int_t, int_t, link_c, fuzzy_c, check_v>;



	template < typename R, typename N, size_t doubling_stall = 1, size_t doubling_max = -1 > using BTree = _BTree<R, N, doubling_stall, doubling_max>;
}