/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <atomic>
#include <tuple>
#include <utility>
#include <thread>
#include <chrono>

#include "keys.hpp"

namespace tdb
{
	using namespace std;

#pragma pack(push,1)
#pragma warning( push )
#pragma warning( disable : 4200 )

	template <typename T, size_t C> struct _OrderedArray
	{
		array<T, C> elements;
		size_t count = 0;

		void Expand(int c)
		{
			for (int i = (int)count - 1; i >= c; i--)
				elements[i + 1] = elements[i];

			count++;
		}

		template <typename F> int Insert(const T& k, F f)
		{
			if (count == C)
				return -1;

			if (!count)
			{
				elements[0] = k;
				count++;

				return 0;
			}

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (f(elements[middle], k))
				{
				case -1:
					low = middle + 1;
					break;
				case 1:
					high = middle - 1;
					break;
				case 0:
					low = middle;
					goto BREAK;
				}
			}
		BREAK:
			Expand(low);

			elements[low] = k;

			return 0;
		}
	};

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

	template < typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c > struct _BaseNode
	{
		static const uint64_t _guard = 0xfe3451dceabc45afull;

		key_t keys[bin_c];

		pointer_t pointers[bin_c];

		link_t links[link_c] = { 0 };

		int_t count = 0;

		int_t header_guard = (int_t)_guard; //Moved to tail for better cacheline alignement.

		int_t footer_guard = (int_t)_guard;

		void Lock()
		{
			static constexpr auto lock_delay = std::chrono::milliseconds(5);

			auto lock = (std::atomic<int_t> * )&header_guard;

			int_t expected = (int_t)_guard;
			while (!lock->compare_exchange_strong(expected, 0))
			{
				expected = (int_t)_guard;
				std::this_thread::sleep_for(lock_delay);
			}
		}

		void Wait()
		{
			static constexpr auto lock_delay = std::chrono::milliseconds(5);

			auto lock = (std::atomic<int_t>*) & header_guard;

			while (lock->load() == 0)
				std::this_thread::sleep_for(lock_delay);
		}

		void Unlock()
		{
			auto lock = (std::atomic<int_t> * )&header_guard;

			lock->store((int_t)_guard);
		}

		void Expand(int c)
		{
			for (int i = (int)count - 1; i >= c; i--)
				memcpy(keys + (i + 1), keys + i, sizeof(key_t));

			for (int i = (int)count - 1; i >= c; i--)
				memcpy(pointers + (i + 1), pointers + i, sizeof(pointer_t));

			count++;
		}


	};

	template < typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c, size_t padding_c = 0 > struct _OrderedListNode : public _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>
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

		int Insert(const key_t& k, const pointer_t& p, pair<pointer_t*, bool>& overwrite, size_t depth)
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

				switch (keys[middle].Compare(k))
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
				//return (low > bin_c / 2) ? 2 : 1;
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

		int Find(const key_t& k, pointer_t** pr, size_t depth)
		{
			*pr = nullptr;

			if (!count)
				return 0;

			int low = 0;
			int high = (int)count - 1;

			while (low <= high)
			{
				int middle = (low + high) >> 1;

				switch (keys[middle].Compare(k))
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
				//return (low > bin_c / 2) ? 2 : 1;
			}
			else
				return 0;
		}
	};

	template < typename int_t, typename key_t, typename pointer_t, typename link_t, size_t bin_c, size_t link_c, size_t fuzz_c, size_t padding_c = 0 > struct _FuzzyHashNode : public _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>
	{
		uint8_t padding[padding_c];

		using Pointer = pointer_t;
		using Key = key_t;
		using Int = int_t;
		using Link = link_t;
		static const size_t Fuzz = fuzz_c;
		static const size_t Bins = bin_c;
		static const size_t Links = link_c;
		static const size_t Padding = padding_c;

		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::keys;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::pointers;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::links;
		using _BaseNode<int_t, key_t, pointer_t, link_t, bin_c, link_c>::count;

		void Init()
		{
			for (size_t i = 0; i < bin_c; i++)
				pointers[i] = (pointer_t)-1;
				//keys[i].Zero();
		}

		size_t max_rec() { return sizeof(key_t)/2; }

		int Insert(const key_t& k, const pointer_t& p, pair<pointer_t*, bool>& overwrite, size_t depth)
		{
			int bin = (*(((uint16_t*)&k) + depth) % bin_c);

			//if (keys[bin].IsZero())
			if (pointers[bin] == (pointer_t)-1)
			{
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
				/*if (count != bin_c && -1 == z)
				{
					//if (keys[low].IsZero())
					if (pointers[low] == (pointer_t)-1)
						z = low;
				}*/

				if (pointers[low] == (pointer_t)-1)
				{
					z = low++;
					continue;
				}

				if (keys[low].Equal(k))
				{
					overwrite = { pointers + low,true };
					//pointers[middle] = p; // We allow the call to update the existing object if needed.
					return 0;
				}

				low++;
			}

			if (z != -1)
			{
				keys[z] = k;
				pointers[z] = p;
				count++;

				overwrite = { pointers + z, false };

				return 0;
			}
			else
				return ((low-1) * link_c / bin_c) + 1;
				//return (low > bin_c / 2) ? 2 : 1;
		}

		int Find(const key_t& k, pointer_t** pr, size_t depth)
		{
			*pr = nullptr;

			if (!count)
				return 0;

			int bin = (*(((uint16_t*)&k) + depth) % bin_c);

			if (keys[bin].Equal(k))
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
				/*if (count != bin_c && -1 == z)
				{
					//if (keys[low].IsZero())
					if (pointers[low] == (pointer_t)-1)
						z = low;
				}*/

				if (pointers[low] == (pointer_t)-1)
				{
					z = low++;
					continue;
				}

				if (keys[low].Equal(k))
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
				//return (low > bin_c / 2) ? 2 : 1;
		}
	};

#pragma warning( pop )
#pragma pack(pop)

	//8 + 40 * N(1637) + 16 + 32 + 0
	using OrderedListPointer = _OrderedListNode<uint64_t, Key32, uint64_t, uint64_t, 1637, 4, 0>;
	static_assert(sizeof(OrderedListPointer) == 64 * 1024);

	template <size_t F> using FuzzyHashPointerT = _FuzzyHashNode<uint64_t, Key32, uint64_t, uint64_t, 1637, 4, F, 0>;
	static_assert(sizeof(FuzzyHashPointerT<1>) == 64 * 1024);

	template <size_t F> using BigFuzzyHashPointerT = _FuzzyHashNode<uint64_t, Key32, uint64_t, uint64_t, 6552, 4, F, 8>;
	static_assert(sizeof(BigFuzzyHashPointerT<1>) == 256 * 1024);

	using FuzzyHashPointer = _FuzzyHashNode<uint64_t, Key32, uint64_t, uint64_t, 1637, 4, 4, 0>;
	static_assert(sizeof(FuzzyHashPointer) == 64 * 1024);

	//8 + 64 * N(1023) + 16 + 32 + 8
	using OrderedListKey = _OrderedListNode<uint64_t, Key32, Key32, uint64_t, 1023, 4, 8>;
	static_assert(sizeof(OrderedListKey) == 64 * 1024);

	//8 + 64 * N(1023) + 16 + 32 + 8
	using OrderedListKP = _OrderedListNode<uint64_t, Key32, KeyP, uint64_t, 1023, 4, 8>;
	static_assert(sizeof(OrderedListKP) == 64 * 1024);

	//8 + 64 * N(1023) + 16 + 32 + 8
	using OrderedListKP2 = _OrderedListNode<uint64_t, Key32, KeyP2, uint64_t, 1023, 4, 8>;
	static_assert(sizeof(OrderedListKP2) == 64 * 1024);

	template < typename R, typename node_t > class _BTree
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

		void Validate() {}

		void Open(R* _io, link_t _n)
		{
			root_n = _n;
			io = _io;

			if (io->size() <= root_n)
			{
				auto r = &io->template Allocate<node_t>();
				r->Init();
			}
		}

private: 
		
		template < typename F > int _Iterate(node_t* node, F && f) const
		{
			int count = node->count;

			if (!count)
				return 0;

			for (int i = 0, c = 0; i < (int)node->count; c++)
			{
				if(node->pointers[c] != (pointer_t)-1)
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

public: 
		
		template < typename F > int Iterate(F &&f) const
		{
			if (!Root())
				return 0;
			else
				return _Iterate(Root(),std::move(f));
		}

		pointer_t* Find(const key_t& k) const
		{
			node_t* current = Root();

			if (!current)
				return nullptr;

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				pointer_t* pr;
				int result = current->Find(k, &pr, depth++);

				if (!result)
					return pr;
				else
				{
					result--;

					next = &io->template Lookup<node_t>(current->links[result]);

					if (!next)
						return nullptr;
					else
						current = next;
				}
			}

			return nullptr;
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
				if(depth >= current->max_rec())
					return { nullptr,false };

				pair<pointer_t*, bool> overwrite;
				int result = current->Insert(k, p, overwrite,depth++);

				if (!result)
					return overwrite;
				else
				{
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

		pointer_t* FindLock(const key_t& k) const
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
				int result = current->Find(k, &pr,depth++);

				if (!result)
					return pr;
				else
				{
					result--;

					next = &io->template Lookup<node_t>(current->links[result]);

					if (!next)
						return nullptr;
					else
						current = next;
				}
			}

			return nullptr;
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
				if (depth >= current->max_rec())
					return { nullptr,false };

				bool locked = false;
				if (current->count != node_t::Bins) //Must be locked to insert
				{
					current->Lock(); 
					locked = true;
				}

				pair<pointer_t*, bool> overwrite;
				int result = current->Insert(k, p, overwrite,depth++);

				if (!result)
				{
					if(locked) current->Unlock();
					return overwrite;
				}
				else
				{
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

			return { nullptr,false };
		}

		/*int Delete(const key_t& k)
		{
			return -1;
			BTreeNode * current = NULL;

			if (*_this->root == 0)
				return -1;
			else
				current = (BTreeNode*)Recycler_Lookup(_this->rcyc, (*_this->root) - 1);

			BTreeNode * next = NULL;

			while (current)
			{
				uint32_t overwrite;
				int result = Node_Insert(current, (BTreeKey*)k, (uint32_t)-1, &overwrite);

				if (overwrite != (uint32_t)-1)
					Recycler_Delete(_this->rcyc, overwrite);

				if (!result)
					return 0;
				else
				{
					result--;

					next = (BTreeNode*)Recycler_Lookup(_this->rcyc, current->links[result]);

					if (!next)
						return -1;
					else
						current = next;
				}
			}

			return -1;
		}*/
	};
}