/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <tuple>
#include <utility>

#include "keys.hpp"

namespace tdb
{
	using namespace std;

#pragma warning( push )
#pragma warning( disable : 4200 )

	template <typename T, int C> struct _OrderedArray
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

	template < typename int_t, typename key_t, typename pointer_t, typename link_t, int bin_c, int link_c, int padding_c = 0 > struct _OrderedListNode
	{
		static const uint64_t _guard = 0xfe3451dceabc45afull;
		using Pointer = pointer_t;
		using Key = key_t;
		using Int = int_t;
		using Link = link_t;
		static const int Bins = bin_c;
		static const int Links = link_c;
		static const int Padding = padding_c;

		int_t header_guard = (int_t)_guard;

		key_t keys[bin_c];

		pointer_t pointers[bin_c];

		link_t links[link_c] = { 0 };

		int_t count = 0;

		int_t footer_guard = (int_t)_guard;

		uint8_t padding[padding_c] = { 0x9a };

		void Expand(int c)
		{
			for (int i = (int)count - 1; i >= c; i--)
				memcpy(keys + (i + 1), keys + i, sizeof(key_t));

			for (int i = (int)count - 1; i >= c; i--)
				memcpy(pointers + (i + 1), pointers + i, sizeof(pointer_t));

			count++;
		}

		int Insert(const key_t& k, const pointer_t& p, pair<pointer_t*, bool>& overwrite)
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
					//pointers[middle] = p;
					return 0;
				}
			}

			if (count == bin_c)
			{
				if (low == bin_c)
					low--;
				else if (low == -1)
					low++;

				return (low > bin_c / 2) ? 2 : 1;
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

		int Find(const key_t& k, pointer_t** pr)
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

				return (low > bin_c / 2) ? 2 : 1;
			}
			else
				return 0;
		}
	};

#pragma warning( pop )

	//8 + 40 * N(1637) + 16 + 32 + 0
	using OrderedListPointer = _OrderedListNode<uint64_t, Key32, uint64_t, uint64_t, 1637, 4, 0>;
	static_assert(sizeof(OrderedListPointer) == 64 * 1024);

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
		static const int link_c = node_t::Links;

		R* io = nullptr;
		node_t* root = nullptr;
	public:
		_BTree() {}

		_BTree(R* _io)
		{
			Open(_io);
		}

		void Validate() {}

		void Open(R* _io, int n)
		{
			io = _io;

			if (io->size() <= n)
				root = &io->template Allocate<node_t>();
			else
				root = &io->template Lookup<node_t>(n);
		}

	private: template < typename F > int _Iterate(node_t* node, F f)
	{
		int count = node->count;

		if (!count)
			return 0;

		for (int i = 0; i < (int)node->count; i++)
			if (f(node->pointers[i]))
				return i;

		for (int i = 0; i < link_c; i++)
			if (node->links[i])
				count += _Iterate(&io->template Lookup<node_t>((uint64_t)node->links[i]), f);

		return count;
	}

	public: template < typename F > int Iterate(F f)
	{
		if (!root)
			return 0;
		else
			return _Iterate(f);
	}


		  pointer_t* Find(const key_t& k)
		  {
			  node_t* current = root;

			  if (!root)
				  return nullptr;

			  node_t* next = nullptr;

			  while (current)
			  {
				  pointer_t* pr;
				  int result = current->Find(k, &pr);

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
			  node_t* current = root;

			  if (!current)
				  return { nullptr,false };

			  node_t* next = nullptr;

			  while (current)
			  {
				  pair<pointer_t*, bool> overwrite;
				  int result = current->Insert(k, p, overwrite);

				  if (!result)
					  return overwrite;
				  else
				  {
					  result--;

					  next = (current->links[result]) ? &io->template Lookup<node_t>(current->links[result]) : nullptr;

					  if (!next)
					  {
						  next = &io->template Allocate<node_t>();
						  current->links[result] = io->template Index<node_t>(*next);
					  }

					  current = next;
				  }
			  }

			  return { nullptr,false };
		  }

		  int Delete(const key_t& k)
		  {
			  return -1;
			  /*BTreeNode * current = NULL;

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

			  return -1;*/
		  }
	};
}