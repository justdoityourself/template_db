/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "btree.hpp"

namespace tdb
{
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

		void Open(R* _io, size_t& _n)
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

				auto& desc = io->GetDescriptor(root_n);

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

		template < typename F > int _Iterate(node_t* node, F&& f) const
		{
			int count = node->count;

			if (!count)
				return 0;

			for (int i = 0, c = 0; i < (int)node->count; c++)
			{
				if (node->pointers[c] != (pointer_t)-1) // this is designed to filter out unused type in fuzzy map. Doesn't work with non int value types.
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
					if (!f(node->keys[c], node->pointers[c]))
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

		template < typename F > int Iterate(F&& f) const
		{
			if (!Root())
				return 0;
			else
				return _Iterate(Root(), std::move(f));
		}

		template < typename F > int IterateKV(F&& f) const
		{
			if (!Root())
				return 0;
			else
				return _IterateKV(Root(), std::move(f));
		}

		pointer_t* Find(const key_t& k, void* ref_page = nullptr) const
		{
			node_t* current = Root();

			if (!current)
				return nullptr;

			node_t* next = nullptr;
			size_t depth = 0;

			while (current)
			{
				pointer_t* pr;
				int result = current->Find(k, &pr, depth++, (void*)io, ref_page);

				if (!result)
					return pr;
				else
				{
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

		template <typename F> void MultiFind(F&& f, const key_t& k, void* ref_page = nullptr) const
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

			auto [low, high] = current->FindRange(f, low_k, high_k, 0, (void*)io, ref_page);

			if (!low && !high)
				return;
			else
			{
				low--; high--;

				for (; low <= high; low++)
				{
					auto next = (current->links[low]) ? &io->template Lookup<node_t>(current->links[low]) : nullptr;
					if (next)
						RangeFind(f, low_k, high_k, ref_page, next);
				}

			}
		}

		void Insert(const gsl::span<key_t>& ks, const pointer_t& p)
		{
			for (auto& k : ks)
				Insert(k, p);
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
				int result = current->Insert(k, p, overwrite, depth++, (void*)io);

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
				int result = current->Find(k, &pr, depth++, (void*)io, ref_page);

				if (!result)
					return pr;
				else
				{
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
				int result = current->Insert(k, p, overwrite, depth++, (void*)io);

				if (!result)
				{
					if (locked) current->Unlock();
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

		template <typename F> pair<pointer_t*, bool> InsertLockContext(const key_t& k, const pointer_t& p, F&& f)
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

			return f(pair<pointer_t*, bool>{ nullptr, false });
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



	template < typename R, typename N > using BTree = _BTree<R, N>;
}