/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "btree.hpp"

namespace tdb
{


	template < typename R, typename node_t > class _STree
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
		_STree() {}

		_STree(R* _io)
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
				if (node->pointers[c] != (pointer_t)-1)
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

					next = &io->template Lookup<node_t>(current->links[result]);

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

					next = &io->template Lookup<node_t>(current->links[result]);

					if (!next)
						return;
					else
						current = next;
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
				if (depth >= current->max_rec())
					return { nullptr,false };

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

					next = &io->template Lookup<node_t>(current->links[result]);

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
				if (depth >= current->max_rec())
					return { nullptr,false };

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
				if (depth >= current->max_rec())
					return f(pair<pointer_t*, bool>{ nullptr, false });

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
}