/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

namespace tdb
{
	using namespace std;

	template < typename R, typename element_t, typename ... index_t > class _Table
	{
		using link_t = element_t::Link;
		using int_t = element_t::Int;
		static const size_t max_pages = element_t::max_pages;
		static const size_t page_elements = element_t::page_elements;

		std::tuple<index_t...> indexes;

		template < typename T > void InstallIndex(T& t, size_t& n)
		{
			t.Open(this, n);
		}

		template < typename T > void InsertIndex(element_t & e, T& t)
		{
			t.Insert(e);
		}

		template < typename T > void InsertLockIndex(element_t& e, T& t)
		{
			t.InsertLock(e);
		}

		void _Open(size_t & n)
		{
			std::apply([&](auto& ...x) {(InstallIndex(x, n++), ...); }, indexes);
		}

		R* io = nullptr;
		link_t root_n;

		struct lookup_t
		{
			link_t pages[max_pages] = {};

			int_t used = 0;
			int_t capacity = 0;
		};

		struct page_t
		{
			element_t elements[page_elements];
		};

		auto Root() const
		{
			return &io->template Lookup<lookup_t>(root_n);
		}

	public:

		_Table() {}

		_Table(R* _io)
		{
			Open(_io);
		}

		void Validate() {} //TODO

		void Open(R* _io, size_t & _n)
		{
			root_n = _n;
			io = _io;

			if (io->size() <= root_n)
				io->template Allocate<lookup_t>();

			_Open(n);
		}

		size_t size() { return (size_t)Root()->used; }

		void resize(size_t count)
		{
			if (count > max_pages * page_elements)
				throw std::runtime_error("Out of bounds");

			auto r = Root();

			if (count < r->total)
				return;

			auto target_page = index / page_elements + 1;

			r->capacity = target_page * page_elements;

			for (size_t i = 0; i < target_page; i++)
			{
				if(!r->pages[i])
					r->pages[i] = io->template Allocate<page_t>();
			}
		}

		element_t& at(size_t index)
		{
			if (index > size())
				throw std::runtime_error("Out of bounds");

			auto page = index / page_elements;
			auto element = index % page_elements;

			return io->template Lookup<lookup_t>(Root()->pages[page]).elements[element];
		}

		element_t& operator[](size_t index)
		{
			return at(index);
		}

		template<size_t DX> auto& Index()
		{
			return std::get<D>(indexes);
		}

		template <typename ... t_args> element_t& Insert(t_args ... args)
		{
			auto r = Root();

			if (r->used >= r->capacity)
				resize(r->used + 1);

			auto page = r->used / page_elements;
			auto element = (r->used)++ % page_elements;

			auto p = &r->pages[page].elements[element];

			new(p) element_t(args...);

			std::apply([&](auto& ...x) {(InsertIndex(*p,x), ...); }, indexes);

			return *p;
		}

		//TODO
		template <typename ... t_args> element_t& InsertLock(t_args ... args)
		{
			//TODO Make this lock free
			auto r = Root();

			if (r->used >= r->capacity)
				resize(r->used + 1);

			auto page = r->used / page_elements;
			auto element = (r->used)++ % page_elements;

			auto p = &r->pages[page].elements[element];

			new(p) element_t(args...);

			std::apply([&](auto& ...x) {(InsertLockIndex(*p, x), ...); }, indexes);

			return *p;
		}
	};
}