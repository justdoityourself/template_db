/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

namespace tdb
{
	using namespace std;

	template < typename R, typename element_t, typename ... index_t > class _Table
	{
		using link_t = typename element_t::Link;
		using int_t = typename element_t::Int;
		static const size_t max_pages = element_t::max_pages;
		static const size_t page_elements = element_t::page_elements;
		static const size_t lookup_padding = element_t::lookup_padding;
		static const size_t page_padding = element_t::page_padding;

		std::tuple<index_t...> indexes;

		template < typename T > void InstallIndex(T& t, size_t& n)
		{
			t.Open(io, n);
		}

		template<size_t I = 0, typename... Tkey,typename... Tidx> void InsertIndex(const std::tuple<Tkey...>& ks, std::tuple<Tidx...> & dx, link_t v)
		{
			std::get<I>(dx).Insert(std::get<I>(ks), v);

			if constexpr (I + 1 != sizeof...(Tidx))
				InsertIndex<I + 1>(ks,dx);
		}

		template<size_t I = 0, typename... Tkey, typename... Tidx> void InsertIndexLock(const std::tuple<Tkey...>& ks, std::tuple<Tidx...>& dx, link_t v)
		{
			std::get<I>(dx).InsertLock(std::get<I>(ks), v);

			if constexpr (I + 1 != sizeof...(Tidx))
				InsertIndexLock<I + 1>(ks, dx);
		}

		void _Open(size_t & n)
		{
			std::apply([&](auto& ...x) { (InstallIndex(x, n), ...); }, indexes);
		}

		R* io = nullptr;
		link_t root_n;

#pragma pack(push,1)

		struct lookup_t
		{
			lookup_t() {}

			link_t pages[max_pages] = {};

			int_t used = 0;
			int_t capacity = 0;

			uint8_t padding[lookup_padding];
		};

		static_assert(sizeof(lookup_t) == R::UnitSize);

		struct page_t
		{
			page_t() {}

			element_t elements[page_elements];

			uint8_t padding[page_padding];
		};

		static_assert(sizeof(page_t) == R::UnitSize);

#pragma pack(pop)

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
			root_n = _n++;
			io = _io;

			if (io->size() <= root_n)
				io->template Allocate<lookup_t>();

			_Open(_n);
		}

		size_t size() { return (size_t)Root()->used; }

		void resize(size_t count)
		{
			if (count > max_pages * page_elements)
				throw std::runtime_error("Out of bounds");

			auto r = Root();

			if (count < r->capacity)
				return;

			auto target_page = count / page_elements + 1;

			r->capacity = target_page * page_elements;

			for (size_t i = 0; i < target_page; i++)
			{
				if(!r->pages[i])
					r->pages[i] = io->template Index<page_t>(io->template Allocate<page_t>());
			}
		}

		element_t& At(size_t index)
		{
			if (index > size())
				throw std::runtime_error("Out of bounds");

			auto page = index / page_elements;
			auto element = index % page_elements;

			return io->template Lookup<page_t>(Root()->pages[page]).elements[element];
		}

		element_t* pAt(size_t index)
		{
			if (index > size())
				return nullptr;

			auto page = index / page_elements;
			auto element = index % page_elements;

			return &io->template Lookup<page_t>(Root()->pages[page]).elements[element];
		}

		element_t& operator[](size_t index)
		{
			return At(index);
		}

		template<size_t DX> auto& Index()
		{
			return std::get<DX>(indexes);
		}

		template <typename ... t_args> element_t& Emplace(t_args ... args)
		{
			auto r = Root();

			if (r->used >= r->capacity)
				resize(r->used + 1);

			auto page = r->used / page_elements;
			auto element = r->used % page_elements;

			auto p = &(io->template Lookup<page_t>(r->pages[page]).elements[element]);

			new(p) element_t(args...);

			InsertIndex<>(p->Keys(),indexes, r->used++);

			return *p;
		}

		template < size_t I, typename K > element_t * Find(const K & k)
		{
			auto dx = std::get<I>(indexes).Find(k);

			if (!dx)
				return nullptr;

			return pAt(*dx);
		}

		//TODO
		/*template <typename ... t_args> element_t& EmplaceLock(t_args ... args)
		{
			//TODO Make this lock free
			auto r = Root();

			if (r->used >= r->capacity)
				resize(r->used + 1);

			auto page = r->used / page_elements;
			auto element = (r->used)++ % page_elements;

			auto p = &r->pages[page].elements[element];

			new(p) element_t(args...);

			//std::apply([&](auto& ...x) {(InsertLockIndex(*p, x), ...); }, indexes);

			return *p;
		}*/
	};
}