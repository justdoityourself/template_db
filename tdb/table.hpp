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
				InsertIndex<I + 1>(ks,dx,v);
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

			auto start_page = r->capacity / page_elements;
			auto target_page = count / page_elements + 1;

			r->capacity = target_page * page_elements;

			for (size_t i = start_page; i < target_page; i++)
				r->pages[i] = io->template Index<page_t>(io->template Allocate<page_t>());
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

			InsertIndex<>(p->Keys(io->GetReference((uint8_t*)p)),indexes, r->used++);

			return *p;
		}

		template < size_t I, typename K > element_t * Find(const K & k, void* ref = nullptr)
		{
			auto dx = std::get<I>(indexes).Find(k,ref);

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



	template < typename R, typename element_t, typename ... index_t > using FixedTable = _Table<R, element_t, index_t...>;

	template < typename int_t, typename link_t, size_t _max_pages, size_t _lookup_padding, size_t _page_elements, size_t _page_padding, typename child_t > struct TableElementBase : public child_t
	{
		static const size_t max_pages = _max_pages;
		static const size_t page_elements = _page_elements;
		static const size_t lookup_padding = _lookup_padding;
		static const size_t page_padding = _page_padding;

		using Int = int_t;
		using Link = link_t;
	};

	template < size_t page_s, typename int_t, typename link_t, typename child_t >
	using TableElementBuilder = TableElementBase<   int_t,
		link_t,
		(page_s - sizeof(int_t) * 2) / sizeof(link_t),
		(page_s - sizeof(int_t) * 2) % sizeof(link_t),
		page_s / sizeof(child_t),
		page_s % sizeof(child_t),
		child_t >;

	template < typename child_t, size_t page_s = 64 * 1024, typename int_t = uint64_t> using SimpleTableElementBuilder = TableElementBuilder<page_s, int_t, int_t, child_t>;






	template < typename R, typename element_t, typename ... index_t > class _GrowingTable
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

		template<size_t I = 0, typename... Tkey, typename... Tidx> void InsertIndex(const std::tuple<Tkey...>& ks, std::tuple<Tidx...>& dx, link_t v)
		{
			std::get<I>(dx).Insert(std::get<I>(ks), v);

			if constexpr (I + 1 != sizeof...(Tidx))
				InsertIndex<I + 1>(ks, dx, v);
		}

		template<size_t I = 0, typename... Tkey, typename... Tidx> void InsertIndexLock(const std::tuple<Tkey...>& ks, std::tuple<Tidx...>& dx, link_t v)
		{
			std::get<I>(dx).InsertLock(std::get<I>(ks), v);

			if constexpr (I + 1 != sizeof...(Tidx))
				InsertIndexLock<I + 1>(ks, dx);
		}

		void _Open(size_t& n)
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

			link_t next = 0;

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

		_GrowingTable() {}

		_GrowingTable(R* _io)
		{
			Open(_io);
		}

		void Validate() {} //TODO

		void Open(R* _io, size_t& _n)
		{
			root_n = _n++;
			io = _io;

			if (io->size() <= root_n)
				io->template Allocate<lookup_t>();

			_Open(_n);
		}

		size_t size() { return (size_t)Root()->used; }

		link_t& get_page(size_t page)
		{
			auto root = page / max_pages;
			auto index = page % max_pages;

			auto r = Root();

			for (size_t i = 1; i < root; i++)
			{
				if (!r->next)
					r->next = io->template Index<lookup_t>(io->template Allocate<lookup_t>());

				r = &io->template Lookup<lookup_t>(r->next);
			}

			return r->pages[index];
		}

		void resize(size_t count)
		{
			auto r = Root();

			if (count < r->capacity)
				return;

			auto start_page = r->capacity / page_elements;
			auto target_page = count / page_elements + 1;

			r->capacity = target_page * page_elements;

			for (size_t i = start_page; i < target_page; i++)
				get_page(i) = io->template Index<page_t>(io->template Allocate<page_t>());
		}

		element_t& At(size_t index)
		{
			if (index > size())
				throw std::runtime_error("Out of bounds");

			auto root = index / page_elements * max_pages;
			auto page = index / page_elements;
			auto element = index % page_elements;

			return io->template Lookup<page_t>(get_page(page)).elements[element];
		}

		element_t* pAt(size_t index)
		{
			if (index > size())
				return nullptr;

			auto page = index / page_elements;
			auto element = index % page_elements;

			return &io->template Lookup<page_t>(get_page(page)).elements[element];
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

			auto p = &(io->template Lookup<page_t>(get_page(page)).elements[element]);

			new(p) element_t(args...);

			InsertIndex<>(p->Keys(io->GetReference((uint8_t*)p)), indexes, r->used++);

			return *p;
		}

		template < size_t I, typename K > element_t* Find(const K& k, void* ref = nullptr)
		{
			auto dx = std::get<I>(indexes).Find(k, ref);

			if (!dx)
				return nullptr;

			return pAt(*dx);
		}
	};
}