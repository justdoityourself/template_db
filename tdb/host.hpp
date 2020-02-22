/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <tuple>
#include <utility>

namespace tdb
{
	using namespace std;

	template < typename R, typename ... tables_t> class _Database : public R
	{
		std::tuple<tables_t...> tables;

		template < typename T > void InstallTable(T& t, int n)
		{
			t.Open(this, n);
		}

		template < typename T > void ValidateTable(T& t)
		{
			t.Validate();
		}
	public:
		_Database() {}

		void Validate()
		{
			std::apply([&](auto& ...x) {(ValidateTable(x), ...); }, tables);
		}

		template <typename ... t_args> _Database(t_args ... args)
		{
			Open(args...);
		}

		template <typename ... t_args> auto Open(t_args ... args)
		{
			R::Open(args...);

			int n = 0;
			std::apply([&](auto& ...x) {(InstallTable(x, n++), ...); }, tables);

			return tables;
		}

		template <size_t I > auto Table()
		{
			return get<I>(tables);
		}

		auto Index()
		{
			return get<0>(tables);
		}
	};
}