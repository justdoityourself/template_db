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

		template < typename T > void InstallTable(T& t, size_t &n)
		{
			t.Open(this, n);
		}

		template < typename T > void ValidateTable(T& t)
		{
			t.Validate();
		}
	public:
		_Database() {}

		using R::Flush;
		using R::Stale;
		using R::Incidental;
		using R::Close;
		using R::GetObject;

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

			size_t n = 0;
			std::apply([&](auto& ...x) {(InstallTable(x, n), ...); n++; }, tables);

			return tables;
		}

		template <size_t I > auto Table() const
		{
			return get<I>(tables);
		}

		auto ReadWriteIndex()
		{
			return get<0>(tables);
		}

		const auto & ReadOnlyIndex() const
		{
			return get<0>(tables);
		}
	};
}