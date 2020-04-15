/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "../gsl-lite.hpp"

namespace tdb
{

	template < typename R, typename node_t > class _NullIndex
	{
		using key_t = typename node_t::Key;
		using pointer_t = typename node_t::Pointer;

	public:
		_NullIndex() {}

		bool Validate() { return true; }

		void Open(R* _io, size_t& _n) { }

	public:

		template < typename F > int Iterate(F&& f) const
		{
			return 0;
		}

		pointer_t* Find(const key_t& k, void* ref_page = nullptr) const
		{
			return nullptr;
		}

		template <typename F> void MultiFind(F&& f, const key_t& k, void* ref_page = nullptr) const { }

		void Insert(const gsl::span<key_t>& ks, const pointer_t& p) { }

		pair<pointer_t*, bool> Insert(const key_t& k, const pointer_t& p)
		{
			return { nullptr,false };
		}

		pointer_t* FindLock(const key_t& k, void* ref_page = nullptr) const
		{
			return nullptr;
		}

		void InsertLock(const gsl::span<key_t>& ks, const pointer_t& p) { }

		pair<pointer_t*, bool> InsertLock(const key_t& k, const pointer_t& p)
		{
			return { nullptr,false };
		}

		template <typename F> pair<pointer_t*, bool> InsertLockContext(const key_t& k, const pointer_t& p, F&& f)
		{
			return f(pair<pointer_t*, bool>{ nullptr, false });
		}
	};

	template < typename R, typename N > using NullIndex = _NullIndex<R, N>;
}