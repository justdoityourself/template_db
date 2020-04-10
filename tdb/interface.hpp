#pragma once

namespace tdb
{
	/*
		This module is intended for context relating / merging code.
		Or other code common to modules.

		In the form of:

		1.	Helpers.
		2.	Utilities.
		3.	Joined Operations.
		...
	*/



	template < typename DB, typename _TABLE> class TableHelper
	{
		_TABLE& table;
		DB& database;
	public:

		TableHelper(DB& _database, _TABLE& _table)
			: database(_database)
			, table(_table) {}

		template < size_t value_c, typename F> void StringSearch(std::string_view text,F && f)
		{
			table.Iterate([&](auto & row)
			{
				bool _continue = true;
				if(-1 != std::string_view(row.Value<value_c>()).find(text))
					_continue = f(row);

				return _continue;
			});
		}

		/*
			Operations that take place from the context of the TABLE and DATABASE go here:
		*/
	};



	template < typename _INDEX, typename DB > class IndexHelper : public _INDEX
	{
		DB& database;

	public:
		IndexHelper(DB& _database, const _INDEX& index)
			: database(_database)
			, _INDEX(index) {}

		/*
			Operations that take place from the context of the _INDEX and DATABASE go here:
		*/
	};



	template < typename _INDEX, typename DB > class KeyValueIndex : public _INDEX
	{
		DB& db;

	public:
		KeyValueIndex(DB& _database, const _INDEX& index)
			: db(_database)
			, _INDEX(index) {}

		/*
			This block implements a key / value store on top of any index using Incidental Allocation.

			This code is better here than in every index implementation!

			TODO Move to Key / Value table object.
		*/

		template <typename K, typename V> bool InsertObject(const K& k, const V& v)
		{
			auto [ptr, status] = _INDEX::Insert(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = db.Incidental(v.size());

			if (!iptr)
				return false;

			std::copy(v.begin(), v.end(), iptr);
			*ptr = offset;

			return true;
		}

		template <typename K, typename V> bool InsertObjectLock(const K& k, const V& v)
		{
			auto [ptr, status] = _INDEX::InsertLock(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = db.Incidental(v.size());

			if (!iptr)
				return false;

			std::copy(v.begin(), v.end(), iptr);
			*ptr = offset;

			return true;
		}

		template <typename K, typename V, typename SZ = uint16_t> bool InsertSizedObject(const K& k, const V& v)
		{
			auto [ptr, status] = _INDEX::Insert(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = db.Incidental(v.size() + 2);

			if (!iptr)
				return false;

			auto sz = (SZ*)iptr;
			*sz = (SZ)v.size();
			std::copy(v.begin(), v.end(), iptr + sizeof(SZ));
			*ptr = offset;

			return true;
		}

		template <typename K, typename V, typename SZ = uint16_t> bool InsertSizedObjectLock(const K& k, const V& v)
		{
			auto [ptr, status] = _INDEX::InsertLock(k, uint64_t(0));

			if (status) return false;

			auto [iptr, offset] = db.Incidental(v.size() + 2);

			if (!iptr)
				return false;

			auto sz = (SZ*)iptr;
			*sz = (SZ)v.size();
			std::copy(v.begin(), v.end(), iptr + sizeof(SZ));
			*ptr = offset;

			return true;
		}

		template <typename K> uint8_t* FindObject(const K& k, void* ref = nullptr)
		{
			auto ptr = _INDEX::Find(k, ref);

			if (!ptr)
				return nullptr;

			return db.GetObject(*ptr);
		}

		template <typename K> uint8_t* FindObjectLock(const K& k, void* ref = nullptr)
		{
			auto ptr = _INDEX::FindLock(k, ref);

			if (!ptr)
				return nullptr;

			return db.GetObject(*ptr);
		}

		template <typename K, typename SZ = uint16_t> gsl::span<uint8_t> FindSizedObject(const K& k, void* ref = nullptr)
		{
			auto ptr = _INDEX::Find(k, ref);

			if (!ptr)
				return nullptr;

			auto obj = db.GetObject(*ptr);

			if (!obj)
				return gsl::span<uint8_t>();

			auto sz = (SZ*)obj;

			return gsl::span<uint8_t>(obj + sizeof(SZ), (size_t)*sz);
		}

		template <typename K, typename SZ = uint16_t> gsl::span<uint8_t> FindSizedObjectLock(const K& k, void* ref = nullptr)
		{
			auto ptr = _INDEX::FindLock(k, ref);

			if (!ptr)
				return gsl::span<uint8_t>();

			auto obj = db.GetObject(*ptr);

			if (!obj)
				return gsl::span<uint8_t>();

			auto sz = (SZ*)obj;

			return gsl::span<uint8_t>(obj + sizeof(SZ), (size_t)*sz);
		}
	};

}