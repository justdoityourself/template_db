/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

namespace tdb
{
	enum TableType : uint32_t
	{
		undefined_table_type,
		btree_sorted_list,
		btree_sorted_multilist,
		btree_fuzzymap,

	};

	enum KeyMode : uint8_t
	{
		undefined_key_mode,
		key_mode_direct,
		key_mode_surrogate,
		key_mode_local_surrogate,
	};
}