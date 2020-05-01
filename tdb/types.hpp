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

		table_fixed,
		table_dynamic,
		table_surrogate,
	};

	enum KeyMode : uint8_t
	{
		undefined_key_mode,
		key_mode_direct,
		key_mode_surrogate,
		key_mode_local_surrogate,
	};

	enum KeyType : uint8_t
	{
		undefined_key_type,
		key_type_distributed,
		key_type_sequential,
		key_type_mixed,
	};

	enum PointerMode : uint8_t
	{
		undefined_pointer_mode,
		pointer_mode_payload,
		pointer_mode_incidental,
		pointer_mode_bucket,
		pointer_mode_table_row,
	};
}