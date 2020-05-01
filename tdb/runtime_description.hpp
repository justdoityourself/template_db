/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string>

#include "types.hpp"

namespace tdb
{
	template < typename D > std::string PrintDescriptor(const D& desc)
	{
		std::string result;

		auto about_index = [&]()
		{
			/*
			uint8_t key_type;

			uint8_t pointer_mode;*/

			result += "Balance Algorithm: " + ((desc.standard_index.self_balanced) ? "True" : "False") + "\r\n";
			result += "Distributed Key: " + ((desc.standard_index.requires_distributed_key) ? "True" : "False") + "\r\n";

			result += "Key Size: " + std::to_string(desc.standard_index.key_sz) + "\r\n";
			result += "Pointer Size: " + std::to_string(desc.standard_index.pointer_sz) + "\r\n";
			result += "Link Size: " + std::to_string(desc.standard_index.link_sz) + "\r\n";
			result += "Link Count: " + std::to_string(desc.standard_index.link_count) + "\r\n";

			result += "Max Capacity: " + std::to_string(desc.standard_index.max_capacity) + "\r\n";
			result += "Min Capacity: " + std::to_string(desc.standard_index.min_capacity) + "\r\n";
			result += "Max Page: " + std::to_string(desc.standard_index.max_page) + "\r\n";
			result += "Min Page: " + std::to_string(desc.standard_index.min_page) + "\r\n";

			switch (desc.standard_index.key_mode)
			{
			default:
			case undefined_key_mode:
				result += "Key Mode: Unknown\r\n";
				break;
			case key_mode_direct:
				result += "Key Mode: Literal\r\n";
				break;
			case key_mode_surrogate:
				result += "Key Mode: Surrogate\r\n";
				break;
			case key_mode_local_surrogate:
				result += "Key Mode: Local\r\n";
				break;
			};

			switch (desc.standard_index.key_type)
			{
			default:
			case undefined_key_type:
				result += "Key Type: Unknown\r\n";
				break;
			case key_type_distributed:
				result += "Key Type: Distributed\r\n";
				break;
			case key_type_sequential:
				result += "Key Type: Sequential\r\n";
				break;
			case key_type_mixed:
				result += "Key Type: Mixed\r\n";
				break;
			};

			switch (desc.standard_index.key_type)
			{
			default:
			case undefined_pointer_mode:
				result += "Pointer Mode: Unknown\r\n";
				break;
			case pointer_mode_payload:
				result += "Pointer Mode: Payload\r\n";
				break;
			case pointer_mode_incidental:
				result += "Pointer Mode: Incidental\r\n";
				break;
			case pointer_mode_bucket:
				result += "Pointer Mode: Bucket\r\n";
				break;
			case pointer_mode_table_row:
				result += "Pointer Mode: Table Row\r\n";
				break;
			};
		};

		switch (desc.type)
		{
		default:
		case undefined_table_type:
			result += "Type: Unknown\r\n";
			about_index();
			break;
		case btree_sorted_list:
			result += "Type: BTREE Sorted List\r\n";
			about_index();
			break;
		case btree_sorted_multilist:
			result += "Type: BTREE Sorted Multilist\r\n";
			about_index();
			break;
		case btree_fuzzymap:
			result += "Type: BTREE Hashmap\r\n";
			about_index();
			break;
		case table_fixed:
			result += "Type: Fixed TABLE\r\n";
			break;
		case table_dynamic:
			result += "Type: Dynamic TABLE\r\n";
			break;
		case table_surrogate:
			result += "Type: Surrogate TABLE\r\n";
			break;
		}

		return result;
	}

	template < typename T > std::string DescribeDatabase(const T& db)
	{
		std::string result;

		for (size_t i = 0; i < db.TableCount(); i++)
			result += PrintDescriptor(db.GetDescriptor(i));

		return result;
	}
}
