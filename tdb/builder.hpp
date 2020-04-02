/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include "recycling.hpp"
#include "mapping.hpp"
#include "host.hpp"

namespace tdb
{
	/*
		On Disk, Read / Write, Memory Mapped Databases
	*/

	template <size_t GROW = 1024 * 1024, size_t PAGE = 64 * 1024> using SyncMap = _Recycling< _MapFile<GROW>, PAGE >; //Single Map, growth remaps entire address space therefore cannot be used with multiple threads.
	template <size_t GROW = 1024 * 1024, size_t PAGE = 64 * 1024> using AsyncMap = _Recycling< _MapList<GROW>, PAGE >; //List of maps, address space will always remain valid even when object grows, can be used with multiple threads.



	/*
		In Memory, Read Only Databases
	*/

	template <size_t GROW = 1024 * 1024, size_t PAGE = 64 * 1024> using SyncMemoryView = _Recycling< _ReadMemoryFile<GROW>, PAGE >; //Single buffer database object.
	template <size_t GROW = 1024 * 1024, size_t PAGE = 64 * 1024> using AsyncMemoryView = _Recycling< _ReadMemoryList<GROW>, PAGE >; //Multi buffer database object.



	/*
		Build database:	
	*/

	template <typename RECYCLER, typename ... TABLELIST> using DatabaseBuilder = _Database<RECYCLER, TABLELIST...>;

}