/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <array>
#include <type_traits>
#include <cryptopp/sha.h>

namespace tdb
{
	using namespace std;

	template < typename T, typename H > typename std::enable_if<sizeof(T) == 16, void>::type HashT(T & t, const H & h)
	{
		array<uint8_t,32> dest;
		CryptoPP::SHA256 hh;
		hh.CalculateDigest(dest.data(), (CryptoPP::byte*)h.data(), h.size());

		memcpy(&t, &dest, 16);
	}

	template < typename T, typename H > typename std::enable_if<sizeof(T) == 24, void>::type HashT(T & t, const H & h)
	{
		array<uint8_t, 32> dest;
		CryptoPP::SHA256 hh;
		hh.CalculateDigest(dest.data(), (CryptoPP::byte*)h.data(), h.size());

		memcpy(&t, &dest, 24);
	}

	template < typename T, typename H > typename std::enable_if<sizeof(T) == 32, void>::type HashT(T & t, const H & h)
	{
		CryptoPP::SHA256 hh;
		hh.CalculateDigest((uint8_t *)&t, (CryptoPP::byte*)h.data(), h.size());
	}

	template < typename T, typename H > typename std::enable_if<sizeof(T) == 64, void>::type HashT(T & t, const H & h)
	{
		CryptoPP::SHA512 hh;
		hh.CalculateDigest((uint8_t *)&t, (CryptoPP::byte*)h.data(), h.size());
	}
}