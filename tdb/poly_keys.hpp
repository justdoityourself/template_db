#pragma once

#include "keys.hpp"

#include "hash/polynomial.hpp"

namespace tdb
{
	namespace polynomial
	{
#pragma pack(push,1)
		class F1B4C1_Key //Feed 1 byte, Block 4 bytes, complexity 1^2 round
		{
		public:
			
			static const uint8_t mode = KeyMode::key_mode_direct;
			static const uint8_t type = KeyType::key_type_distributed;

			F1B4C1_Key() {}

			template < typename T > F1B4C1_Key(const T& t)
			{
				first = template_hash::polynomial::general_hash<uint8_t>(t, std::array<uint32_t, 1>{1999934317});
			}

			void Zero()
			{
				first = 0;
			}

			bool IsZero()
			{
				return first == 0;
			}

			bool Equal(const F1B4C1_Key& k, void* ref_page = nullptr, void* ref_page2 = nullptr)
			{
				return k.first == first;
			}

			int Compare(const F1B4C1_Key& p, void* ref_page = nullptr, void* ref_page2 = nullptr)
			{
				if (first == p.first)
					return 0;

				if (first > p.first)
					return 1;

				return -1;
			}

			bool operator==(const F1B4C1_Key& p)
			{
				return Compare(p) == 0;
			}

			bool operator>(const F1B4C1_Key& p)
			{
				return Compare(p) == 1;
			}

			/*uint8_t& operator[] (size_t dx)
			{
				return *(data() + dx);
			}

			const uint8_t& operator[] (size_t dx) const
			{
				return *(data() + dx);
			}*/

			const uint8_t* data() const { return (uint8_t*)this; }
			size_t size() const { return sizeof(*this); }

			const uint8_t* begin() const { return data(); }
			const uint8_t* end() const { return data() + size(); }

			uint32_t first;
		};
#pragma pack(pop)
	}
}