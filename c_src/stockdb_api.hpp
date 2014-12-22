#include <endian.h>
#include <tuple>

#include "stockdb_api.h"

namespace stockdb {

  //----------------------------------------------------------------------------
  inline BitReader::tuple<int32_t>
  BitReader::bits_read32() {
    auto offset = m_offset + 32;
    if (offset > long(m_size) || (m_offset & 7) != 0) return tuple<int32_t>();
    auto n = *(uint32_t*)(m_bytes + (m_offset >> 3));

    m_offset = offset;
    return tuple<int32_t>(be32toh(n));
  }

  //----------------------------------------------------------------------------
  inline const char* BitReader::
  decode_delta(bool append, bool has_value, int& old, const char* reason) {
    int64_t inc;
    if (!has_value)
      inc = 0;
    else {
      auto dlt = leb128_decode_signed();
      if (!dlt.ok) return reason;
      inc = dlt.value;
    }

    if (append)
      old += inc;
    else
      old  = inc;

    return nullptr;
  }

  //----------------------------------------------------------------------------
  inline std::pair<BitReader::rec_type, const char*>
  BitReader::read_rec_type()
  {
    if (!m_size) return std::make_pair(UNDEFINED, "empty");

    auto full_md = bit_get();
    if (!full_md.ok) return std::make_pair(UNDEFINED, "full_md");

    if (!full_md.value)
      return std::make_pair(MKT_DATA_DLT, nullptr);

    auto is_trade = bit_get();
    if (!is_trade.ok) return std::make_pair(UNDEFINED, "is_trade");

    return std::make_pair(is_trade.value ? TRADE : MKT_DATA, nullptr);
  }

  //----------------------------------------------------------------------------
  inline std::tuple<BitReader::rec_type, size_t, const char*>
  BitReader::read_next(bool a_append_delta) {
    static const rec_type s_undefined = UNDEFINED;
    static const size_t   s_zero      = 0ul;

    auto res = read_rec_type();
    if (res.first == UNDEFINED)
      return std::tie(s_undefined, s_zero, res.second);

    std::pair<int, const char*> ret;

    switch (res.first) {
      case TRADE:
        ret = read_trade();
        break;
      case MKT_DATA:
      case MKT_DATA_DLT:
        ret = read_mkt_data(a_append_delta);
        break;
      default:
        break;
    }

    return (ret.first <= 0)
      ? std::tie(s_undefined,  s_zero, ret.second)
      : std::tie(res.first, ret.first, ret.second);
  }

  //----------------------------------------------------------------------------
  // DBState
  //----------------------------------------------------------------------------
  template <class T>
  inline const T* DBState::cast(long a_offset, const char* a_where) const {
    auto p = m_mem + a_offset;
    if (p >= m_mem_end)
      throw std::runtime_error
        ("Attempt to read invalid offset " + to_string(a_offset) +
        " in the file " + m_filename + " of size " + to_string(file_size()));
    return reinterpret_cast<const T*>(p);
  }

} // namespace stockdb
