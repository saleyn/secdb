// vim:ts=2:sw=2:et
//----------------------------------------------------------------------------
/// \file   secdb_api.hpp
/// \author Serge Aleynikov
//----------------------------------------------------------------------------
/// \brief API for working with secdb timeseries database storage format.
//----------------------------------------------------------------------------
// Copyright (c) 2014 Serge Aleynikov <saleyn@gmail.com>
// Created: 2015-01-03
//----------------------------------------------------------------------------
/*
***** BEGIN LICENSE BLOCK *****

This file is part of the secdb open-source project.

Copyright (C) 2014 Serge Aleynikov <saleyn@gmail.com>

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

***** END LICENSE BLOCK *****
*/
#include <endian.h>
#include <tuple>

#include "secdb_api.h"

namespace secdb {

  inline std::string Quote::to_string() const {
    std::stringstream s;
    s << m_qty << '@' << std::fixed
      << std::setprecision(m_price.precision()) << m_price.to_double();
    return s.str();
  }

  template <class QuoteT>
  inline bool BasicMDSnapshot<QuoteT>::
  is_same_prices(const BasicMDSnapshot& a_md, int a_depth, bool a_price_only) {
    auto it1 = m_bids.cbegin(),      end1 = m_bids.cend();
    auto it2 = a_md.bids().cbegin(), end2 = a_md.bids().cend();

    auto nb1 = std::min<int>(a_depth, m_bids.size());
    auto na1 = std::min<int>(a_depth, m_asks.size());
    auto nb2 = std::min<int>(a_depth, a_md.bids().size());
    auto na2 = std::min<int>(a_depth, a_md.asks().size());

    if ((nb1 != nb2) || (na1 != na2))
      return false;

    for (int i = 0; i < nb1; ++i, ++it1, ++it2)
      if (!((it1->price() == it2->price()) &&
            (a_price_only || it1->qty() == it2->qty())))
        return false;

    it1 = m_asks.cbegin(),      end1 = m_asks.cend();
    it2 = a_md.asks().cbegin(), end2 = a_md.asks().cend();

    for (int i = 0; i < na1; ++i, ++it1, ++it2)
      if (!((it1->price() == it2->price()) &&
            (a_price_only || it1->qty() == it2->qty())))
        return false;

    return true;
  }

  template <class QuoteT>
  std::ostream& BasicMDSnapshot<QuoteT>::operator<<(std::ostream& out) const {
    out << timestamp();

    auto it  = bids().cbegin();
    auto end = bids().cend();
    for (int i = 0; it != end; ++it, ++i) {
      out.precision(it->precision());
      out << ' ' << it->qty() << '@' << std::fixed << it->px();
    }

    out << " |";

    it  = asks().cbegin();
    end = asks().cend();
    for (int i = 0; it != end; ++it, ++i) {
      out.precision(it->precision());
      out << ' ' << it->qty() << '@' << std::fixed << it->px();
    }

    return out << '\n';
  }

  inline std::ostream& Candle::dump
    (std::ostream& out, const char* title, uint8_t precision, uint32_t scale)
  const
  {
    char buf[256];
    snprintf(buf, sizeof(buf),
             "%s{O=%.*f, H=%.*f, L=%.*f, C=%.*f, VB=%lu, VS=%lu}\n",
             title,
             precision, double(m_open)  / scale,
             precision, double(m_high)  / scale,
             precision, double(m_low)   / scale,
             precision, double(m_close) / scale,
             m_buy_volume, m_sell_volume);
    return out << buf;
  }

  //----------------------------------------------------------------------------
  /// Utility function to decode a ULEB128 value.
  //----------------------------------------------------------------------------
  inline uint64_t DBState::Reader::
  decode_unsigned_leb128(const uint8_t*& a_ptr) {
    auto p = a_ptr;
    uint64_t value = 0;
    unsigned shift = 0;
    do {
      value += uint64_t(*p & 0x7f) << shift;
      shift += 7;
    } while (*p++ & 0x80);

    a_ptr = p;
    return value;
  }

  inline uint64_t DBState::Reader::decode_unsigned_leb128() {
    return decode_unsigned_leb128(m_rd_ptr);
  }

  //----------------------------------------------------------------------------
  /// Utility function to decode a SLEB128 value.
  //----------------------------------------------------------------------------
  inline int64_t DBState::Reader::
  decode_signed_leb128(const uint8_t*& a_ptr) {
    auto p     = a_ptr;
    int64_t  value = 0;
    unsigned shift = 0;
    uint8_t  byte;
    do {
      byte = *p++;
      value |= ((byte & 0x7f) << shift);
      shift += 7;
    } while (byte & 0x80);
    // Sign extend negative numbers.
    if (byte & 0x40)
      value |= (-1ULL) << shift;

    a_ptr = p;
    return value;
  }

  inline int64_t DBState::Reader::decode_signed_leb128() {
    return decode_signed_leb128(m_rd_ptr);
  }

  //----------------------------------------------------------------------------
  inline uint32_t DBState::Reader::read32() {
    auto res  = be32toh(*(uint32_t*)m_rd_ptr);
    m_rd_ptr += 4;
    return res;
  }

  //----------------------------------------------------------------------------
  inline uint64_t DBState::Reader::read64() {
    auto res  = be64toh(*(uint64_t*)m_rd_ptr);
    m_rd_ptr += 8;
    return res;
  }

  //----------------------------------------------------------------------------
  inline std::pair<RecordT, const char*>
  DBState::Reader::read_rec_type()
  {
    if (m_rd_ptr+1 >= m_end)
      return std::make_pair(RecordT::UNDEFINED, "not_enough_data");

    RecordT type = RecordT(*(uint8_t*)m_rd_ptr >> 6);

    return std::make_pair(type, nullptr);
  }

  //----------------------------------------------------------------------------
  inline std::tuple<RecordT, size_t, const char*>
  DBState::Reader::Reader::read_next() {
    static const RecordT s_undefined = RecordT::UNDEFINED;
    static const size_t  s_zero      = 0ul;

    const char* err;
    std::tie(m_rec_type, err) = read_rec_type();
    if (m_rec_type == RecordT::UNDEFINED)
      return std::tie(m_rec_type, s_zero, err);

    RecordT rt = m_rec_type;
    std::pair<size_t, const char*> ret;

    switch (m_rec_type) {
      case RecordT::MDS:
      case RecordT::MDS_DLT:
        ret = read_mkt_data();
        break;
      case RecordT::TRADE:
      case RecordT::TRADE_DLT:
        ret = read_trade();
        break;
      default:
        break;
    }

    return (ret.first <= 0)
      ? std::tie(s_undefined, s_zero,    ret.second)
      : std::tie(rt,          ret.first, ret.second);
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

} // namespace secdb
