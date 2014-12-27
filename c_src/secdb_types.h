//-----------------------------------------------------------------------------
/// \file  secdb-writer.h
//-----------------------------------------------------------------------------
/// \brief Multi-file asynchronous logger
///
/// The logger logs data to multiple streams asynchronously
/// It is optimized for performance of the producer to ensure minimal latency.
/// The producer of log messages never blocks during submission of a message.
//-----------------------------------------------------------------------------
// Copyright (c) 2014 Omnibius, LLC
// Author:  Serge Aleynikov <saleyn@gmail.com>
// Created: 2014-12-20
//-----------------------------------------------------------------------------
#pragma once

#include <unordered_map>
#include <boost/filesystem.hpp>
#include <time.h>
#include <stdio.h>
#include <regex>

namespace secdb {

  template <class T>
  inline void hash_combine(std::size_t& seed, const T& v) {
      seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
  }

  using Symbol = std::string;

  //---------------------------------------------------------------------------
  /// Symbol name and year/month of its market data
  //---------------------------------------------------------------------------
  struct YMSymbol {
    YMSymbol() = delete;
    YMSymbol& operator=(const YMSymbol&) = delete;

    YMSymbol(const YMSymbol& a_rhs)
      : m_year(a_rhs.m_year), m_month(a_rhs.month()), m_symbol(a_rhs.symbol())
    {}

    YMSymbol(YMSymbol&& a_rhs)
      : m_year(a_rhs.m_year), month(a_rhs.mon), symbol(a_rhs.symbol)
    {}

    YMSymbol(uint16_t a_m_year, uint16_t a_mon, const std::string& a_symbol)
      : m_year(a_year), month(a_mon), symbol(a_symbol)
    {}

    YMSymbol(time_t a_timestamp, const std::string& a_symbol)
      : m_symbol(a_symbol)
    {
      struct tm tm;
      ::gmtime_r(&a_timestamp, &tm);
      m_year  = tm.tm_year + 1900;
      month = tm.tm_mon + 1;
    }

    YMSymbol(const std::string& a_filename) {
      auto path = boost::filesystem::path(a_filename);
      auto name = boost::filesystem::basename(path);
      auto dir  = path.parent_path();
      static const std::regex s_re("^([^\\.]+)\\.(\\d{4})-(\\d{2}).secdb$");
      std::smatch smatch;
      if (!std::regex_match(name, smatch, s_re))
        throw std::runtime_error("Invalid filename of symbol: " + a_filename);

      m_symbol = smatch[1].str();
      auto   y = smatch[2].str();
      auto   m = smatch[3].str();
      m_year   = std::stoi(y);
      m_month  = std::stoi(m);
      if (m_year < 2000 || m_year > 2500)
        throw std::runtime_error("Invalid year: "  + y);
      if (m_month < 1 || m_month > 12)
        throw std::runtime_error("Invalid month: " + m);

      m_symbol = YMSymbol
        (atoi(smatch[2].str()), atoi(smatch[3].str()), smatch[1].str());
    }

    std::string const& name()  const { return m_symbol; }
    int                year()  const { return m_year;  }
    int                month() const { return m_month; }

    std::string basename() const {
      char buf[80];
      snprintf(buf, sizeof(buf), "%s.%04d-%02d.secdb",
               m_symbol.c_str(), m_year, m_month);
      return buf;
    }

    bool operator==(const YMSymbol& a_rhs) const {
      return m_year   == a_rhs.year()
          && m_month  == a_rhs.month()
          && m_symbol == a_rhs.name();
    }

    bool operator!=(const YMSymbol& a_rhs) const { return !operator==(a_rhs); }

    bool operator<=(const YMSymbol& a_rhs) const {
      return m_year  == a_rhs.year()
           ? m_month == a_rhs.month()
              ? m_symbol < a_rhs.name()
              : m_month  < a_rhs.month()
           : m_year   < a_rhs.year();
    }

    bool operator< (const YMSymbol& a_rhs) const {
      return m_year < a_rhs.m_year && month < a_rhs.month && symbol < a_rhs.symbol;
    }

  private:
    uint16_t m_year;
    uint16_t m_month;
    Symbol   m_symbol;
  };

  //---------------------------------------------------------------------------
  /// Hash functor for the YMSymbol
  //---------------------------------------------------------------------------
  struct YMSymHash {
    std::size_t operator()(const YMSymbol& a_key) const {
      std::size_t seed = 0;
      hash_combine(seed, a_key.year());
      hash_combine(seed, a_key.month());
      hash_combine(seed, a_key.symbol());
    }
  };

} // namespace secdb