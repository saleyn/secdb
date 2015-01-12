// vim:ts=2:sw=2:et
//----------------------------------------------------------------------------
/// \file   secdb_types.h
/// \author Serge Aleynikov
//----------------------------------------------------------------------------
/// \brief  Types used by secdb API
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
#pragma once

#include <unordered_map>
#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <regex>

namespace secdb {

  template <class T>
  inline void hash_combine(std::size_t& seed, const T& v) {
      seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
  }

  using Symbol = std::string;

#if defined(_WIN32) || defined(__windows__) || defined(_WIN64)
  inline constexpr char path_sep() { return '\\'; }
#else
  inline constexpr char path_sep() { return '/'; }
#endif

  inline std::pair<std::string, std::string> split(const std::string& a_path) {
    auto n    = a_path.find_last_of(path_sep());
    auto name = n == std::string::npos ? a_path : a_path.substr(n + 1);
    auto dir  = n == std::string::npos ? ""     : a_path.substr(0,  n);
    return std::make_pair(dir, name);
  }

  //---------------------------------------------------------------------------
  /// Symbol name and year/month of its market data
  //---------------------------------------------------------------------------
  struct YMSymbol {
    YMSymbol() = delete;
    YMSymbol& operator=(const YMSymbol&) = delete;

    YMSymbol(const YMSymbol& a_rhs)
      : m_year(a_rhs.m_year), m_month(a_rhs.month()), m_symbol(a_rhs.m_symbol)
      , m_filename(a_rhs.m_filename)
    {}

    YMSymbol(YMSymbol&& a_rhs)
      : m_year(a_rhs.m_year), m_month(a_rhs.m_month), m_symbol(a_rhs.m_symbol)
      , m_filename(a_rhs.m_filename)
    {}

    YMSymbol(uint16_t a_year, uint16_t a_mon, const std::string& a_symbol,
             const std::string& a_dir = "")
      : m_year(a_year), m_month(a_mon), m_symbol(a_symbol)
      , m_filename(a_dir)
    {
      if (!m_filename.empty() && m_filename.back() != path_sep())
        m_filename += path_sep();

      m_filename.append(basename());
    }

    YMSymbol(time_t a_timestamp, const std::string& a_symbol,
             const std::string& a_dir = "")
      : m_symbol  (a_symbol)
      , m_filename(a_dir)
    {
      struct tm tm;
      ::gmtime_r(&a_timestamp, &tm);
      m_year  = tm.tm_year + 1900;
      m_month = tm.tm_mon + 1;

      if (!m_filename.empty() && m_filename.back() != path_sep())
        m_filename += path_sep();

      m_filename.append(basename());
    }

    YMSymbol(const std::string& a_filename)
      : m_filename(a_filename)
    {
      std::string dir, name;
      std::tie   (dir, name) = split(a_filename);

      static const std::regex s_re("^(.+)\\.(\\d{4})-(\\d{2})-(\\d{2}).secdb$");
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
    }

    std::string const& name()     const { return m_symbol; }
    int                year()     const { return m_year;  }
    int                month()    const { return m_month; }
    std::string const& filename() const { return m_filename; }
    std::string basename()        const {
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
      return m_year   < a_rhs.m_year && m_month < a_rhs.m_month
          && m_symbol < a_rhs.m_symbol;
    }

  private:
    uint16_t    m_year;
    uint16_t    m_month;
    Symbol      m_symbol;
    std::string m_filename;
  };

  //---------------------------------------------------------------------------
  /// Hash functor for the YMSymbol
  //---------------------------------------------------------------------------
  struct YMSymHash {
    std::size_t operator()(const YMSymbol& a_key) const {
      std::size_t seed = 0;
      hash_combine(seed, a_key.year());
      hash_combine(seed, a_key.month());
      hash_combine(seed, a_key.name());
      return seed;
    }
  };

  //---------------------------------------------------------------------------
  /// Integer price representation with given precision
  //---------------------------------------------------------------------------
  struct Price {
    static const unsigned s_def_precision = 5;
    static const unsigned s_max_precision = 7;

    explicit Price(double a_px, unsigned a_precision = s_def_precision) {
      set(a_px * scale(a_precision) + 0.5, a_precision);
    }

    explicit Price(int      a_px, unsigned a_precision = s_def_precision) {
      set(a_px, a_precision);
    }
    explicit Price(unsigned a_px, unsigned a_precision = s_def_precision) {
      set(a_px, a_precision);
    }
    explicit Price(long     a_px, unsigned a_precision = s_def_precision) {
      set(a_px, a_precision);
    }

    Price() : m_value(0), m_precision(s_def_precision) {}
    Price(const Price&)            = default;
    Price(Price&&)                 = default;
    Price& operator=(const Price&) = default;
    Price& operator=(Price&&)      = default;

    int      value()              const { return m_value;            }
    int&     value()                    { return m_value;            }

    void     precision(unsigned a_prec) { m_precision = a_prec;      }
    unsigned precision()          const { return m_precision;        }
    unsigned scale()              const { return scale(m_precision); }

    double   to_double()          const { return scaled(m_value);    }
    operator double()             const { return scaled(m_value);    }

    Price& operator+=(const Price& a_rhs) {
      auto p = std::move(normalize(a_rhs)); m_value += p.m_value; return *this;
    }
    Price& operator-=(const Price& a_rhs) {
      auto p = std::move(normalize(a_rhs)); m_value -= p.m_value; return *this;
    }

    Price  operator+ (const Price& a) const { auto p = *this; return p += a; }
    Price  operator- (const Price& a) const { auto p = *this; return p -= a; }

    double operator* (long a)         const { return scaled(m_value*a); }
    double operator/ (long a)         const { return scaled(m_value/a); }

    void set(long a_price)                  { m_value = a_price; }

    void set(long a_price, unsigned a_precision) {
      assert(a_precision <= s_max_precision);
      m_value     = a_price;
      m_precision = a_precision;
    }

  private:
    int      m_value;
    unsigned m_precision;

    static unsigned scale(unsigned a_precision) {
      const unsigned s_precisions[]  = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000
      };
      return s_precisions[a_precision];
    }

    /// Normalize current price so that it can be used for arithmetic ops with
    /// the returned price.
    /// @return argument \a a normalized for direct arithmetic ops with this
    ///         instance.
    Price normalize(Price const& a) {
      if (m_precision == a.m_precision) return a;
      if (m_precision <  a.m_precision) {
        set(m_value * scale(a.m_precision - m_precision), a.m_precision);
        return a;
      } else {
        return Price(a.m_value * scale(m_precision - a.m_precision), m_precision);
      }
    }

    double scaled(long a_val) const { return double(a_val) / scale(m_precision); }
  };

  //----------------------------------------------------------------------------
  // Exception helpers
  //----------------------------------------------------------------------------
  namespace detail {
    class streamed_exception : public std::exception {
    protected:
      std::shared_ptr<std::stringstream> m_out;
      mutable std::string m_str;

      streamed_exception& output() { return *this; }

      template <class T, class... Args>
      streamed_exception& output(const T& h, const Args&... t) {
          *m_out << h;
          return output(t...);
      }

    public:
      streamed_exception() : m_out(new std::stringstream()) {}
      virtual ~streamed_exception() throw() {}

      template <class T>
      streamed_exception& operator<< (const T& a) { *m_out << a; return *this; }

      virtual const char* what()  const throw() { m_str = str(); return m_str.c_str(); }
      virtual std::string str()   const { return m_out->str();  }
    };
  }

  //----------------------------------------------------------------------------
  // \brief Exception class with ability to stream arguments to it.
  //   <tt>throw runtime_error("Test", 1, " result:", 2);</tt>
  //   <tt>throw runtime_error() << "Test" << 1 << " result:" << 2;</tt>
  // so we choose to reallocate the m_out string instead.
  //----------------------------------------------------------------------------
  class runtime_error : public detail::streamed_exception {
  public:
    runtime_error() {}
    runtime_error(const std::string& a_str) { *this << a_str; }

    template <class T, class... Args>
    runtime_error(const T& a_first, Args&&... a_rest) {
      *this << a_first;
      output(std::forward<Args>(a_rest)...);
    }

    /// This streaming operator allows throwing exceptions in the form:
    /// <tt>throw runtime_error("Test") << ' ' << 10 << " failed";
    template <class T>
    runtime_error& operator<< (T&& a) {
      *static_cast<detail::streamed_exception*>(this) << a; return *this;
    }
  };

  //----------------------------------------------------------------------------
  /// Invalid fime format exception
  //----------------------------------------------------------------------------
  using invalid_file_format = runtime_error;

} // namespace secdb