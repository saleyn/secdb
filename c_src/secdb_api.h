// vim:ts=2:sw=2:et
//----------------------------------------------------------------------------
/// \file   secdb_api.h
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
#pragma once

#include <erl_nif.h>
#include <memory>
#include <cstdint>
#include <utility>
#include <vector>
#include <deque>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <time.h>
#include <error.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include "secdb_types.h"

namespace secdb {
  using namespace std;

  using Symbol = std::string;

  //----------------------------------------------------------------------------
  /// Quote class containing qty and price data
  //----------------------------------------------------------------------------
  class Quote {
    int   m_qty;
    Price m_price;
  public:
    Quote()                             : m_qty(0)    , m_price(0)    {}
    Quote(int a_qty, Price const& a_px) : m_qty(a_qty), m_price(a_px) {}
    Quote(int a_qty, int a_px, unsigned a_precision)
      : m_qty(a_qty), m_price(a_px, a_precision)
    {}
    Quote(const Quote&)            = default;
    Quote(Quote&&)                 = default;
    Quote& operator=(Quote&&)      = default;
    Quote& operator=(const Quote&) = default;

    int           qty()        const { return m_qty;   }
    Price const&  price()      const { return m_price; }
    double        px()         const { return m_price; }
    unsigned      precision()  const { return m_price.precision(); }
    void          precision(unsigned a_prec) { m_price.precision(a_prec); }

    int&          qty()              { return m_qty;   }
    Price&        price()            { return m_price; }

    void qty  (int a_qty)            { m_qty = a_qty;  }
    void price(int a_px, unsigned a_prec)    { m_price.set(a_px, a_prec); }

    std::string   to_string()  const;

    void set(int a_qty, int a_px)    { m_qty = a_qty; m_price.set(a_px);  }
    void set(int a_qty, int a_px, unsigned a_precision) {
      m_qty = a_qty; m_price.set(a_px, a_precision);
    }
  };

  //----------------------------------------------------------------------------
  // Trade detail
  //----------------------------------------------------------------------------
  struct Trade {
    Trade() {}
    Trade(const Trade&)            = default;
    Trade(Trade&&)                 = default;
    Trade& operator=(Trade&&)      = default;
    Trade& operator=(const Trade&) = default;

    enum Aggressor { UNDEFINED, PASSIVE, AGGRESSIVE };
    enum Side      { BUY, SELL };

    Trade(long a_time, int a_qty, float a_px, Aggressor a_aggr = UNDEFINED)
      : m_timestamp(a_time), m_qty(a_qty), m_price(a_px), m_aggressor(a_aggr)
    {}

    long         timestamp()  const { return m_timestamp; }
    int          qty()        const { return m_qty;       }
    Price const& price()      const { return m_price;     }
    double       px()         const { return m_price;     }
    unsigned     precision()  const { return m_price.precision(); }
    Aggressor    aggressor()  const { return m_aggressor; }
    Side         side()       const { return m_side;      }

    char      aggr_to_char()  const { return m_aggressor == AGGRESSIVE ? 'Y'
                                           : m_aggressor == PASSIVE ? 'N':' '; }

    void set(long a_time, int a_qty, Price const& a_px, Side a_sd, Aggressor a_agg) {
      m_timestamp = a_time;
      m_qty       = a_qty;
      m_price     = a_px;
      m_side      = a_sd;
      m_aggressor = a_agg;
    }

  private:
    long      m_timestamp;
    int       m_qty;
    Price     m_price;
    Aggressor m_aggressor;
    Side      m_side;
  };

  //----------------------------------------------------------------------------
  // Market data snapshot
  //----------------------------------------------------------------------------
  template <class QuoteT>
  class BasicMDSnapshot {
    long                 m_timestamp;
    std::vector<QuoteT>  m_bids;  // bid levels
    std::vector<QuoteT>  m_asks;  // ask levels
  public:
    using iterator        = typename std::vector<QuoteT>::iterator;
    using const_iterator  = typename std::vector<QuoteT>::const_iterator;

    BasicMDSnapshot() : m_timestamp(0) {}

    BasicMDSnapshot(size_t a_max_depth)
      : m_timestamp(0)
      , m_bids(a_max_depth), m_asks(a_max_depth)
    {}
    BasicMDSnapshot(const BasicMDSnapshot& a_rhs)
      : m_timestamp(a_rhs.timestamp())
      , m_bids(a_rhs.m_bids), m_asks(a_rhs.m_asks)
    {}

    long timestamp()       const { return m_timestamp;       }
    long timestamp(long ts)      { return m_timestamp = ts;  }

    std::vector<QuoteT> const& bids() const { return m_bids; }
    std::vector<QuoteT> const& asks() const { return m_asks; }

    std::vector<QuoteT>&       bids()       { return m_bids; }
    std::vector<QuoteT>&       asks()       { return m_asks; }

    std::pair<QuoteT const*, QuoteT const*>
    operator[] (unsigned a_lev) const {
      auto bid = a_lev < m_bids.size() ? &m_bids[a_lev] : nullptr;
      auto ask = a_lev < m_asks.size() ? &m_asks[a_lev] : nullptr;
      return std::make_pair(bid, ask);
    }

    bool is_same_prices
      (const BasicMDSnapshot& a_md, int a_depth, bool a_price_only);

    void operator= (const BasicMDSnapshot& a_rhs) {
      m_timestamp = a_rhs.timestamp();
      m_bids      = a_rhs.m_bids;
      m_asks      = a_rhs.m_asks;
    }

    std::ostream& operator<< (std::ostream& out) const;

    friend inline std::ostream&
    operator<< (std::ostream& out, const BasicMDSnapshot<QuoteT>& a) {
      return a.operator<<(out);
    }
  };

  using MDSnapshot = BasicMDSnapshot<Quote>;

  // Type of record
  enum class RecordT {
    UNDEFINED =  -1,
    MDS_DLT   = 0x0, // MD Delta Snapshot
    MDS       = 0x1, // MD Full Snapshot
    TRADE_DLT = 0x2, // Trade Delta
    TRADE     = 0x3
  };

  //----------------------------------------------------------------------------
  /// Candle aggregation information
  //----------------------------------------------------------------------------
  class Candle {
    bool     m_found;
    uint32_t m_open;
    uint32_t m_high;
    uint32_t m_low;
    uint32_t m_close;
    uint64_t m_buy_volume;
    uint64_t m_sell_volume;
  public:
    Candle()
    : m_open(0), m_high(0), m_low(0), m_close(0)
    , m_buy_volume(0),      m_sell_volume(0)
    {}

    Candle(const char* a_bytes) { read(a_bytes); }

    Candle(Candle&&)      = default;
    Candle(Candle const&) = default;

    static constexpr size_t size() { return 4 * sizeof(uint32_t); }

    // Read candle from stream
    const char* read(const char* a_mem) {
      const uint32_t* p = (const uint32_t*)a_mem;
      auto tmp          = be32toh(*p++);
      m_found           = tmp & 0x80000000;
      m_open            = tmp & 0x7FFFffff;
      m_high            = be32toh(*p++);
      m_low             = be32toh(*p++);
      m_close           = be32toh(*p++);
      m_buy_volume      = be32toh(*p++);
      m_sell_volume     = be32toh(*p++);
      return (const char*)p;
    }

    uint32_t open ()       const { return m_open;        }
    uint32_t high ()       const { return m_high;        }
    uint32_t low  ()       const { return m_low;         }
    uint32_t close()       const { return m_close;       }
    uint64_t buy_volume()  const { return m_buy_volume;  }
    uint64_t sell_volume() const { return m_sell_volume; }

    bool     found() const { return m_found; }

    std::ostream& dump
      (std::ostream& out, const char* title, uint8_t precision, uint32_t scale)
    const;
  };

  //----------------------------------------------------------------------------
  /// Database I/O file management
  //----------------------------------------------------------------------------
  class DBState {
    static const int OFFSETLEN     = sizeof(uint32_t);
    static const int CANDLE_SIZE   = 6*sizeof(uint32_t);
    static const int IDX_ITEM_SIZE = OFFSETLEN + CANDLE_SIZE;

    template <typename T>
    struct tuple {
      const bool ok;
      const T    value;

      tuple()            : ok(false), value(0)     {}
      tuple(T value)     : ok(true),  value(value) {}

      tuple(tuple const&        a_rhs) = default;
      tuple(tuple&&             a_rhs) = default;
      tuple& operator= (tuple&& a_rhs) = default;
    };

  public:
    //--------------------------------------------------------------------------
    /// Timestamp index chunk
    //--------------------------------------------------------------------------
    class Chunk {
      uint32_t m_number;
      long     m_timestamp;
      size_t   m_offset;
      size_t   m_size;
      Candle   m_candle;

      static long tsmask(uint64_t a_val) { return long(a_val & ~(1ull << 63)); }
    public:
      Chunk(Chunk const&) = default;
      Chunk(Chunk&&)      = default;

      Chunk() : m_number(0), m_timestamp(0), m_offset(0) {}

      Chunk(uint32_t a_num, long a_ts_usec, uint64_t a_pos, const Candle& a_cnd)
        : m_number(a_num)
        , m_timestamp(tsmask(a_ts_usec)) // clear two upper bits of tstamp
        , m_offset(a_pos)
      {}

      static bool valid(uint64_t a_timestamp) {
        //     date -d "2012-01-01 00:00:00" +%s
        //                                    date -d "2200-01-01 00:00:00" +%s
        return a_timestamp > 1325394000000 && a_timestamp < 7258136400000;
      }

      uint32_t      number   () const { return m_number;    }
      long          timestamp() const { return m_timestamp; }
      size_t        offset   () const { return m_offset;    }
      size_t        size     () const { return m_size;      }
      const Candle& candle   () const { return m_candle;    }

      void size(size_t a_size)        { m_size = a_size;    }
    };


    //--------------------------------------------------------------------------
    /// Reader is separated in a local class
    //--------------------------------------------------------------------------
    /// Such separation helps to call it from either C++ or Erlang
    /// implementation when decoding market data.
    class Reader {
      const uint8_t* m_begin;
      const uint8_t* m_rd_ptr;
      const uint8_t* m_end;
      long&          m_last_ts;
      Quote&         m_last_quote;
      Trade&         m_trade;
      MDSnapshot&    m_md;
      RecordT        m_rec_type;
      int            m_verbose;

      uint8_t  bits_peek(unsigned a_skip, unsigned a_get) const {
        assert(a_skip + a_get <= 8);
        uint8_t n = *m_rd_ptr;
        uint8_t v = (n >> (7-a_skip)) & ((1 << a_get) - 1);
        return  v;
      }

      uint8_t  read8()   { return *m_rd_ptr++; }
      uint32_t read32();
      uint64_t read64();

      int64_t  decode_signed_leb128();
      uint64_t decode_unsigned_leb128();

      std::pair<long, const char*>  read_trade();
      std::pair<long, const char*>  read_mkt_data();

      bool valid() const { return m_rd_ptr <= m_end; }

    public:
      Reader(const uint8_t* a_data,    size_t a_offset, const uint8_t* a_end,
             long&       a_last_ts, Quote& a_last_quote,
             Trade&      a_trade,   MDSnapshot& a_md, int a_verbose = 0)
        : m_begin     (a_data)
        , m_rd_ptr    (m_begin + a_offset)
        , m_end       (a_end)
        , m_last_ts   (a_last_ts)
        , m_last_quote(a_last_quote)
        , m_trade     (a_trade)
        , m_md        (a_md)
        , m_rec_type  (RecordT::UNDEFINED)
        , m_verbose   (a_verbose)
      {}

      Reader(const  uint8_t*   a_begin,
             long&  a_last_ts, Quote&      a_last_quote,
             Trade& a_trade,   MDSnapshot& a_md, int a_verbose = 0)
      : Reader
        (a_begin, 0, nullptr, a_last_ts, a_last_quote, a_trade, a_md, a_verbose)
      {}

      void set(const uint8_t* a_data, const uint8_t* a_end) {
        assert(a_data >  m_begin);
        assert(a_data <= a_end);
        m_rd_ptr = a_data;
        m_end    = a_end;
      }

      static int64_t  decode_signed_leb128  (const uint8_t*& p);
      static uint64_t decode_unsigned_leb128(const uint8_t*& p);

      //------------------------------------------------------------------------
      /// Read next record
      //------------------------------------------------------------------------
      std::tuple<RecordT, size_t, const char*> read_next();
      std::pair <RecordT, const char*>         read_rec_type();
    };

    DBState();
    DBState(const YMSymbol& a_sym, int a_verbose);
    ~DBState();

    int          file           () const { return m_file           ;      }
    int          version        () const { return m_version        ;      }
    std::string  symbol         () const { return m_symbol         ;      }
    time_t       date           () const { return m_date           ;      }
    int          year           () const { return m_year           ;      }
    int          month          () const { return m_month          ;      }
    int          depth          () const { return m_depth          ;      }
    int          scale          () const { return m_scale          ;      }
    int          precision      () const { return m_precision      ;      }
    int          interval       () const { return m_interval       ;      }
    int          num_intervals  () const { return m_num_intervals  ;      }
    long         daycandle_pos  () const { return m_daycandle_pos  ;      }
    long         chunkmap_pos   () const { return m_chunkmap_pos   ;      }
    long         data_pos       () const { return m_data_offset    ;      }

    int          chunkmap_size  () const { return m_num_intervals*IDX_ITEM_SIZE;}

    std::deque<Chunk>& chunkmap ()       { return m_chunkmap;             }
    const Candle&      candle   () const { return m_candle;               }

    void date           (time_t a_date)  { m_date             = a_date;   }
    void interval       (int  a_int)     { m_interval         = a_int;    }
    void daycandle_pos  (long a_offset)  { m_daycandle_pos    = a_offset; }
    void chunkmap_pos   (long a_offset)  { m_chunkmap_pos  = a_offset; }

    long file_size()               const { return m_mem_end - m_mem;      }

    bool validate_chunk(const Chunk& a_chunk, long& a_bad_offset) {
      // TODO: do chunk validation
      return true;
    }

    enum class PrintFlags : int {
      None              = 0,
      OutputSymbolName  = 1 << 0, // Print symbol name
      EpochTimeFormat   = 1 << 1, // Print time in num. of msec since epoch
      UniquePrice       = 1 << 2, // Don't print MDs without price changes
      UniquePriceAndQty = 1 << 3, // Don't print MDs without price/qty changes
      DisplayCandles    = 1 << 4, // Print candles
      DisplayFileInfo   = 1 << 5, // Display file info rather than market data
    };

    // Print the content of the file to stream
    void print
      (std::ostream& out, int a_depth=INT_MAX,
       int flags = int(PrintFlags::OutputSymbolName));

  private:
    std::string       m_filename;
    int               m_verbose;
    int               m_file;
    int               m_version;
    std::string       m_symbol;
    time_t            m_date;
    time_t            m_year;
    time_t            m_month;
    int               m_depth;
    int               m_scale;
    int               m_precision;
    int               m_interval;
    int               m_num_intervals;
    long              m_daycandle_pos;
    long              m_chunkmap_pos;
    long              m_data_offset;
    char*             m_mem;
    const char*       m_mem_end;
    const char*       m_rd_ptr;

    std::deque<Chunk> m_chunkmap;
    Candle            m_candle;

    long              m_last_sec;
    char              m_last_date[12];
    char              m_last_time[9];

    long              m_last_interval_time;
    long              m_next_interval_time;

    long              m_last_timestamp;
    Quote             m_last_quote;
    Trade             m_last_trade;
    MDSnapshot        m_last_md;

    long total_size()             const { return m_mem_end - m_mem;     }
    long size()                   const { return m_mem_end - m_rd_ptr;  }
    const char* data()            const { return m_mem + m_data_offset; }
    long offset()                 const { return m_rd_ptr  - m_mem;     }

    template <typename T>
    long offset(const T* a_mem)   const { const char* p = (const char*)a_mem;
                                          assert(p > m_mem); return  p-m_mem;   }
    void offset(long a_pos)             { m_rd_ptr = m_mem + a_pos; }

    void add_offset(long a_offset)      { m_rd_ptr += a_offset; }

    bool valid_offset(long a_pos) const { return m_mem    + a_pos <  m_mem_end; }
    bool check_space (long a_inc) const { return m_rd_ptr + a_inc <= m_mem_end; }

    const Candle& read_candle();
    void          read_chunk_map();

    void update_last_time(long a_timestamp_ms);

    //--------------------------------------------------------------------------
    template <class T>
    const T* cast(long a_offset, const char* a_where) const;

    //--------------------------------------------------------------------------
    /// Remap the file in memory
    //--------------------------------------------------------------------------
    void remap(size_t a_size);
  };
} // namespace secdb