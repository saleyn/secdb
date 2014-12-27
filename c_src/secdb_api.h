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
#include <time.h>
#include <error.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

namespace secdb {
  using namespace std;

  using Symbol = std::string;

  //----------------------------------------------------------------------------
  /// Quote class containing qty and price data
  //----------------------------------------------------------------------------
  class Quote {
    int m_qty;
    int m_price;
  public:
    Quote()                        : m_qty(0)    , m_price(0)    {}
    Quote(int a_qty, int a_px)     : m_qty(a_qty), m_price(a_px) {}
    Quote(const Quote&)            = default;
    Quote(Quote&&)                 = default;
    Quote& operator=(Quote&&)      = default;
    Quote& operator=(const Quote&) = default;

    int qty()       const { return m_qty;   }
    int price()     const { return m_price; }

    int& qty()            { return m_qty;   }
    int& price()          { return m_price; }

    void qty  (int a_qty) { m_qty = a_qty;  }
    void price(int a_px)  { m_price = a_px; }

    void set(int a_qty, int a_px) { m_qty = a_qty; m_price = a_px; }
  };

  //----------------------------------------------------------------------------
  /// Extended Quote class with indicators of whether price and qty are defined
  //----------------------------------------------------------------------------
  class QuoteEx : public Quote {
    bool m_has_qty;
    bool m_has_px;

    void init() { m_has_qty = m_has_px = false; }
  public:
    QuoteEx()                                               { init(); }
    QuoteEx(int a_qty, int a_px)       : Quote(a_qty, a_px) { init(); }
    QuoteEx(const Quote&  a_rhs)       : Quote(a_rhs)       { init(); }
    QuoteEx(Quote&&  a_rhs)            : Quote(a_rhs)       { init(); }
    QuoteEx(const QuoteEx&)            = default;
    QuoteEx(QuoteEx&&)                 = default;
    QuoteEx& operator=(QuoteEx&&)      = default;
    QuoteEx& operator=(const QuoteEx&) = default;

    bool has_qty()    const { return m_has_qty; }
    bool has_px()     const { return m_has_px;  }

    bool& has_qty()         { return m_has_qty; }
    bool& has_px()          { return m_has_px;  }

    void has(bool a_has_qty, bool a_has_px) {
      m_has_qty = a_has_qty;
      m_has_px  = a_has_px;
    }
  };

  //----------------------------------------------------------------------------
  /// Price level of an order book
  //----------------------------------------------------------------------------
  template <class QuoteT>
  class BasicPriceLevel {
    QuoteT m_bid;
    QuoteT m_ask;
  public:
    BasicPriceLevel() {}
    BasicPriceLevel(const QuoteT& a_bid, const Quote& a_ask)
      : m_bid(a_bid),  m_ask(a_ask) {}
    BasicPriceLevel(QuoteT&& a_bid, QuoteT&& a_ask)
      : m_bid(std::move(a_bid)), m_ask(std::move(a_ask)) {}
    BasicPriceLevel(const BasicPriceLevel&)            = default;
    BasicPriceLevel(BasicPriceLevel&&)                 = default;
    BasicPriceLevel& operator=(BasicPriceLevel&&)      = default;
    BasicPriceLevel& operator=(const BasicPriceLevel&) = default;

    QuoteT const& bid() const { return m_bid; }
    QuoteT const& ask() const { return m_ask; }
    QuoteT&       bid()       { return m_bid; }
    QuoteT&       ask()       { return m_ask; }
  };

  using PriceLevel   = BasicPriceLevel<Quote>;
  using PriceLevelEx = BasicPriceLevel<QuoteEx>;

  //----------------------------------------------------------------------------
  // Trade detail
  //----------------------------------------------------------------------------
  struct Trade {
    Trade() {}
    Trade(const Trade&)            = default;
    Trade(Trade&&)                 = default;
    Trade& operator=(Trade&&)      = default;
    Trade& operator=(const Trade&) = default;

    enum Aggressor { UNDEFINED = 0, PASSIVE = 1, AGGRESSIVE = 2 };

    Trade(long a_time, int a_qty, float a_px, Aggressor a_aggr = UNDEFINED)
      : m_timestamp(a_time), m_qty(a_qty), m_price(a_px), m_aggressor(a_aggr)
    {}

    long      timestamp()  const { return m_timestamp; }
    long      qty()        const { return m_qty;       }
    long      price()      const { return m_price;     }
    Aggressor aggressor()  const { return m_aggressor; }

    void set(long a_time, int a_qty, float a_px, Aggressor a_aggr) {
      m_timestamp = a_time;
      m_qty       = a_qty;
      m_price     = a_px;
      m_aggressor = a_aggr;
    }

  private:
    long      m_timestamp;
    int       m_qty;
    float     m_price;
    Aggressor m_aggressor;
  };

  //----------------------------------------------------------------------------
  // Market data snapshot
  //----------------------------------------------------------------------------
  template <class LevelT>
  class BasicMDSnapshot {
    long                 m_timestamp;
    std::vector<LevelT>  m_levels;  // bid levels
  public:
    using iterator       = typename std::vector<LevelT>::iterator;
    using const_iterator = typename std::vector<LevelT>::const_iterator;

    BasicMktData(size_t a_depth) : m_timestamp(0), m_levels(a_depth) {}
    BasicMktData(const BasicMktData& a_rhs)
      : m_timestamp(a_rhs.timestamp())
      , m_levels(a_rhs.m_levels)
    {}

    long timestamp()       const { return m_timestamp;       }
    void timestamp(long ts)      { m_timestamp = ts;         }

    LevelT&       operator[] (unsigned a_lev)       { return m_levels[a_lev]; }
    LevelT const& operator[] (unsigned a_lev) const { return m_levels[a_lev]; }

    size_t depth()         const { return m_levels.size();   }
    void depth(size_t a_depth)   { m_levels.resize(a_depth); }

    iterator        begin()      { return m_levels.begin();  }
    iterator        end()        { return m_levels.end();    }

    const_iterator  cbegin()     { return m_levels.cbegin(); }
    const_iterator  cend()       { return m_levels.cend();   }
  };

  using MDSnapshot   = BasicMDSnapshot<PriceLevel>;
  using MDSnapshotEx = BasicMDSnapshot<PriceLevelEx>;

  // Type of record
  enum class RecordT {
    UNDEFINED,
    MDS,            // MD Full Snapshot
    MDS_DLT,        // MD Delta Snapshot
    TRADE
  };


  //----------------------------------------------------------------------------
  /// BitReader class for reading secdb data
  //----------------------------------------------------------------------------
  class BitReader {
    template <typename T>
    struct tuple {
      const bool ok;
      const T    value;

      tuple()            : ok(false), value(0)     {}
      tuple(T value)     : ok(true),  value(value) {}

      tuple(tuple const&      a_rhs) = default;
      tuple(tuple&&           a_rhs) = default;
      tuple& operator= (tuple&& a_rhs) = default;
    };

  public:
    using Level = BasicPriceLevel<QuoteEx>;

    BitReader(size_t a_depth)
      : m_bytes(nullptr), m_size(0), m_offset(0)
      , m_rec_type(RecordT::UNDEFINED)
      , m_mkt_data(a_depth)
    {}

    void set(unsigned char* a_bytes, size_t a_size) {
      m_bytes  = a_bytes;
      m_size   = a_size << 3;
      m_offset = 0;
    }

    int scale() const { return m_scale; }
    int depth() const { return m_mkt_data.depth(); }

    void scale(unsigned a_scale) { m_scale = a_scale; }
    void depth(unsigned a_depth) {
      m_full_md_rec_size = 8*(8 + 2*2*a_depth*4); // in bits
      m_mkt_data.depth(a_depth);
    }

    //--------------------------------------------------------------------------
    /// Read next record
    //--------------------------------------------------------------------------
    std::tuple<RecordT, size_t, const char*> read_next(bool a_append_delta);

  private:
    unsigned char*       m_bytes;
    size_t               m_size;
    long                 m_offset;

    int                  m_scale;

    RecordT             m_rec_type;
    Trade                m_trade;
    BasicMktData<Level>  m_mkt_data;

    int                  m_full_md_rec_size;

    long     bits_remain() const { return m_size - m_offset; }
    bool     bits_skip(unsigned a_size) {
      if (m_offset + a_size > long(m_size)) return false;
      m_offset += a_size;
      return true;
    }

    unsigned char* bits_head() { return &m_bytes[m_offset >> 3]; }

    tuple<bool> bit_look() {
      return (m_offset == long(m_size))
        ? tuple<bool>()
        : tuple<bool>((m_bytes[m_offset >> 3] >> (7 - (m_offset & 7))) & 1);
    }

    tuple<bool> bit_get() {
      auto res = std::move(bit_look());
      if (res.ok) m_offset++;
      return res;
    }

    tuple<int32_t>  bits_read32();
    tuple<uint64_t> bits_get(int a_bits);

    void bits_align() {
      if (m_offset & 7)
        m_offset = ((m_offset >> 3)+1) << 3;
      // if(br->offset > br->size*8) {
      //   br->offset = br->size*8;
      // }
    }

    unsigned        bits_byte_offset() const { return m_offset >> 3; }

    tuple<uint64_t> leb128_decode_unsigned();
    tuple<int64_t>  leb128_decode_signed();

    const char*     decode_delta
      (bool append, bool has_value, int& old, const char* reason);

    std::pair<RecordT, const char*>  read_RecordT();
    std::pair<int, const char*>      read_trade();

    /// @param a_append if true and current record type is market data delta,
    ///                 the delta values will be added to mkt_data() record
    std::pair<int, const char*>      read_mkt_data(bool a_append = false);
  };

  //----------------------------------------------------------------------------
  /// Invalid fime format exception
  //----------------------------------------------------------------------------
  class invalid_file_format : public std::exception {
    std::string m_what;
  public:
    invalid_file_format() noexcept {}
    invalid_file_format(std::string const& a_what) : m_what(a_what) {}
    virtual ~invalid_file_format   ()  noexcept {}
    virtual const char*   what() const noexcept { return m_what.c_str(); }
  };

  //----------------------------------------------------------------------------
  /// Timestamp index chunk
  //----------------------------------------------------------------------------
  class Chunk {
    uint32_t m_number;
    long     m_timestamp;
    uint32_t m_offset;

    static long tsmask(uint64_t a_val) { return long(a_val & ~(1ull << 63)); }
  public:
    Chunk(Chunk const&) = default;
    Chunk(Chunk&&)      = default;

    Chunk() : m_number(0), m_timestamp(0), m_offset(0) {}

    Chunk(uint32_t a_num, long a_time_usec, uint32_t a_offset)
      : m_number(a_num)
      , m_timestamp(tsmask(a_time_usec)) // clear two upper bits of tstamp
      , m_offset(a_offset)
    {}

    static bool valid(uint64_t a_timestamp) { return a_timestamp >> 63; }

    uint32_t number   () const { return m_number;    }
    long     timestamp() const { return m_timestamp; }
    uint32_t offset   () const { return m_offset;    }
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
  public:
    Candle() : m_open(0), m_high(0), m_low(0), m_close(0) {}
    Candle(Candle&&)      = default;
    Candle(Candle const&) = default;

    static constexpr size_t size() { return 4 * sizeof(uint32_t); }

    // Read candle from stream
    const char* read(const char* a_mem) {
      const uint32_t*     p = (const uint32_t*)a_mem;
      auto tmp = be32toh(*p++);
      m_found  = tmp & 0x80000000;
      m_open   = tmp & 0x7FFFffff;
      m_high   = be32toh(*p++);
      m_low    = be32toh(*p++);
      m_close  = be32toh(*p++);
      return (const char*)p;
    }

    uint32_t open () const { return m_open;  }
    uint32_t high () const { return m_high;  }
    uint32_t low  () const { return m_low;   }
    uint32_t close() const { return m_close; }

    bool     found() const { return m_found; }

    void dump(std::ostream& out, const char* title) {
      out << title << "{O="   << m_open
          << " H=" << m_high  << " L=" << m_low
          << " C=" << m_close << "}\n";
    }
  };

  //----------------------------------------------------------------------------
  /// Database I/O file management
  //----------------------------------------------------------------------------
  struct DBState {
    static const int OFFSETLEN_BITS = 4; // in bytes

    DBState();
    DBState(const YMSymbol& a_sym);
    ~DBState();

    int          file           () const { return m_file           ;      }
    int          version        () const { return m_version        ;      }
    std::string  symbol         () const { return m_symbol         ;      }
    time_t       date           () const { return m_date           ;      }
    int          year           () const { return m_year           ;      }
    int          month          () const { return m_month          ;      }
    int          depth          () const { return m_depth          ;      }
    int          scale          () const { return m_scale          ;      }
    int          chunk_size     () const { return m_chunk_size     ;      }
    int          chunk_count    () const { return m_chunk_count    ;      }
    bool         have_candle    () const { return m_have_candle    ;      }
    long         candle_offset  () const { return m_candle_offset  ;      }
    long         chunkmap_offset() const { return m_chunkmap_offset;      }

    int          chunkmap_size  () const { return m_chunk_count*OFFSETLEN_BITS;}

    std::deque<Chunk>& chunkmap ()       { return m_chunkmap;             }
    const Candle&      candle   () const { return m_candle;               }

    void date           (time_t a_date)  { m_date             = a_date;   }
    void chunk_size     (int  a_size)    { m_chunk_size       = a_size;   }
    void candle_offset  (long a_offset)  { m_candle_offset    = a_offset; }
    void chunkmap_offset(long a_offset)  { m_chunkmap_offset  = a_offset; }

    long               file_size() const { return m_mem_end - m_mem;      }

    bool validate_chunk(const Chunk& a_chunk, long& a_bad_offset) {
      // TODO: do chunk validation
      return true;
    }

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
    int               m_chunk_size;
    int               m_chunk_count;
    bool              m_have_candle;
    long              m_candle_offset;
    long              m_chunkmap_offset;
    char*             m_mem;
    const char*       m_mem_end;
    const char*       m_rd_ptr;

    std::deque<Chunk> m_chunkmap;
    Candle            m_candle;

    long offset()                 const { return m_rd_ptr - m_mem;  }
    template <typename T>
    long offset(const T* a_mem)   const { const char* p = (const char*)a_mem;
                                          assert(p > m_mem); return  p-m_mem;   }
    void offset(long a_pos)             { m_rd_ptr = m_mem + a_pos; }

    bool valid_offset(long a_pos) const { return m_mem    + a_pos <  m_mem_end; }
    bool check_space (long a_inc) const { return m_rd_ptr + a_inc <= m_mem_end; }

    const Candle& read_candle();
    void          read_chunk_map();

    //----------------------------------------------------------------------------
    template <class T>
    const T* cast(long a_offset, const char* a_where) const;

    //--------------------------------------------------------------------------
    /// Remap the file in memory
    //--------------------------------------------------------------------------
    void remap(size_t a_size);
  };
} // namespace secdb