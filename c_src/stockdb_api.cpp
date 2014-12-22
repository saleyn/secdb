// vim:ts=2:sw=2:et
#include "stockdb_api.hpp"
#include <assert.h>

// LEB128 decoding, see:
// http://llvm.org/docs/doxygen/html/LEB128_8h_source.html

namespace stockdb {

  //----------------------------------------------------------------------------
  BitReader::tuple<uint64_t>
  BitReader::leb128_decode_unsigned() {
    uint64_t result  = 0;
    int      shift   = 0;
    bool     move_on = true;

    if (m_offset & 0x7) {
      for (; move_on; shift += 7) {
        auto res = std::move(bit_get());
        if (!res.ok) return tuple<uint64_t>();
        move_on = res.value;
        auto chunk = std::move(bits_get(7));
        if (!chunk.ok) return tuple<uint64_t>();
        result |= (chunk.value << shift);
        // fprintf(stderr, "ULeb: %d, %llu, %llu\r\n", move_on, chunk, result);
      }
    } else {
      for (; move_on; shift += 7) {
        if(bits_remain() < 8) return 0;
        unsigned char b = *bits_head();
        move_on =  b >> 7;
        result |= (b & 0x7F) << shift;
        bits_skip(8);
      }
    }

    return tuple<uint64_t>(result);
  }

  //----------------------------------------------------------------------------
  BitReader::tuple<int64_t>
  BitReader::leb128_decode_signed() {
    int64_t result  = 0;
    int     shift   = 0;
    bool    move_on = true;
    bool    sign = 0;
    if(m_offset & 0x7) {
      for (; move_on; shift += 7) {
        auto bit   = std::move(bit_get());
        if (!bit.ok)   return tuple<int64_t>();
        move_on    = bit.value;
        auto signb = std::move(bit_look());
        if (!signb.ok)  return tuple<int64_t>();
        sign       = signb.value;
        auto chunk = bits_get(7);
        if (!chunk.ok) return tuple<int64_t>();
        result |= (chunk.value << shift);
        // fprintf(stderr, "SLeb: %d, %llu, %lld\r\n", move_on, chunk, sign ? result | - (1 << shift) :  result);
      }
    } else {
      for (; move_on; shift += 7) {
        if (bits_remain() < 8) return tuple<int64_t>();
        auto b  = *bits_head();
        move_on =  b >> 7;
        sign    = (b >> 6) & 1;
        result |= (b & 0x7F) << shift;
        bits_skip(8);
      }

    }
    if (sign)
      result |= - (1 << shift);
    return tuple<int64_t>(result);
  }

  //----------------------------------------------------------------------------
  inline BitReader::tuple<uint64_t>
  BitReader::bits_get(int a_bits)
  {
    if (m_offset + a_bits > long(m_size)) return tuple<uint64_t>();

    uint64_t result = 0;

    if (a_bits <= 8 && (m_offset & 0x7)) {
      for (; a_bits; --a_bits) {
        auto res = std::move(bit_get());
        if (!res.ok) return tuple<uint64_t>();
        result = (result << 1) | res.value;
      }
      return tuple<uint64_t>(result);
    }

    if(a_bits == 32 && !(m_offset & 7)) {
      result    = be32toh(*(uint32_t *)(&m_bytes[m_offset / 8]));
      m_offset += 32;
      return tuple<uint64_t>(result);
    }

    while (a_bits) {
      while (a_bits >= 8 && !(m_offset & 0x7)) {
        a_bits   -= 8;
        result    = (result << 8) | m_bytes[m_offset >> 3];
        m_offset += 8;
      }
      if (a_bits) {
        auto res = std::move(bit_get());
        if (!res.ok) return tuple<uint64_t>();
        result = (result << 1) | res.value;
        a_bits--;
      }
    }
    return tuple<uint64_t>(result);
  }

  //----------------------------------------------------------------------------
  std::pair<int, const char*>
  BitReader::read_trade() {
    // fprintf(stderr, "Read trade %d\r\n", bin.size);
    if (m_size < 8*(8 + 2*4)) return std::make_pair(-1, "more_data_for_trade");

    auto timestamp = bits_get(62);
    if(!timestamp.ok) return std::make_pair(-1, "more_data_for_trade");

    auto raw_px = bits_get(32);
    if (!raw_px.ok) return std::make_pair(-1, "more_data_for_trade_price");

    // Cut to 32 bits and consider signed
    double px    = double((int32_t)raw_px.value & 0xFFFFFFFF);
    double price = m_scale ? (px / m_scale) : px;

    auto qty = bits_get(32);
    if(!qty.ok) return std::make_pair(-1, "more_data_for_trade_volume");

    m_trade.set(timestamp.value, qty.value, price);

    return std::make_pair(8 + 2*4, nullptr);
  }

  //----------------------------------------------------------------------------
  /// @param a_append_delta if true and current record type is market data delta,
  ///                       the delta values will be added to mkt_data() record
  //----------------------------------------------------------------------------
  std::pair<int, const char*> BitReader::
  read_mkt_data(bool a_append_delta)
  {
    switch (m_rec_type) {
      case MKT_DATA: {
        // fprintf(stderr, "Read full md %d\r\n", bin.size);

        if (long(m_size) < m_full_md_rec_size)
          // fprintf(stderr, "Only %d bytes for depth %d, %d\r\n",
          //   (int)bin.size, (int)depth, (int)(8 + 2*2*depth*4));
          return std::make_pair(0, "more_data_for_full_md");

        // Here is decoding of simply coded full string
        auto timestamp = bits_get(62);
        if (!timestamp.ok) return std::make_pair(0, "more_data_for_md_ts");

        m_mkt_data.timestamp(timestamp.value);

        int i;
        MktData<Level>::iterator mdi;

        for (i=0, mdi = m_mkt_data.begin(); i < depth(); ++i, ++mdi) {
          auto bid_px = bits_read32();
          auto bid_sz = bits_read32();
          mdi->bid().set(bid_sz.value, bid_px.value);
        }

        mdi = m_mkt_data.begin();
        for (i=0, mdi = m_mkt_data.begin(); i < depth(); ++i, ++mdi) {
          auto ask_px = bits_read32();
          auto ask_sz = bits_read32();
          mdi->ask().set(ask_sz.value, ask_px.value);
        }
        break;
      }
      case MKT_DATA_DLT: {
        // fprintf(stderr, "Read delta md %d\r\n", bin.size);

        if(!bits_skip(3))
          return std::make_pair(0, "more_data_for_bidask_delta_flags");

        int i;
        MktData<Level>::iterator mdi;

        for (i=0, mdi = m_mkt_data.begin(); i < depth(); ++i, ++mdi) {
          auto px_dlt = bit_get();
          if (!px_dlt.ok) return std::make_pair(0, "more_data_for_bidask_delta_flags");
          auto sz_dlt = bit_get();
          if (!sz_dlt.ok) return std::make_pair(0, "more_data_for_bidask_delta_flags");
          mdi->bid().has(sz_dlt.value, px_dlt.value);
        }

        for (i=0, mdi = m_mkt_data.begin(); i < depth(); ++i, ++mdi) {
          auto px_dlt = bit_get();
          if (!px_dlt.ok) return std::make_pair(0, "more_data_for_bidask_delta_flags");
          auto sz_dlt = bit_get();
          if (!sz_dlt.ok) return std::make_pair(0, "more_data_for_bidask_delta_flags");
          mdi->ask().has(sz_dlt.value, px_dlt.value);
        }

        bits_align();

        auto timestamp = leb128_decode_unsigned();

        if (!timestamp.ok) return std::make_pair(0, "more_data_for_delta_ts");

        m_mkt_data.timestamp(timestamp.value);

        for (i=0, mdi = m_mkt_data.begin(); i < depth(); ++i, ++mdi) {
          auto& md = mdi->bid();
          auto erp = decode_delta(a_append_delta, md.has_px(), md.price(), "more_data_for_delta_bid_px");
          if  (erp) return std::make_pair(0, erp);
          auto erq = decode_delta(a_append_delta, md.has_qty(),  md.qty(), "more_data_for_delta_bid_qty");
          if  (erq) return std::make_pair(0, erq);
        }

        for (i=0, mdi = m_mkt_data.begin(); i < depth(); ++i, ++mdi) {
          auto& md = mdi->ask();
          auto erp = decode_delta(a_append_delta, md.has_px(), md.price(), "more_data_for_delta_ask_px");
          if  (erp) return std::make_pair(0, erp);
          auto erq = decode_delta(a_append_delta, md.has_qty(),  md.qty(), "more_data_for_delta_ask_qty");
          if  (erq) return std::make_pair(0, erq);
        }

        bits_align();
        break;
      }
      case TRADE:
        assert(false);  // Wrong function call - must call read_trade()!
      default:
        return std::make_pair(-1, "unknown_rec_type");
    }

    m_rec_type = UNDEFINED;
    int shift  = bits_byte_offset();

    return std::make_pair(shift, nullptr);
  }

  //----------------------------------------------------------------------------
  // DBState
  //----------------------------------------------------------------------------
  DBState::DBState()
    : m_file            (-1)
    , m_version         (0)
    , m_symbol          ()
    , m_date            (0)
    , m_depth           (10)
    , m_scale           (100)
    , m_chunk_size      (0)
    , m_chunk_count     (0)
    , m_have_candle     (0)
    , m_candle_offset   (0)
    , m_chunkmap_offset (0)
    , m_mem             ((char*)MAP_FAILED)
    , m_rd_ptr          (nullptr)
  {}

  //----------------------------------------------------------------------------
  DBState::DBState(const std::string& a_filename, int a_verbose)
    : m_filename(a_filename)
    , m_verbose (a_verbose)
    , m_file    (-1)
    , m_mem     ((char*)MAP_FAILED)
    , m_rd_ptr  (nullptr)
  {
    if (a_filename.empty() || a_filename == "-")
      throw runtime_error("DBState: reading from stdin not implemented!");

    m_file = open(a_filename.c_str(), O_RDONLY);

    if (!m_file < 0)
      throw runtime_error
        (string("Cannot open file: ") + a_filename + ": " + strerror(errno));

    // Increase the size of the internal file's buffer
    //setvbuf(file, nullptr, _IOFBF, buf_size);
    //std::ios_base::sync_with_stdio(false);

    if (m_verbose)
      cerr << "Opened database file: " << m_filename << endl;

    // Establish memory mapping on the current file descriptor:
    remap(0);

    // Note: it's not necessary to keep m_file open after mmap, but we do so
    // in case we'll need to remap the file later (i.e. to use a small moving
    // mmap window - feature not currently implemented)

    //--------------------------------------------------------------------------
    // Read file header
    //--------------------------------------------------------------------------
    //file.rdbuf()->sgetn( (char*)&data[0], buf_size ); // low-level

    // Stockdb file header:
    // ====================
    //
    // #!/usr/bin/env stockdb
    // chunk_size: 300
    // date: 2012-01-15
    // depth: 2
    // scale: 100
    // stock: NASDAQ.AAPL
    // version: 2
    // have_candle: true

    char date_str[80], symbol[64], cndle[20];

    int  n = sscanf(m_mem,
      "#!/usr/bin/env stockdb\n"
      "chunk_size: %d\n"
      "date: %s\n"
      "depth: %d\n"
      "scale: %d\n"
      "stock: %s\n"
      "version: %d\n"
      "have_candle: %s\n\n",
      &m_chunk_size, date_str, &m_depth, &m_scale, symbol, &m_version, cndle);

    if (n != 7) {
      std::stringstream s;
      s << "Invalid file format:\n\n";
      const char* p = m_mem, *q;
      for (int i=0; p && i < 8; i++, p = q)
        q = (const char*)memchr(p, '\n', 64);

      throw invalid_file_format
        ("Invalid file format:\n\n" + string(m_mem, p ? q-p : 64));
    }


    m_rd_ptr = strstr(m_mem, "\n\n") + 2;

    m_have_candle     = strcmp(cndle, "true") == 0;
    m_candle_offset   = m_have_candle ? offset() : -1;
    m_chunkmap_offset = m_have_candle ? m_candle_offset + 4*4 : offset();
    m_chunk_count     = 24 * 3600 / m_chunk_size + 1;

    tzset();  // Update timezone value

    struct tm tm;
    strptime(date_str, "%Y-%m-%d", &tm);
    m_date = mktime(&tm) - timezone;

    if (m_verbose)
      cerr << "Instrument: " << m_symbol          << '\n'
            << "Date......: " << date_str          << '\n'
            << "Depth.....: " << m_depth           << '\n'
            << "Scale.....: " << m_scale           << '\n'
            << "ChunkSize.: " << m_chunk_size
                              << " (count="        << m_chunk_count << ")\n"
            << "HaveCandle: " << cndle             << "\n\n"
            << "CandlePos.: " << m_candle_offset   << '\n'
            << "ChnkMapPos: " << m_chunkmap_offset << endl;

    if (m_have_candle)
      read_candle();

    read_chunk_map();
  }

  //----------------------------------------------------------------------------
  DBState::~DBState() {
    if (m_mem != MAP_FAILED)
      { munmap(m_mem, file_size()); m_mem = (char*)MAP_FAILED; }
    close(m_file);
  }

  //----------------------------------------------------------------------------
  const Candle& DBState::read_candle() {
    if (!check_space(Candle::size()))
      throw runtime_error
        ("Cannot read candle at position " + to_string(offset()) + " of "
        + to_string(file_size()));

    m_rd_ptr = m_candle.read(m_rd_ptr);

    if (m_verbose) {
      if (!m_candle.found())
        cerr << "Candle....: No candle found\n";
      else
        m_candle.dump(cerr, "Candle....: ");
    }
    return m_candle;
  }

  //----------------------------------------------------------------------------
  void DBState::read_chunk_map() {
    // Read raw chunk map and return {Number, Offset} list for non-empty chunks
    size_t n = chunkmap_size(); // in bytes

    if (m_verbose > 1)
      cerr << "Reading chunkmap at offset " << chunkmap_offset()
          << " size=" << n << endl;

    m_chunkmap.clear();

    auto chunk = (const uint32_t*)m_rd_ptr;
    auto end   = chunk + (n / sizeof(uint32_t));

    assert(m_rd_ptr + n == (const char*)end);

    for (int i = 0; chunk != end; ++chunk, ++i) {
      if (!*chunk) continue;

      // Read the integer offset
      auto pos    = be32toh(*chunk);
      // Read the tstamp at offset
      auto tstamp = be64toh(*cast<uint64_t>(m_chunkmap_offset+pos, "read_chunk_map"));

      if (!Chunk::valid(tstamp))
        throw invalid_file_format
          ("read_chunk_map: database " + m_filename +
          " invalid chunk timestamp at " +
          to_string(offset(chunk) + 4 /* sizeof(offset) */) +
          ": " + to_string(tstamp));

      m_chunkmap.emplace_back(i, tstamp, pos);

      if (m_verbose > 1)
        cerr << "  chunk " << setw(6) << i    << " (pos=" << pos << ", time="
              << m_chunkmap.back().timestamp() << ')'      << endl;
    }

    m_rd_ptr = (const char*)end;

    auto valid_file_size = chunkmap_offset() + chunkmap_size();

    if (m_chunkmap.empty()) {
      if (file_size() < valid_file_size)
        throw invalid_file_format
          ("Empty database " + m_filename + " has invalid chunkmap - delete it!");
      else if (file_size() > valid_file_size) {
        if (m_verbose)
          cerr << "Empty database " << m_filename << " (size=" << file_size()
              << " is longer than the minumum required size: truncating to "
              << valid_file_size   << endl;

        ftruncate64(m_file, valid_file_size);
        remap(valid_file_size);
      }
    } else {
      auto abs_offset = m_chunkmap_offset + m_chunkmap.back().offset();
      if (!valid_offset(abs_offset))
        throw invalid_file_format
          ("Database file " + m_filename +
          " has inconsistent size (last_chunk_pos=" + to_string(abs_offset) +
          ", file_size=" + to_string(file_size()));

      long bad_offset;
      if (!validate_chunk(m_chunkmap.back(), bad_offset)) {
        // TODO trundate the bile to bad_offset???
        throw invalid_file_format
          ("Database file " + m_filename + " is broken at offset " +
          to_string(bad_offset));
      }
    }
  }

  //----------------------------------------------------------------------------
  void DBState::remap(size_t a_size) {
    auto pos = offset();

    struct stat buf;
    fstat(m_file, &buf);
    auto fsize = buf.st_size;

    if (m_mem != MAP_FAILED)
      munmap(m_mem, file_size());

    auto sz = a_size ? a_size : fsize;

    if (m_verbose)
      cerr << "Mapping memory of size " << sz << " (file size=" << fsize
            << ")\n";

    auto flags = MAP_PRIVATE|MAP_NORESERVE|MAP_POPULATE;
    m_mem = (char*)mmap(nullptr, sz, PROT_READ, flags, m_file, 0);

    if (m_mem == MAP_FAILED) {
      int err = errno;
      close(m_file); // This function maybe called from ctor, so close the handle
      throw runtime_error("Cannot mmap the file " + m_filename + ": "
                          + strerror(err));
    }

    m_mem_end = m_mem + sz; // Set buffer size to fit the entire file:
    offset(pos);            // Restore old pointer
  }

} // namespace stockdb
