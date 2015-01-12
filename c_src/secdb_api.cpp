// vim:ts=2:sw=2:et
//----------------------------------------------------------------------------
/// \file   secdb_api.cpp
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
#include "secdb_api.hpp"
#include <cmath>
#include <stdio.h>
#include <assert.h>

// LEB128 decoding, see:
// http://llvm.org/docs/doxygen/html/LEB128_8h_source.html

namespace secdb {

  inline std::tuple<int, int, int, int>
  timestamp_to_time(long a_ts, int a_scale) {
    long d = 86400 * a_scale;
    // Drop Y-M-D part
    long n = a_ts / d; a_ts -= n * d; d = 3600 * a_scale;
    int  h = a_ts / d; a_ts -= h * d; d = 60   * a_scale;
    int  m = a_ts / d; a_ts -= m * d; d = a_scale;
    int  s = a_ts / d; a_ts -= s * d;
    int ms = a_ts;
    return std::tie(h,m,s,ms);
  }

  //----------------------------------------------------------------------------
  std::pair<long, const char*>
  DBState::Reader::read_trade() {
    //  01 23 4 567
    // +--+--+-+---+-------+--+---+
    // |RT|Ag|S|XXX|TSdelta|Px|Qty|
    // +--+--+-+---+-------+--+---+
    //  ^ ^ ^ ^  ^      ^   ^   ^
    //  \ / | |  |      |   |   |
    //   |  | |  |      |   |   |
    //   |  | |  |      |   |   +-- (......) Qty
    //   |  | |  |      |   +------ (......) Price
    //   |  | |  |      +---------- (......) Timestmap
    //   |  | |  +----------------- (3 bits) Unused
    //   |  | +-------------------- (1 bit ) Side
    //   |  +---------------------- (2 bits) Aggressor
    //   +------------------------- (2 bits) Record Type Indicator
    //
    // RecordType: 0x3  = Full Trade,  0x2   = Delta Trade
    // Aggressor:  0x0  = none,        0x1   = passive, 0x2 = aggressor
    // Side:       0x0  = buy,         0x1   = sell
    // Timestamp:  Full = uint64_t;    Delta = LEB128_int
    // Price:      Full = LEB128_uint; Delta = LEB128_int
    // Qty:        Full = LEB128_uint; Delta = LEB128_int

    const auto begin = m_rd_ptr;

    auto byte = read8();

    if (m_verbose > 1)
      cerr << "Reading "    << ((byte >> 6) == 0x1 ? "full" : "delta")
           << " TRADE at "  << (begin - m_begin) << endl;

#ifndef NDEBUG
    uint8_t rectp = byte >> 6;
#endif

    assert(rectp == 0x2 || rectp == 0x3);
    assert(m_rec_type == RecordT::TRADE || m_rec_type == RecordT::TRADE_DLT);

    Trade::Aggressor aggr = Trade::Aggressor((byte >> 4) & 0x3);

    if ((int)aggr == 0x3)
      return std::make_pair(-1, "invalid_aggressor_value");

    Trade::Side side  = Trade::Side((byte >> 3) & 1);

    if (m_rec_type == RecordT::TRADE) {
      read8(); // filler
      m_last_ts            = read64();
      int     price        = decode_unsigned_leb128();
      int     qty          = decode_unsigned_leb128();
      m_trade.set(m_last_ts, qty,
                  Price(price, m_last_quote.precision()), side, aggr);
    } else {
      int64_t tstamp_delta = decode_signed_leb128();
      int64_t price        = decode_signed_leb128();
      int64_t qty          = decode_signed_leb128();

      // Apply deltas:
      m_last_ts           += tstamp_delta;
      price               += m_last_quote.price().value();
      qty                 += m_last_quote.qty();

      m_trade.set(m_last_ts, qty,
                  Price(price, m_last_quote.precision()), side, aggr);
    }

    m_last_quote.set(m_trade.qty(), m_trade.price().value());

    m_rec_type = RecordT::UNDEFINED;
    return std::make_pair(m_rd_ptr - begin, nullptr);
  }

  //----------------------------------------------------------------------------
  std::pair<long, const char*>
  DBState::Reader::read_mkt_data()
  {
    // Record Type:  0x1  = FullMD, 0x0 = DeltaMD
    // NBids:        Number of bid levels
    // NAsks:        Number of ask levels
    // BidsMask:     Presence map of the Bid Price and Qty (bitsize: 2 * NBids)
    // AsksMask:     Presence map of the Ask Price and Qty (bitsize: 2 * NAsks)
    // Timestamp:    Full = uint64_t;    Delta = LEB128_int
    // [{Price,Qty}] List of LEB128-encoded Price,Qty pairs
    // Price:        Full = LEB128_uint; Delta = LEB128_int
    // Qty:          Full = LEB128_uint; Delta = LEB128_int
    //
    // FullMD:
    // =======
    //  0  2...8 9..15 16     79
    // +--+-----+-----+---------+-------------+
    // |RT|NBids|NAsks|Timestamp|[{Price,Qty}]|
    // +--+-----+-----+---------+-------------+
    //  ^ ^  ^     ^       ^           ^   ^
    //  \ /  |     |       |           |   |
    //   |   |     |       |           |   |
    //   |   |     |       |           |   +-- (......) Qty
    //   |   |     |       |           +------ (......) Price
    //   |   |     |       +------------------ (64 bits) Timestmap
    //   |   |     +-------------------------- (7  bits) NAsks = Number of Asks
    //   |   +-------------------------------- (7  bits) NBids = Number of Bids
    //   +------------------------------------ (2  bits) Record Type Indicator
    //
    // DeltaMD:
    // ========
    //  0  2...8 9..15
    // +--+-----+-----+--------+--------+---+---------+----------------+
    // |01|NBids|NAsks|BidsMask|AsksMask|Pad|Timestamp|[{PxDlt,QtyDlt}]|
    // +--+-----+-----+--------+--------+---+---------+----------------+
    //  ^ ^  ^     ^       ^       ^      ^      ^           ^
    //  \ /  |     |       |       |      |      |           |
    //   |   |     |       |       |      |      |       Price, Qty deltas
    //   |   |     |       |       |      |      +------ Timestamp delta
    //   |   |     |       |       |      +--- (0 or  4) 8-byte Align Padding
    //   |   |     |       |       +---------- (2*NAsks) Asks Presence Map
    //   |   |     |       +------------------ (2*NBids) Bids Presence Map
    //   |   |     +-------------------------- (7  bits) NAsks = Number of Asks
    //   |   +-------------------------------- (7  bits) NBids = Number of Bids
    //   +------------------------------------ (2  bits) Record Type Indicator
    //
    const auto begin = m_rd_ptr;

    auto byte1 = read8();
    auto byte2 = read8();

    if (m_verbose > 2)
      cerr << "Reading "    << ((byte1 >> 6) == 0x1 ? "full" : "delta")
           << " MD at "     << (begin - m_begin) <<  " last-quote="
           << m_last_quote.to_string()           << endl;

    assert((byte1 >> 6) == 0x1 || (byte1 >> 6) == 0x0);

    // Read number of bids and asks:
    auto nbids = ((byte1 & 0x3f) << 1) | ((byte2 >> 7) & 1);
    auto nasks = byte2 & 0x7f;

    // fprintf(stderr, "Read md %d bids=%d, asks=%d\r\n", m_rec_type, nbids, nasks);

    if (m_rec_type == RecordT::MDS) {
      // Decode full MD
      m_last_ts = m_md.timestamp(read64());

      m_md.bids().clear();

      for (uint8_t i=0; i < nbids; ++i) {
        auto px = decode_unsigned_leb128();
        auto sz = decode_unsigned_leb128();
        m_md.bids().emplace_back(sz, px, m_last_quote.precision());
      }

      m_md.asks().clear();

      for (uint8_t i=0; i < nasks; ++i) {
        auto px = decode_unsigned_leb128();
        auto sz = decode_unsigned_leb128();
        m_md.asks().emplace_back(sz, px, m_last_quote.precision());
      }

      m_last_quote = m_md.bids().size() ? m_md.bids().front() : Quote();
    } else {
      assert(m_rec_type == RecordT::MDS_DLT);

      auto last_quote = m_last_quote;

      // Mark beginning/end of the bids/asks presence map
      auto pmap = (const uint8_t*)m_rd_ptr;

      int pmap_len = 2*(nbids + nasks);
      pmap_len += pmap_len & 7; // Pad bits to align to 8-bit boundary

      m_rd_ptr += (pmap_len >> 3); // Add this number of bytes

      auto pmap_end = (const uint8_t*)m_rd_ptr;

      if (!valid()) goto MORE_DATA;

      // Next comes the LEB128-coded timestamp of the MD snapshot delta:
      auto timestamp = decode_signed_leb128();
      m_last_ts      = m_md.timestamp(m_last_ts + timestamp);

      uint8_t j = 0;

      // Next are the price & quantity delta's of bids according to the bitmap:
      m_md.bids().clear();
      for (uint8_t i=0; i < nbids; ++i) {
        auto& md = m_md.bids();

        // Check if bid price or qty are present
        bool has_px  = ((*pmap) >> (7-j++)) & 0x1;
        bool has_qty = ((*pmap) >> (7-j++)) & 0x1;
        if (j == 8) { ++pmap; j = 0; assert(pmap <= pmap_end); }

        auto px = has_px  ? decode_signed_leb128() : 0;
        auto sz = has_qty ? decode_signed_leb128() : 0;
        if (!valid()) goto MORE_DATA;

        auto qty       = last_quote.qty()           + sz;
        auto price     = last_quote.price().value() + px;
        auto precision = last_quote.precision();

        if (m_verbose > 2)
          cerr << "  Read delta quote " << sz  << '@'
               << std::fixed << std::setprecision(precision) << px
               << " (" << qty << '@'
               << std::fixed << std::setprecision(precision) << price
               << ")\n";

        md.emplace_back(qty, price, last_quote.precision());
        last_quote = md.back();
      }

      // Next are the price & quantity delta's of asks according to the bitmap:
      last_quote = m_last_quote;
      m_md.asks().clear();
      for (uint8_t i=0; i < nasks; ++i) {
        auto& md = m_md.asks();

        // Check if bid price or qty are present
        bool has_px  = ((*pmap) >> (7-j++)) & 0x1;
        bool has_qty = ((*pmap) >> (7-j++)) & 0x1;
        if (j == 8) { ++pmap; j = 0; assert(pmap <= pmap_end); }

        auto px = has_px  ? decode_signed_leb128() : 0;
        auto sz = has_qty ? decode_signed_leb128() : 0;
        if (!valid()) goto MORE_DATA;

        auto qty   = last_quote.qty()           + sz;
        auto price = last_quote.price().value() + px;

        if (m_verbose > 2)
          cerr << "  Read delta quote " << sz  << '@' << std::fixed
               << std::setprecision(m_last_quote.precision()) << px
               << " (" << qty << '@' << std::fixed
               << std::setprecision(m_last_quote.precision()) << price
               << ")\n";

        md.emplace_back(qty, price, last_quote.precision());
        last_quote = md.back();
      }
    }

    {
      static const Quote s_empty;
      const Quote& quote = m_md.bids().size() ? m_md.bids().front() : s_empty;
      m_last_quote = quote;
    }

  MORE_DATA:
    m_rec_type = RecordT::UNDEFINED;
    return std::make_pair(m_rd_ptr - begin, nullptr);
  }

  //----------------------------------------------------------------------------
  // DBState
  //----------------------------------------------------------------------------
  DBState::DBState()
    : m_file              (-1)
    , m_version           (0)
    , m_symbol            ()
    , m_date              (0)
    , m_depth             (10)
    , m_scale             (100)
    , m_interval          (0)
    , m_num_intervals     (0)
    , m_daycandle_pos     (0)
    , m_chunkmap_pos      (0)
    , m_mem               ((char*)MAP_FAILED)
    , m_rd_ptr            (nullptr)
    , m_last_interval_time(0)
    , m_next_interval_time(0)
  {}

  //----------------------------------------------------------------------------
  DBState::DBState(const YMSymbol& a_symbol, int a_verbose)
    : m_filename(a_symbol.filename())
    , m_verbose (a_verbose)
    , m_file    (-1)
    , m_mem     ((char*)MAP_FAILED)
    , m_rd_ptr  (nullptr)
    , m_next_interval_time(0)
  {
    if (a_symbol.filename().empty() || a_symbol.filename() == "-")
      throw runtime_error("DBState: reading from stdin not implemented!");

    m_file = open(a_symbol.filename().c_str(), O_RDONLY);

    if (!m_file < 0)
      throw runtime_error
        ("Cannot open file: ", a_symbol.filename(), ": ", strerror(errno));

    // Increase the size of the internal file's buffer
    //setvbuf(file, nullptr, _IOFBF, buf_size);
    //std::ios_base::sync_with_stdio(false);

    if (m_verbose)
      cerr << "Opened database file: " << m_filename << endl;

    // Establish memory mapping on the current file descriptor:
    remap(0);

    // Note: it's not necessary to keep m_file open after mmap, but we do so
    // in case we'll need to remap the file later (i.e. to use a small moving
    // mmap window_sec - feature not currently implemented)

    //--------------------------------------------------------------------------
    // Read file header
    //--------------------------------------------------------------------------
    //file.rdbuf()->sgetn( (char*)&data[0], buf_size ); // low-level

    // Stockdb file header:
    // ====================
    //
    // #!/usr/bin/env secdb
    // version : 100
    // symbol  : NSDQ.AAPL
    // date    : 2015-01-05
    // interval: 300
    // depth   : 2
    // scale   : 100

    char date_str[80], symbol[128];

    int  n = sscanf(m_mem,
      "#!/usr/bin/env secdb\n"
      "version   : %d\n"
      "symbol    : %s\n"
      "date      : %s\n"
      "window_sec: %d\n"
      "depth     : %d\n"
      "scale     : %d\n\n",
      &m_version, symbol, date_str, &m_interval, &m_depth, &m_scale);

    if (n != 6) {
      std::stringstream s;
      s << "Invalid file format:\n\n";
      const char* p = m_mem, *q;
      for (int i=0; p && i < 8; i++, p = q)
        q = (const char*)memchr(p, '\n', 64);

      throw invalid_file_format
        ("Invalid file format:\n\n" + string(m_mem, p ? q-p : 64));
    }

    m_symbol        = symbol;
    m_precision     = m_scale ? (round(std::log(m_scale)/std::log(10))) : 0;
    m_rd_ptr        = strstr(m_mem, "\n\n") + 2;

    m_num_intervals = 24 * 3600 / m_interval + 1;
    m_daycandle_pos = offset();
    m_chunkmap_pos  = m_daycandle_pos + CANDLE_SIZE;
    m_data_offset   = m_chunkmap_pos  + chunkmap_size();

    tzset();  // Update timezone value

    struct tm tm = {0};
    strptime(date_str, "%Y-%m-%d", &tm);
    m_date  = mktime(&tm);
    m_date -= timezone;

    m_next_interval_time = (m_date + m_interval) * 1000;

    strncpy(m_last_date, date_str, sizeof(m_last_date)-1);
    m_last_date[sizeof(m_last_date)-1] = '\0';
    m_last_time[0] = '\0';

    if (m_verbose)
      cerr << "Instrument.: " << m_symbol          << '\n'
           << "Date.......: " << date_str          << '\n'
           << "Depth......: " << m_depth           << '\n'
           << "Precision..: " << m_precision       << '\n'
           << "Scale......: " << m_scale           << '\n'
           << "Interval...: " << m_interval
                              << " (count="        << m_num_intervals << ")\n\n";
    if (m_verbose > 1)
      cerr << "CandlePos..: " << m_daycandle_pos   << '\n'
           << "ChnkMapPos.: " << m_chunkmap_pos    << '\n'
           << "DataPos....: " << m_data_offset     << endl;

    read_candle();  // This is day-wide candle
    read_chunk_map();

    m_last_quote.precision(m_precision);
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
        cerr << "DayCandle..: No candle found\n";
      else
        m_candle.dump(cerr, "DayCandle..: ", m_precision, m_scale);
    }
    return m_candle;
  }

  //----------------------------------------------------------------------------
  void DBState::read_chunk_map() {
    // Read raw chunk map and return {Number, Offset, Candle} list for
    // non-empty chunks:
    size_t n = chunkmap_size(); // in bytes

    if (m_verbose > 1)
      cerr << "Reading chunkmap at offset " << chunkmap_pos()
           << " size=" << n << endl;

    m_chunkmap.clear();

    struct IdxEntry {
      uint32_t chunk_offset;
      char     candle[CANDLE_SIZE];
    };

    auto idx = (const IdxEntry*)m_rd_ptr;
    auto end = idx + m_num_intervals;

    assert(m_rd_ptr + n == (const char*)end);

    for (int i = 0; idx != end; ++idx, ++i) {
      if (!idx->chunk_offset) continue;

      // Read the integer offset
      auto   pos = be32toh(idx->chunk_offset) + data_pos();
      Candle candle(idx->candle);

      // Read the tstamp at offset
      const char* p = m_mem + pos;
      if (data() < p && p >= m_mem_end)
        throw runtime_error
          ("Invalid file format: chunk#", i, " has invalid memory offset!");

      uint8_t byte = *p;

      if (((byte >> 6) & 0x1) != 0x1)
        throw runtime_error("Full MD or Trade event not found at pos ", pos);

      auto tstamp = be64toh(*cast<uint64_t>(pos + 2, "read_chunk_map"));

      if (!Chunk::valid(tstamp))
        throw invalid_file_format
          ("read_chunk_map: database ", m_filename,
           " invalid chunk timestamp ", tstamp, " at index offset ",
           (const char*)idx - m_mem);

      if (m_verbose > 1) {
        cerr << "Adding chunk#" << i << " time=" << tstamp << " pos=" << pos;
        candle.dump(cerr, " candle", m_precision, m_scale);
      }

      m_chunkmap.emplace_back(i, tstamp, pos, candle);
    }

    for (size_t i=0; i < m_chunkmap.size(); ++i) {
      Chunk* chunk = &m_chunkmap[i];
      size_t sz    = i == m_chunkmap.size()-1
                   ? total_size() - m_chunkmap.back().offset()
                   : (chunk+1)->offset() - chunk->offset();
      chunk->size(sz);
      if (m_verbose > 1) {
        int      h,m,s,ms;
        std::tie(h,m,s,ms) = timestamp_to_time(chunk->timestamp(), 1000);
        char buf[32];
        sprintf(buf, "%02d:%02d:%02d.%03d", h, m, s, ms);
        auto pos = chunk->offset() - data_pos();
        cerr << "  chunk "      << setw(3) << chunk->number() << '/' << setw(3)
             << m_num_intervals << ", time="       << chunk->timestamp()
             << ' ' << buf      << ", pos="
             << data_pos()   << '+'     << pos  << '='     << chunk->offset()
             << ", size="       << chunk->size()   << ", ";
        chunk->candle().dump(cerr, "candle", m_precision, m_scale);
      }
    }

    if (*(uint8_t*)end != 0xFF)
      throw invalid_file_format
        ("Beginning of data marker not found in file ", m_file);

    m_rd_ptr = (const char*)end;

    auto valid_file_size = chunkmap_pos() + chunkmap_size() + 1;

    if (m_chunkmap.empty()) {
      if (file_size() < valid_file_size)
        throw invalid_file_format
          ("Empty database ", m_filename, " has invalid chunkmap - delete it!");
      else if (file_size() > valid_file_size) {
        if (m_verbose)
          cerr << "Empty database " << m_filename << " (size=" << file_size()
               << " is longer than the minumum required size: truncating to "
               << valid_file_size   << endl;

        ftruncate64(m_file, valid_file_size);
        remap(valid_file_size);
      }
    } else {
      auto last_chunk_offset = m_chunkmap.back().offset();
      if (!valid_offset(last_chunk_offset))
        throw invalid_file_format
          ("Database file ", m_filename,
           " has inconsistent size (last_chunk_pos=", last_chunk_offset,
           ", file_size=", file_size());

      long bad_offset;
      if (!validate_chunk(m_chunkmap.back(), bad_offset)) {
        // TODO trundate the file to bad_offset???
        throw invalid_file_format
          ("Database file ", m_filename, " is broken at offset ", bad_offset);
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

  //----------------------------------------------------------------------------
  void DBState::update_last_time(long a_timestamp_ms)
  {
    long n = a_timestamp_ms / 1000;
    if (n == m_last_sec)
      return;
    n = n % 86400;
    int h = n / 3600; n -= h*3600;
    int m = n / 60;   n -= m*60;
    int s = n;

    sprintf(m_last_time, "%02d:%02d:%02d", h, m, s);
  }

  //----------------------------------------------------------------------------
  void DBState::print(std::ostream& out, int a_depth, int flags)
  {
    const bool unique =  flags & int(DBState::PrintFlags::UniquePrice)
                      || flags & int(DBState::PrintFlags::UniquePriceAndQty);
    const bool uprice =  flags & int(DBState::PrintFlags::UniquePrice);

    auto update_ts = [&, this, flags](long a_timestamp, char*& p, char c) {
      update_last_time(a_timestamp);
      if (flags & int(PrintFlags::EpochTimeFormat))
        p += sprintf(p, "%ld |%c|", a_timestamp, c);
      else
        p += sprintf(p, "%s %s.%03ld |%c|", m_last_date, m_last_time,
                    a_timestamp % 1000, c);
      if (flags & int(PrintFlags::OutputSymbolName)) {
        *p++ = ' ';
        p    = stpcpy(p, m_symbol.c_str());
      }
    };

    Reader reader((const uint8_t*)m_mem, m_last_timestamp,
                  m_last_quote, m_last_trade, m_last_md, m_verbose);

    for (auto& chunk : m_chunkmap)
    {
      offset(chunk.offset());
      if (m_verbose > 1)
        cerr << "Setting offset " << offset()
             << " (chunk="        << chunk.number()
             << ", size="         << chunk.size() << ')' << endl;

      m_last_interval_time = (m_date + m_interval * chunk.number()) * 1000;
      m_next_interval_time = (m_date + m_interval * (chunk.number() + 1)) * 1000;

      if (m_verbose || (flags & int(DBState::PrintFlags::DisplayCandles))) {
        char buf[64]; char* p = buf;
        update_ts(m_last_interval_time, p, 'C');
        cerr << buf;
        chunk.candle().dump(cerr, " ", m_precision, m_scale);
      }

      const char* end = m_rd_ptr + chunk.size();

      reader.set((const uint8_t*)m_rd_ptr, (const uint8_t*)end);

      if (flags & int(DBState::PrintFlags::DisplayFileInfo)) {
        // Show candle information only
        m_rd_ptr = end;
        continue;
      }

      MDSnapshot last_md;

      char        buf[4096];
      const char* buf_end = buf + sizeof(buf);

      while (m_rd_ptr < end) {
        auto rbegin = (const uint8_t*)m_rd_ptr;
        RecordT     type;
        size_t      sz;
        const char* err;
        std::tie(type, sz, err) = reader.read_next();

        if (m_verbose > 2) {
          cerr << "  size=" << setw(4) << left << sz << " <<";
          for (auto p = rbegin, end = rbegin + sz; p < end; ++p)
            cerr << (p == rbegin ? "" : ",") << (int)*p;
          cerr << ">>\n";
        }

        //if (m_last_timestamp >= m_next_interval_time)
        //  break;

        if (sz < 0)
          throw runtime_error
            ("Error reading market data at position ", offset(), ": ", err);
        else if (sz == 0) {
          if (m_verbose > 1)
            std::cerr << "Chunk#" << chunk.number()
                      << ": need more data at position "
                      << offset() << ": " << err << endl;
          break;
        } else if (sz > chunk.size())
          throw runtime_error
            ("Error reading market data at position ", offset(),
             " expected size ", chunk.size(), " got size ", sz);

        auto md_print = [&, a_depth, this](char* p) {
          auto it  = m_last_md.bids().cbegin();
          auto end = m_last_md.bids().cend();
          for (int i = 0; it != end && i < a_depth; ++it, ++i)
            p += snprintf(p, buf_end-p, " %d@%.*f", it->qty(),
                          it->precision(), it->px());

          p = stpcpy(p, " |");

          it  = m_last_md.asks().cbegin();
          end = m_last_md.asks().cend();
          for (int i = 0; it != end && i < a_depth; ++it, ++i)
            p += snprintf(p, buf_end-p, " %d@%.*f", it->qty(),
                          it->precision(), it->px());

          out << buf << '\n';
        };

        char* p = buf;

        switch (type) {
          case RecordT::MDS:
          case RecordT::MDS_DLT: {
            if (unique &&
                last_md.is_same_prices(m_last_md, a_depth, uprice))
              break;
            update_ts(m_last_md.timestamp(), p, 'M');
            md_print(p);
            last_md = m_last_md;
            break;
          }
          case RecordT::TRADE:
          case RecordT::TRADE_DLT: {
            auto& t = m_last_trade;
            update_ts(t.timestamp(), p, 'T');
            p = stpcpy(p, t.qty() > 0 ? " B " : " S ");
            sprintf(p, "%10d %9.*f %c\n",
                    abs(t.qty()), t.precision(), t.px(), t.aggr_to_char());
            out << buf;
            break;
          }
          default:
            break;
        }

        add_offset(sz);  // increment the offset pointer
      }
    }
  }

} // namespace secdb
