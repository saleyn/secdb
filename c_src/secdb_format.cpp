// vim:ts=2:sw=2:et
//----------------------------------------------------------------------------
/// \file   secdb_format.cpp
/// \author Serge Aleynikov
//----------------------------------------------------------------------------
/// \brief C++/Erlang NIF implementation.
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
#include <stdint.h>
#include <strings.h>
#include "erl_nif.h"
#include "secdb_api.hpp"
#include <errno.h>
#include <stdio.h>
#include <arpa/inet.h>

using namespace secdb;

static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_md;
static ERL_NIF_TERM am_trade;
static ERL_NIF_TERM am_undefined;
static ERL_NIF_TERM am_buy;
static ERL_NIF_TERM am_sell;
static ERL_NIF_TERM am_true;
static ERL_NIF_TERM am_false;
static ERL_NIF_TERM am_full;
static ERL_NIF_TERM am_delta;
static ERL_NIF_TERM am_signed;
static ERL_NIF_TERM am_unsigned;

//------------------------------------------------------------------------------
// Erlang NIF API
//------------------------------------------------------------------------------
static int on_load   (ErlNifEnv*, void**,         ERL_NIF_TERM);
static int on_upgrade(ErlNifEnv*, void**, void**, ERL_NIF_TERM);
static int on_reload (ErlNifEnv*, void**,         ERL_NIF_TERM);

static ERL_NIF_TERM
read_one_row(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM
decode_leb128(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifFunc secdb_funcs[] =
{
  {"do_decode_packet", 5, read_one_row},
  {"do_decode_packet", 6, read_one_row},
  {"do_leb128_decode", 3, decode_leb128}
};

ERL_NIF_INIT(secdb_format, secdb_funcs, on_load, on_reload, on_upgrade, NULL);

//------------------------------------------------------------------------------
// NIF implementations
//------------------------------------------------------------------------------
static int on_load(ErlNifEnv* env, void**, ERL_NIF_TERM) {
  am_ok        = enif_make_atom(env, "ok");
  am_error     = enif_make_atom(env, "error");
  am_md        = enif_make_atom(env, "md");
  am_trade     = enif_make_atom(env, "trade");
  am_undefined = enif_make_atom(env, "undefined");
  am_buy       = enif_make_atom(env, "buy");
  am_sell      = enif_make_atom(env, "sell");
  am_true      = enif_make_atom(env, "true");
  am_false     = enif_make_atom(env, "false");
  am_full      = enif_make_atom(env, "full");
  am_delta     = enif_make_atom(env, "delta");
  am_signed    = enif_make_atom(env, "signed");
  am_unsigned  = enif_make_atom(env, "unsigned");
  return 0;
}

static int on_upgrade(ErlNifEnv*, void** priv_data, void** old_data, ERL_NIF_TERM info) {
  return 0;
}

static int on_reload(ErlNifEnv*, void** priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static unsigned to_precision(unsigned a_scale) {
  if (a_scale <= 1)         return 0;
  if (a_scale == 10)        return 1;
  if (a_scale == 100)       return 2;
  if (a_scale == 1000)      return 3;
  if (a_scale == 10000)     return 4;
  if (a_scale == 100000)    return 5;
  if (a_scale == 1000000)   return 6;
  return round(log(a_scale) / log(10));
}

//------------------------------------------------------------------------------
static ERL_NIF_TERM
read_one_row(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  // Args: Bin, Offset, Scale, LastTS, LastQuote
  ErlNifBinary        bin;
  size_t              offset;
  unsigned            scale;
  long                last_ts;
  int                 arity;
  const ERL_NIF_TERM* last_qte;
  long                last_px;
  long                last_qty;
  unsigned            verbose = 0;

  if (argc < 5)                                     return enif_make_badarg(env);
  if (!enif_inspect_binary(env, argv[0], &bin))     return enif_make_badarg(env);
  if (!enif_get_ulong(env, argv[1], &offset))       return enif_make_badarg(env);
  if (!enif_get_uint (env, argv[2], &scale))        return enif_make_badarg(env);
  if (!enif_get_long (env, argv[3], &last_ts))      return enif_make_badarg(env);
  if (!enif_get_tuple(env, argv[4], &arity, &last_qte)
      || arity != 2)                                return enif_make_badarg(env);
  if (!enif_get_long (env, last_qte[0], &last_px))  return enif_make_badarg(env);
  if (!enif_get_long (env, last_qte[1], &last_qty)) return enif_make_badarg(env);
  if (argc == 6 &&
     (!enif_get_uint (env, argv[6], &verbose)))     return enif_make_badarg(env);

  Quote           quote(last_qty, last_px, to_precision(scale));
  Trade           trade;
  MDSnapshot      md;
  DBState::Reader reader(bin.data, offset, bin.data + bin.size,
                         last_ts, quote, trade, md, verbose);
  RecordT         type;
  size_t          sz;
  const char*     err;
  std::tie(type,  sz, err) = reader.read_next();

  switch (type) {
    case RecordT::MDS:
    case RecordT::MDS_DLT: {
      // Format bids
      auto tail = enif_make_list (env, 0);

      for (auto it = md.bids().rbegin(), end = md.bids().rend(); it != end; ++it) {
        auto px  = enif_make_double(env, it->px());
        auto qty = enif_make_ulong (env, it->qty());
        tail     = enif_make_list_cell(env, enif_make_tuple2(env, px, qty), tail);
      }

      auto bids = tail;

      // Format asks
      tail = enif_make_list(env, 0);

      for (auto it = md.asks().rbegin(), end = md.asks().rend(); it != end; ++it) {
        auto px  = enif_make_double(env, it->px());
        auto qty = enif_make_ulong (env, it->qty());
        tail     = enif_make_list_cell(env, enif_make_tuple2(env, px, qty), tail);
      }

      auto asks  = tail;

      auto kind  = (type == RecordT::MDS) ? am_full : am_delta;
      auto ts    = enif_make_ulong(env, last_ts);
      auto pkt   = enif_make_tuple4(env, am_md, ts, bids, asks);
      auto pos   = enif_make_ulong(env, offset + sz);

      if (!md.bids().empty())
        quote = md.bids().front();
      else if (!md.asks().empty())
        quote = md.asks().front();

      auto px    = enif_make_ulong(env, quote.price().value());
      auto qty   = enif_make_ulong(env, quote.qty());
      auto qte   = enif_make_tuple2(env, px, qty);

      // Return:
      //  {full|delta, {md, TS, Bids, Asks}, Offset, TS, {Px,Qty}}
      return enif_make_tuple5(env, kind, pkt, pos, ts, qte);
    }
    case RecordT::TRADE:
    case RecordT::TRADE_DLT: {
      auto kind = (type == RecordT::TRADE) ? am_full : am_delta;
      auto ts   = enif_make_ulong(env, last_ts);
      auto side = trade.side() == Trade::BUY ? am_buy : am_sell;
      auto aggr = trade.aggressor() == Trade::UNDEFINED
                ? am_undefined
                : trade.aggressor() == Trade::AGGRESSIVE ? am_true : am_false;
      auto px   = enif_make_double(env, trade.px());
      auto qty  = enif_make_ulong (env, trade.qty());

      auto pkt  = enif_make_tuple6(env, am_trade, ts, side, px, qty, aggr);
      auto pos  = enif_make_ulong(env, offset + sz);

      px        = enif_make_ulong(env, trade.price().value());
      auto qte  = enif_make_tuple2(env, px, qty);

      // Return:
      //  {full|delta, {trade, TS, Side, Px, Qty, Aggr}, Offset, TS, {Px,Qty}}
      return enif_make_tuple5(env, kind, pkt, pos, ts, qte);
    }
    default: {
      static const auto reason = enif_make_atom(env, "invalid_rec_type");
      return enif_make_tuple2(env, am_error, reason);
    }
  }
}

//------------------------------------------------------------------------------
static ERL_NIF_TERM
decode_leb128(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  // Args: Bin, signed|unsigned, Offset
  ErlNifBinary bin;
  size_t       offset;
  ERL_NIF_TERM res;

  if (argc != 3)                                    return enif_make_badarg(env);
  if (!enif_inspect_binary(env, argv[0], &bin))     return enif_make_badarg(env);
  if (!enif_get_ulong     (env, argv[2], &offset))  return enif_make_badarg(env);

  auto begin = (const uint8_t*)bin.data + offset;
  auto p     = begin;
  auto end   = p + bin.size;

  if (enif_is_identical(argv[1], am_signed)) {
    auto n = DBState::Reader::decode_signed_leb128(p);
    if (p > end) return enif_make_badarg(env);
    res = enif_make_long(env, n);
  } else if (enif_is_identical(argv[1], am_unsigned)) {
    auto n = DBState::Reader::decode_unsigned_leb128(p);
    if (p > end) return enif_make_badarg(env);
    res = enif_make_ulong(env, n);
  } else
    return enif_make_badarg(env);

  return enif_make_tuple2(env, res, enif_make_ulong(env, offset + (p - begin)));
}
