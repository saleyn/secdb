-module(secdb_format_tests).
-include("../include/secdb.hrl").
-include("secdb.hrl").

% EUnit tests
-include_lib("eunit/include/eunit.hrl").

-import(secdb_format, [l1_quote/1]).

full_md_test() ->
  TS      = 16#138BDF77CBA,
  LQ      = {1530, 250},
  MD      = #md{timestamp = 16#138BDF77CBA, bid = [{15.30, 250}, {15.20, 111}], ask = [{16.73,15}, {17.00,90}]},
  Bin     = <<65,2,0,0,1,56,189,247,124,186,250,11,250,1,240,11,111,137,13,15,164,13,90>>,
  BinSize = byte_size(Bin),
  Tail    = <<7, 239, 183, 19>>,
  DB      = #db{scale=100, depth=2, last_ts=0, last_quote = {0,0}},

  ?assertMatch({Bin,_}, secdb_format:encode(full, MD, DB)),
  ?assertMatch({full, MD, BinSize, TS, LQ},
    secdb_format:decode(<<Bin/binary, Tail/binary>>, 0, 100, 0, {0,0})),
  ?assertMatch({full, MD, BinSize, #db{last_quote = LQ, last_ts = TS}},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 0, DB)).

delta_md_test() ->
  PrevTimestamp = 16#138BDF77CBA,
  DTimestamp    = 270,
  Timestamp     = PrevTimestamp+DTimestamp,

  LastL1        = {1530, 250},
  Scale         = 100,
  %DBid          = [{0, -4}, {0, 0}],
  %DAsk          = [{-230, 100}, {-100, -47}],
  RealBid       = [{15.30, 246}, {15.20, 111}],
  RealAsk       = [{14.43, 115}, {16.00, 43}],

  DB            = #db{scale=Scale, depth=2, last_ts=PrevTimestamp, last_quote=LastL1},

  Bin           = <<1,2,127,142,2,124,118,249,126,169,127,249,126,157,1,184,127>>,
  BinSize       = byte_size(Bin),
  Tail          = <<7, 239, 183, 19>>,

  MD = #md{timestamp=Timestamp,  bid=RealBid, ask=RealAsk},

  ?assertMatch({Bin,_}, secdb_format:encode(delta, MD, DB)),
  ?assertMatch({delta, MD, BinSize, Timestamp, {1530,246}},
    secdb_format:decode(<<Bin/binary, Tail/binary>>, 0, 100, PrevTimestamp, LastL1)),
  ?assertMatch({delta, MD, BinSize, #db{last_quote = {1530,246}, last_ts = Timestamp}},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 0, DB)).


trade_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 16#BEEF,
  Scale = 100,
  Bin = <<200,0,0,0,1,56,189,247,124,186,173,189,3,239,253,2>>,
  Tail = <<7, 239, 183, 19>>,
  DB = #db{scale=Scale, depth=2},
  Trade1 = #trade{timestamp = Timestamp, price = Price / Scale, volume = Volume, side=sell},
  BinSize = byte_size(Bin),

  ?assertMatch({Bin,_}, secdb_format:encode(full, Trade1, DB)),
  ?assertEqual({full,Trade1,BinSize, Timestamp, {16#DEAD,16#BEEF}},
    secdb_format:decode(<<Bin/binary, Tail/binary>>, 0, 100, 0, {0,0})),
  ?assertMatch({full,Trade1,BinSize, #db{last_quote={16#DEAD,16#BEEF}, last_ts=Timestamp}},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 0, DB)),

  % Zero volume test
  Bin2   = <<192,0,0,0,1,56,189,247,124,186,173,189,3,0>>,
  Trade2 = #trade{timestamp = Timestamp, price = Price / Scale, volume = 0, side=buy},

  ?assertMatch({Bin2,_}, secdb_format:encode(full, Trade2, DB)),
  ?assertEqual({full,Trade2,byte_size(Bin2), Timestamp, {16#DEAD,0}},
    secdb_format:decode(Bin2, 0, 100, 0, {0,0})),

  % Negative price/volume test
  Trade3 = #trade{timestamp = Timestamp, price = -Price / Scale, volume =  Volume, side=sell},
  Trade4 = #trade{timestamp = Timestamp, price =  Price / Scale, volume = -Volume, side=sell},
  ?assertError(function_clause, secdb_format:encode(full, Trade3, DB)),
  ?assertError(function_clause, secdb_format:encode(full, Trade4, DB)).



format_header_value_test() ->
  ?assertEqual("2012-07-19", lists:flatten(secdb_format:format_header_value(date, {2012, 7, 19}))),
  ?assertEqual("MICEX.URKA", lists:flatten(secdb_format:format_header_value(symbol, 'MICEX.URKA'))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(depth, 17))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(scale, 17))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(window_sec, 17))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(version, 17))).

parse_header_value_test() ->
  ?assertEqual({2012, 7, 19}, secdb_format:parse_header_value(date, "2012/07/19")),
  ?assertEqual({2012, 7, 19}, secdb_format:parse_header_value(date, "2012-07-19")),
  ?assertEqual('MICEX.URKA', secdb_format:parse_header_value(symbol, "MICEX.URKA")),
  ?assertEqual(17, secdb_format:parse_header_value(depth, "17")),
  ?assertEqual(17, secdb_format:parse_header_value(scale, "17")),
  ?assertEqual(17, secdb_format:parse_header_value(window_sec, "17")),
  ?assertEqual(17, secdb_format:parse_header_value(version, "17")).