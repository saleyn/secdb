-module(secdb_format_tests).
-include("../include/secdb.hrl").
-include("secdb.hrl").

% EUnit tests
-include_lib("eunit/include/eunit.hrl").


full_md_test() ->
  Timestamp = 16#138BDF77CBA,
  RealBid = [{15.30, 250}, {15.20, 111}],
  RealAsk = [{16.73, 15}, {17.00, 90}],
  Depth   = 2,
  Scale   = 100,
  Bid     = [{1530, 250}, {1520, 111}],
  Ask     = [{1673, 15}, {1700, 90}],
  Bin     = <<16#80000138BDF77CBA:64/integer,
              1530:32/integer, 250:32/integer,  1520:32/integer, 111:32/integer,
              1673:32/integer, 15:32/integer,   1700:32/integer, 90:32/integer>>,
  BinSize = byte_size(Bin),
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(Bin, secdb_format:encode_full_md(#md{timestamp = Timestamp, bid = RealBid, ask = RealAsk}, Scale, Depth)),

  ?assertEqual({#md{timestamp = Timestamp, bid=Bid, ask=Ask}, byte_size(Bin)},
    secdb_format:decode_full_md(<<Bin/binary, Tail/bitstring>>, 2)),
  
  ?assertEqual({#md{timestamp = Timestamp, bid = Bid, ask = Ask}, byte_size(Bin)},
    secdb_format:decode_packet(<<Bin/binary, Tail/bitstring>>, Scale)),
  ?assertMatch({#md{}, BinSize}, secdb_format:decode_packet(<<Bin/binary, Tail/bitstring>>, #md{}, Scale)),
  {#md{} = MD, _} = secdb_format:decode_packet(<<Bin/binary, Tail/bitstring>>, #md{}, Scale),
  ?assertEqualMD(#md{timestamp = Timestamp, bid = RealBid, ask = RealAsk}, MD).
  

full_md_negative_price_test() ->
  Timestamp = 16#138BDF77CBA,
  Bid = [{-1530, 250}, {-1520, 111}],
  Ask = [{-1673, 15},  {-1700, 90}],
  Bin = <<16#80000138BDF77CBA:64/integer,
    -1530:32/signed-integer, 250:32/integer,  -1520:32/signed-integer, 111:32/integer,
    -1673:32/signed-integer, 15:32/integer,   -1700:32/signed-integer, 90:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(Bin, secdb_format:encode_full_md(#md{timestamp = Timestamp, bid = Bid, ask = Ask}, 100, 2)),
  ?assertEqual(Bin, secdb_format:encode_full_md(#md{timestamp = Timestamp, bid = Bid, ask = Ask}, 100, 2)),
  ?assertEqual({Timestamp, Bid, Ask, byte_size(Bin)},
    secdb_format:decode_full_md(<<Bin/binary, Tail/bitstring>>, 100)),
  ?assertEqual({ok, #md{timestamp = Timestamp, bid = Bid, ask = Ask}, byte_size(Bin)},
    secdb_format:decode_packet(<<Bin/binary, Tail/bitstring>>, 100)).

full_md_negative_volume_test() ->
  Timestamp = 16#138BDF77CBA,
  Bid = [{1530, 250}, {1520, 111}],
  Ask = [{1673, -15}, {1700, 90}],
  ?assertException(error, function_clause, secdb_format:encode_full_md(#md{timestamp = Timestamp, bid = Bid, ask = Ask}, 100, 2)).


delta_md_test() ->
  PrevTimestamp = 16#138BDF77CBA,
  DTimestamp    = 270,
  Timestamp     = PrevTimestamp+DTimestamp,

  PrevBid       = [{15.30, 250}, {15.20, 111}],
  PrevAsk       = [{16.73, 15}, {17.00, 90}],
  Scale         = 100,
  DBid          = [{0, -4}, {0, 0}],
  DAsk          = [{-230, 100}, {-100, -47}],
  RealBid       = [{15.30, 246}, {15.20, 111}],
  RealAsk       = [{14.43, 115}, {16.00, 43}],

  PrevMD        = #md{timestamp = PrevTimestamp, bid = PrevBid, ask = PrevAsk},

  Bin = <<0:4,  0:1,1:1, 0:1,0:1, 1:1,1:1, 1:1,1:1,  0:4,   142,2,   124,  154,126, 228,0,  156,127, 81>>,
  BinSize = byte_size(Bin),
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(Bin, secdb_format:encode_delta_md(#md{timestamp = Timestamp, bid = RealBid, ask = RealAsk}, PrevMD, Scale)),
  ?assertEqual(Bin, secdb_format:encode_delta_md(DTimestamp, [DBid, DAsk])),
  ?assertEqual(Bin, secdb_format:encode_delta_md(DTimestamp, DBid, DAsk)),
  ?assertEqual({DTimestamp, DBid, DAsk, byte_size(Bin)}, secdb_format:decode_delta_md(<<Bin/bitstring, Tail/bitstring>>, 2)),
  ?assertEqual({ok, {delta_md, DTimestamp, DBid, DAsk}, byte_size(Bin)}, secdb_format:decode_packet(<<Bin/bitstring, Tail/bitstring>>, 2)),
  ?assertMatch({ok, _MD, BinSize}, secdb_format:decode_packet(<<Bin/binary, Tail/bitstring>>, 2, PrevMD, Scale)),

  {ok, #md{} = MD, _} = secdb_format:decode_packet(<<Bin/binary, Tail/bitstring>>, 2, PrevMD, Scale),
  ?assertEqualMD(#md{timestamp = Timestamp, bid = RealBid, ask = RealAsk}, MD).


trade_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 16#BEEF,
  Scale = 100,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/integer, Volume:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(Bin, secdb_format:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual(Bin, secdb_format:encode_trade(#trade{timestamp = Timestamp, price = Price/Scale, volume = Volume}, Scale)),
  ?assertEqual({Timestamp, Price, Volume, byte_size(Bin)}, secdb_format:decode_trade(<<Bin/binary, Tail/binary>>)),
  ?assertEqual({ok, #trade{timestamp = Timestamp, price = Price, volume = Volume}, byte_size(Bin)},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 42)),
  ?assertEqual({ok, #trade{timestamp = Timestamp, price = Price/Scale, volume = Volume}, byte_size(Bin)},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 42, #md{}, Scale)).

trade_zerovolume_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = 16#DEAD,
  Volume = 0,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/integer, Volume:32/integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(Bin, secdb_format:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, byte_size(Bin)}, secdb_format:decode_trade(<<Bin/binary, Tail/binary>>)),
  ?assertEqual({ok, #trade{timestamp = Timestamp, price = Price, volume = Volume}, byte_size(Bin)},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 42)).

trade_negative_test() ->
  Timestamp = 16#138BDF77CBA,
  Price = -16#DEAD,
  Volume = 16#BEEF,
  Bin = <<16#C0000138BDF77CBA:64/integer, Price:32/signed-integer, Volume:32/unsigned-integer>>,
  Tail = <<7, 239, 183, 19>>,

  ?assertEqual(Bin, secdb_format:encode_trade(Timestamp, Price, Volume)),
  ?assertEqual({Timestamp, Price, Volume, byte_size(Bin)}, secdb_format:decode_trade(<<Bin/binary, Tail/binary>>)),
  ?assertEqual({ok, #trade{timestamp = Timestamp, price = Price, volume = Volume}, byte_size(Bin)},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 42)),
  ?assertEqual({ok, #trade{timestamp = Timestamp, price = Price/100, volume = Volume}, byte_size(Bin)},
    secdb_format:decode_packet(<<Bin/binary, Tail/binary>>, 42, anything, 100)),
  % Negative volume must fail to encode
  ?assertException(error, function_clause, secdb_format:encode_trade(Timestamp, Price, -Volume)).


format_header_value_test() ->
  ?assertEqual("2012-07-19", lists:flatten(secdb_format:format_header_value(date, {2012, 7, 19}))),
  ?assertEqual("MICEX.URKA", lists:flatten(secdb_format:format_header_value(symbol, 'MICEX.URKA'))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(depth, 17))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(scale, 17))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(chunk_size, 17))),
  ?assertEqual("17", lists:flatten(secdb_format:format_header_value(version, 17))).

parse_header_value_test() ->
  ?assertEqual({2012, 7, 19}, secdb_format:parse_header_value(date, "2012/07/19")),
  ?assertEqual({2012, 7, 19}, secdb_format:parse_header_value(date, "2012-07-19")),
  ?assertEqual('MICEX.URKA', secdb_format:parse_header_value(symbol, "MICEX.URKA")),
  ?assertEqual(17, secdb_format:parse_header_value(depth, "17")),
  ?assertEqual(17, secdb_format:parse_header_value(scale, "17")),
  ?assertEqual(17, secdb_format:parse_header_value(chunk_size, "17")),
  ?assertEqual(17, secdb_format:parse_header_value(version, "17")).