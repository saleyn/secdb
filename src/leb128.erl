%%% @doc LEB128 (http://en.wikipedia.org/wiki/LEB128) implementation
%%% Only unsigned integers re supported ATM

-module(leb128).
-author('z@gosk.in').
-author('saleyn@gmail.com').

-export([encode/1, decode/1, decode/2, decode/3]).
-export([encode_signed/1, decode_signed/1, decode_signed/2]).

-ifdef(TEST).
-export([profile/0]).
-endif.

-include("log.hrl").

-spec encode_signed(integer()) -> bitstring().
encode_signed(Value) when is_integer(Value)  ->
  encode_ranged(Value, 0, 16#40).

-spec encode(non_neg_integer()) -> binary().
encode(Value) when is_integer(Value), Value >= 0 ->
  encode1(Value).


encode1(V) when is_integer(V), V >= 0,           V < 16#80 ->
  <<0:1, V:7/integer>>;

encode1(V) when is_integer(V), V >= 16#80,       V < 16#4000 ->
  <<1:1, V:7/integer, 0:1, (V bsr 7):7/integer>>;

encode1(V) when is_integer(V), V >= 16#4000,     V < 16#200000 ->
  <<1:1, V:7/integer, 1:1, (V bsr 7):7/integer, 0:1, (V bsr 14):7/integer>>;

encode1(V) when is_integer(V), V >= 16#200000,   V < 16#10000000 ->
  V1 = V bsr 7,
  V2 = V bsr 14,
  V3 = V bsr 21,
  <<1:1,  V:7/integer, 1:1, V1:7/integer, 1:1, V2:7/integer, 0:1, V3:7/integer>>;

encode1(V) when is_integer(V), V >= 16#10000000, V < 16#800000000 ->
  V1 = V bsr 7,
  V2 = V bsr 14,
  V3 = V bsr 21,
  V4 = V bsr 28,
  <<1:1,  V:7/integer, 1:1, V1:7/integer, 1:1, V2:7/integer, 1:1, V3:7/integer,
  0:1, V4:7/integer>>;

encode1(V) when is_integer(V), V >= 16#800000000, V < 16#40000000000 ->
  V1 = V bsr 7,
  V2 = V bsr 14,
  V3 = V bsr 21,
  V4 = V bsr 28,
  V5 = V bsr 35,
  <<1:1,  V:7/integer, 1:1, V1:7/integer, 1:1, V2:7/integer, 1:1, V3:7/integer,
  1:1, V4:7/integer, 0:1, V5:7/integer>>;

encode1(V) when is_integer(V), V >= 16#40000000000, V < 16#2000000000000 ->
  V1 = V bsr 7,
  V2 = V bsr 14,
  V3 = V bsr 21,
  V4 = V bsr 28,
  V5 = V bsr 35,
  V6 = V bsr 42,
  <<1:1,  V:7/integer, 1:1, V1:7/integer, 1:1, V2:7/integer, 1:1, V3:7/integer,
  1:1, V4:7/integer, 1:1, V5:7/integer, 0:1, V6:7/integer>>;

encode1(V) ->
  encode_ranged(V, 0, 16#80).


encode_ranged(Value, Shift, Range) when -Range =< Value, Value < Range ->
  Chunk = Value bsr Shift,
  <<0:1, Chunk:7/integer>>;

encode_ranged(Value, Shift, Range) ->
  Chunk = Value bsr Shift,
  Tail = encode_ranged(Value, Shift+7, Range bsl 7),
  <<1:1, Chunk:7/integer, Tail/binary>>.

-spec decode(binary()) -> {integer(), binary()}.
decode(Bin) when is_binary(Bin) ->
  {Value, Offset} = decode(Bin, unsigned, 0),
  {Value, element(2, split_binary(Bin, Offset))}.

-spec decode_signed(binary()) -> {integer(), binary()}.
decode_signed(Bin) when is_binary(Bin) ->
  {Value, Offset} = decode(Bin, signed, 0),
  {Value, element(2, split_binary(Bin, Offset))}.

-spec decode(binary(), integer()) -> {integer(), integer()}.
decode(Bin, Offset) when is_binary(Bin), is_integer(Offset) ->
  decode(Bin, unsigned, Offset).

-spec decode_signed(binary(), integer()) -> {integer(), integer()}.
decode_signed(Bin, SkipLen) when is_binary(Bin), is_integer(SkipLen) ->
  decode(Bin, signed, SkipLen).


decode(Bin, signed, Offset) ->
  {Size, UValue, Offset1} = take_chunks(Bin, 0, 0, Offset),
  <<Value:Size/signed-integer>> = <<UValue:Size/unsigned-integer>>,
  %?DBG("Decode int  ~w: ~w", [binary:part(Bin, Offset, Offset1 - Offset), Value]),
  {Value, Offset1};

decode(Bin, unsigned, Offset) ->
  {_Size, Value, Offset1} = take_chunks(Bin, 0, 0, Offset),
  %?DBG("Decode uint ~w: ~w", [binary:part(Bin, Offset, Offset1 - Offset), Value]),
  {Value, Offset1}.

take_chunks(Bin, Acc, Shift, Skip) ->
  % Note: the code below is optimized to avoid creation of sub-binary
  % (use "erlc +bin_opt_info" to verify)
  I = binary:at(Bin, Skip),
  case (I band 16#80) of
    0 ->
      Result  = (I bsl Shift) bor Acc,
      {Shift+7, Result, Skip+1};
    16#80 ->
      NextAcc = ((I band 16#7F) bsl Shift) bor Acc,
      take_chunks(Bin, NextAcc, Shift+7, Skip+1)
  end.


% EUnit tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

encode_test() ->
  ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode(624485)).

encode1_test() ->
  ?assertEqual(<<16#E5, 16#8E, 16#26>>, encode(624485)).

decode_test() ->
  ?assertEqual({624485,<<57,113,1>>}, decode(<<16#E5, 16#8E, 16#26, 57,113,1>>)).

encode_signed_test() ->
  ?assertError(function_clause, encode(-1)),
  ?assertEqual(<<16#9B, 16#F1, 16#59>>, encode_signed(-624485)).

decode_signed_test() ->
  ?assertEqual({-624485,<<57,113,1>>}, decode_signed(<<16#9B, 16#F1, 16#59, 57,113,1>>)).


measure(F) ->
  T1 = erlang:now(),
  Res = F(),
  T2 = erlang:now(),
  {timer:now_diff(T2,T1), Res}.

profile() ->
  Count = 100000,
  List0 = [A || <<A:32>> <= crypto:rand_bytes(Count*3)],
  {DeltaE,  List1_} = measure(fun() -> [encode(A)  || A <- List0] end),
  {DeltaE1, List1_} = measure(fun() -> [encode1(A) || A <- List0] end),

  List1 = [<<A/binary, (crypto:rand_bytes(40))/binary>> || A <- List1_],

  {DeltaD1, List2} = measure(fun() -> [decode(B) || B <- List1] end),
  {DeltaD2,_ListX} = measure(fun() -> [secdb_format:do_leb128_decode(B, unsigned, 0) || B <- List1] end),

  List0 = [A || {A,_} <- List2],
  % List1 = [A || {A,<<>>} <- List4],
  
  {enc, [DeltaE / Count, DeltaE1 / Count],  dec, [DeltaD1 / Count, DeltaD2 / Count]}.

-endif.