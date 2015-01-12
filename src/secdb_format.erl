%%% @doc secdb_format: module that codes and decodes 
%%% actual data to/from binary representation.
%%% Format version: 2
%%% Here "changed" flags for delta fields are aggregated at
%%% packet start to improve code/decode performance by
%%% byte-aligning LEB128 parts

-module(secdb_format).
-author('z@gosk.in').
-author('saleyn@gmail.com').

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("../include/secdb.hrl").
-include("secdb.hrl").
-include("log.hrl").

-on_load(init_nif/0).
-export([do_decode_packet/5, do_decode_packet/6, do_leb128_decode/3]).

-export([encode/3, decode/5]).

-export([decode_packet/3, one_quote/1]).
-export([format_header_value/2, parse_header_value/2, get_timestamp/1]).
-export([test0/0]).

init_nif() ->
  Lib  = "priv/secdb_format",
  Name = filename:join(filename:dirname(filename:dirname(code:which(?MODULE))), Lib),
  case erlang:load_nif(Name, 0) of
    ok ->
      ok;
    {error, {Reason,Text}} ->
      error_logger:error_msg("Load secdb_format failed. ~p:~p~n", [Reason, Text])
  end.

-spec encode(full | delta, #md{}, #db{}) -> {binary(), #db{}}.
%% @doc Perform full or delta MD or Trade encoding
encode(full, #md{timestamp=TS, bid=AllBids, ask=AllAsks}, #db{scale=S, depth=D} = DB) ->
  {NBids, Bids} = unscale_split(AllBids, D, S),
  {NAsks, Asks} = unscale_split(AllAsks, D, S),
  Bin =
  <<0:1, 1:1,               % FullMD Indicator
    NBids:7, NAsks:7,       % Number of Bids/Asks
    TS:64/unsigned,         % Timestamp
    (<< <<(leb128eu(P))/binary, (leb128eu(V))/binary>> || {P,V} <- Bids>>)/binary,
    (<< <<(leb128eu(P))/binary, (leb128eu(V))/binary>> || {P,V} <- Asks>>)/binary
  >>,
  {Bin, DB#db{last_ts = TS, last_quote = one_quote(Bids, Asks)}};
encode(delta, #md{} = MD, #db{} = DB) ->
  {NBids, NAsks, TimeDelta, BidDeltas, AskDeltas, NewDB} = compute_delta(MD, DB),
  BidsMask   = << <<(has_value(P,V)):2>> || {P, V} <- BidDeltas>>,
  AsksMask   = << <<(has_value(P,V)):2>> || {P, V} <- AskDeltas>>,
  % Calculate mask bit padding.
  % Bit mask length is a multiple of 4*Depth, so we align it to 8 bits (band 7 returns
  % either 0 or 4:
  BMLen = bit_size(BidsMask) + bit_size(AsksMask),
  BMPad = BMLen band 7,
  Bin =
  <<0:1, 0:1,                               % DeltaMD indicator
    NBids:7, NAsks:7,                       % Number of bids/asks
    BidsMask/bitstring, AsksMask/bitstring, % Bitmasks of non-empty bids/asks
    0:BMPad,                                % Padding
    (leb128es(TimeDelta))/binary,           % Timestamp
    (<< <<(encode_pv(PV))/binary>> || PV <- BidDeltas >>)/binary, % Bids
    (<< <<(encode_pv(PV))/binary>> || PV <- AskDeltas >>)/binary  % Asks
  >>,
  {Bin, NewDB};

encode(full,
    #trade{timestamp=TS, side=Side, price=FP, volume=V, aggressor=Aggr}, #db{scale=S} = DB)
  when is_number(FP), is_integer(V), V >= 0 ->
  P   = scale(FP, S),
  Bin =
  <<1:1, 1:1,                 % Trade (full) indicator
    (encode_aggr(Aggr)):2,    % Aggressor indicator (0=undef, 1=passive, 2=aggressor)
    (encode_side(Side)):1,    % Side (0=buy, 1=sell)
    0:11,                     % Filler
    TS:64/unsigned, (leb128eu(P))/binary, (leb128eu(V))/binary
  >>,
  {Bin, DB#db{last_ts=TS, last_quote = {P,V}}};
encode(delta,
    #trade{timestamp=TS, side=Side, price=FP, volume=V, aggressor=Aggr},
    #db{last_ts=LastTS, last_quote={P0,V0}, scale=S} = DB)
  when is_integer(LastTS), is_number(FP), is_integer(V), V >= 0, is_integer(S) ->
  P   = scale(FP,S),
  DT  = TS - LastTS,
  DP  = P  - P0,
  DV  = V  - V0,
  Bin =
  <<1:1,0:1,                  % Trade (delta) indicator
    (encode_aggr(Aggr)):2,    % Aggressor indicator (0=undef, 1=passive, 2=aggressor)
    (encode_side(Side)):1,    % Side (0=buy, 1=sell)
    0:3,                      % Filler
    (leb128es(DT))/binary,    % DeltaTimestamp
    (leb128es(DP))/binary, (leb128es(DV))/binary  % DeltaPrice, DeltaVol
  >>,
  {Bin, DB#db{last_ts=TS, last_quote={P,V}}}.


has_value(0,0)    -> 0;
has_value(0,_)    -> 1;
has_value(_,0)    -> 2;
has_value(_,_)    -> 3.

encode_pv({0, 0}) -> <<>>;
encode_pv({0, V}) -> leb128es(V);
encode_pv({P, 0}) -> leb128es(P);
encode_pv({P, V}) -> <<(leb128es(P))/binary, (leb128es(V))/binary>>.


%% @doc Main decoding function: takes binary and prev MD and returns packet and its size
-spec decode_packet(Bin::binary(), Offset::integer(), #db{}) ->
  {full | delta, Packet::#md{} | #trade{}, NewOffset::integer(), NewDB::#db{}} |
  {error, Reason::term()}.
decode_packet(Bin, Offset, #db{scale=S, last_ts=LastTS, last_quote=LastQ} = DB)
  when is_binary(Bin), is_integer(Offset) ->
  try
    {Mode, Packet, Offset1, TS, Quote} = do_decode_packet(Bin, Offset, S, LastTS, LastQ),
    ?DBG("Decoded ~-5w pkt at ~w len ~w: ~w\n~*s~w (~w)",
      [Mode, Offset, Offset1 - Offset,
       binary:part(Bin, Offset, Offset1 - Offset), 35, " ", Packet, Quote]),
    {Mode, Packet, Offset1, DB#db{last_ts = TS, last_quote = Quote}}
  catch _:Message ->
    {error, Message}
  end.

%% @doc Proxy function to route the call either to the C++ or Erlang implementation
-spec do_decode_packet(Bin::binary(), Offset::integer(), Scale::integer(),
    LastTS::integer(), LastQuote :: {Price::integer(), Qty::integer()}) ->
  {full | delta, Pkt::#md{} | #trade{}, Offset::integer(), TS::integer(),
    Quote::{Price::integer(), Qty::integer()}} |
  {error, Reason::term()}.
do_decode_packet(Bin, Offset, Scale, LastTS, LastQuote)
  when is_binary(Bin), is_integer(Scale), is_integer(LastTS) ->
  try
    decode(Bin, Offset, Scale, LastTS, LastQuote)
  catch _:Message ->
    {error, Message}
  end.

%% @doc Proxy function to route the call to C++ implementation
-spec do_decode_packet(Bin::binary(), Offset::integer(), Scale::integer(),
    LastTS::integer(), LastQuote :: {Price::integer(), Qty::integer()}, Verbose::integer()) ->
  {full | delta, Pkt::#md{} | #trade{}, Offset::integer(), TS::integer(),
    Quote::{Price::integer(), Qty::integer()}} |
  {error, Reason::term()}.
do_decode_packet(Bin, _Offset, Scale, LastTS, _LastQuote, Verbose)
  when is_binary(Bin), is_integer(Scale), is_integer(LastTS), is_integer(Verbose) ->
  throw({not_implemented, {do_decode_packet, 6}}).

%% @doc Proxy function to route the call either to the C++ or Erlang implementation
do_leb128_decode(Bin, Signed, Pos) when is_binary(Bin), is_atom(Signed), is_integer(Pos) ->
  leb128:decode(Bin, Signed, Pos).

-spec decode(Buffer::binary(), Offset::integer(), Scale::integer(), LastTS::integer(),
    LastQuote::{integer(), integer()}) ->
  {ok, #md{} | #trade{}, integer(), TS::integer(), Quote::{Price::integer(), Qty::integer()}}.
%% @doc Decode MD or Trade packet (either full or delta).
decode(Bin, Offset, Scale, LastTS, LastQuote) ->
  {_, Data} = split_binary(Bin, Offset),
  % Note: the code below was optimized to avoid creation of sub-binaries and
  % extrateous matching contexts (see: "erlc +bin_opt_info"):
  case Data of
    %---------------------------------------------------------------------------
    % Full MD
    %---------------------------------------------------------------------------
    <<0:1, 1:1, NBids:7, NAsks:7, TS:64/unsigned, _/binary>> ->
      {Bids, NewQB, Off0} = decode_full_quotes(Bin, Offset+8+2, NBids, Scale),
      {Asks,_NewQA, Off1} = decode_full_quotes(Bin, Off0,       NAsks, Scale),
      {full, #md{timestamp=TS, bid=Bids, ask=Asks}, Off1, TS, NewQB};
    %---------------------------------------------------------------------------
    % Delta MD
    %---------------------------------------------------------------------------
    <<0:1, 0:1, NBids:7, NAsks:7, _/binary>> ->
      BMBidsSz  = NBids*2,
      BMAsksSz  = NAsks*2,
      % Calculate padding size
      BMsSize   = BMBidsSz + BMAsksSz,
      % BMsSize is a multiple of 4, so BMPadSize is 0 or 4:
      BMPadSize = BMsSize rem 8,
      % Parse packet (the code below was optimized to remove sub-binary creation)
      Pos = 2 + (BMsSize + BMPadSize) div 8,
      {<<_:16, BidBM:BMBidsSz/bitstring, AskBM:BMAsksSz/bitstring, _:BMPadSize>>, Rest} =
        split_binary(Data, Pos),
      {TimeDelta, Off1} = leb128ds(Rest, 0),
      {Bid, NewQB,Off2} = decode_delta_quotes(Rest, Off1, BidBM, LastQuote, Scale),
      {Ask,_NewQA,Off3} = decode_delta_quotes(Rest, Off2, AskBM, LastQuote, Scale),
      NewOff            = Offset + Pos + Off3,
      TS                = LastTS+TimeDelta,
      {delta, #md{timestamp=TS, bid=Bid, ask=Ask}, NewOff, TS, NewQB};
    %---------------------------------------------------------------------------
    % Full trade
    %---------------------------------------------------------------------------
    <<1:1,1:1, Ag:2,Sd:1,0:3, _:8, TS:64/unsigned, _/binary>> ->
      Aggr        = decode_aggr(Ag),
      Side        = decode_side(Sd),
      {P,V, Off1} = decode_full_quote(Data, 10),
      FP          = unscale(P, Scale),
      Trade       = #trade{timestamp=TS, side=Side, price=FP, volume=V, aggressor= Aggr},
      {full, Trade, Offset+Off1, TS, {P,V}};
    %---------------------------------------------------------------------------
    % Delta trade
    %---------------------------------------------------------------------------
    <<1:1,0:1, Ag:2,Sd:1,0:3, _/binary>> ->
      Aggr         = decode_aggr(Ag),
      Side         = decode_side(Sd),
      {DltTS,Off0} = leb128ds(Data, 1),
      {P1,   Off1} = leb128ds(Data, Off0),
      {V1,   Off2} = leb128ds(Data, Off1),
      TS           = LastTS + DltTS,
      {P0,V0}      = LastQuote,
      {P,V}        = {P1+P0, V1+V0},
      FP           = unscale(P, Scale),
      Trade        = #trade{timestamp=TS, side=Side, price=FP, volume=V, aggressor=Aggr},
      {delta, Trade, Offset+Off2, TS, {P,V}}
  end.


decode_full_quotes(Bin, Offset, N, Scale) when N =/= 0 ->
  {P, V, NewOffset} = decode_full_quote(Bin, Offset),
  decode_full_quotes2(Bin, NewOffset, N-1, [{unscale(P, Scale), V}], {P,V}, Scale);
decode_full_quotes(Bin, Offset, N, Scale) ->
  decode_full_quotes2(Bin, Offset, N, [], {0,0}, Scale).

decode_full_quotes2(_Bin, Offset, 0, Acc, L1quote, _Scale) ->
  {lists:reverse(Acc), L1quote, Offset};
decode_full_quotes2(Bin, Offset, N, Acc, L1quote, Scale) ->
  {P,V, NewOffset} = decode_full_quote(Bin, Offset),
  decode_full_quotes2(Bin, NewOffset, N-1, [{unscale(P, Scale), V} | Acc], L1quote, Scale).

decode_full_quote(Bin, Offset) ->
  {P, Skip0} = leb128du(Bin, Offset),
  {V, Skip1} = leb128du(Bin, Skip0),
  {P, V, Skip1}.

decode_delta_quotes(Bin, Offset, <<PF:1, VF:1, T/bitstring>>, {P0,V0}, Scale) ->
  {P1, Off0} = get_delta_field(PF, Bin, Offset),
  {V1, Off1} = get_delta_field(VF, Bin, Off0),
  {P,V} = L1 = {P1+P0, V1+V0},
  decode_delta_quotes2(Bin, Off1, T, L1, Scale, [{unscale(P,Scale),V}], L1);
decode_delta_quotes(_Bin, Offset, <<>>, L1Quote, _Scale) ->
  {[], L1Quote, Offset}.

decode_delta_quotes2(Bin, Offset, <<PF:1, VF:1, T/bitstring>>, {P0,V0}, Scale, Acc, L1Q) ->
  {P1, Off0} = get_delta_field(PF, Bin, Offset),
  {V1, Off1} = get_delta_field(VF, Bin, Off0),
  {P,V} = PV = {P1+P0, V1+V0},
  case Acc of
    [] -> decode_delta_quotes2(Bin, Off1, T, PV, Scale, [{unscale(P,Scale),V} | Acc], L1Q);
    _  -> decode_delta_quotes2(Bin, Off1, T, PV, Scale, [{unscale(P,Scale),V} | Acc], L1Q)
  end;
decode_delta_quotes2(_Bin, Offset, <<>>, _PrevQuote, _Scale, Acc, L1Quote) ->
  {lists:reverse(Acc), L1Quote, Offset}.

get_delta_field(0,_Bin, Offset) -> {0, Offset};
get_delta_field(1, Bin, Offset) -> leb128ds(Bin, Offset).

leb128eu(V) when V >= 0   -> leb128:encode(V).
leb128es(V)               -> leb128:encode_signed(V).
leb128du(Bin, Offset)     -> do_leb128_decode(Bin, unsigned, Offset).
leb128ds(Bin, Offset)     -> do_leb128_decode(Bin, signed,   Offset).

%% apply_delta(P, V, [{P0, V0} | Tail] = _PrevPV, Scale) ->
%%   {{unscale(P+P0, Scale), V+V0}, Tail};
%% apply_delta(P, V, [], Scale) ->
%%   {{unscale(P, Scale), V}, []}.

one_quote([H|_])          -> H;
one_quote([])             -> {0, 0}.

one_quote([H|_],_)        -> H;
one_quote(_,[H|_])        -> H;
one_quote(_,_)            -> {0, 0}.

%% Utility: scale bid/ask when serializing
scale(Price,       0) when is_integer(Price) -> Price;
scale(Price,       0) when is_float(Price)   -> round(Price);
scale(Price,   Scale)                        -> round(Price*Scale).

%% Utility: unscale bid/ask when deserializing
unscale(Price,     0)     -> Price;
unscale(Price, Scale)     -> Price / Scale.

encode_aggr(undefined)    -> 0;
encode_aggr(false)        -> 1;
encode_aggr(true)         -> 2.

decode_aggr(0)            -> undefined;
decode_aggr(1)            -> false;
decode_aggr(2)            -> true.

encode_side(buy)          -> 0;
encode_side(sell)         -> 1.

decode_side(0)            -> buy;
decode_side(1)            -> sell.

%% Utility: get delta md where first argument is old value, second is new one
compute_delta(#md{timestamp=TS2, bid=B2, ask=A2},
              #db{last_ts=TS1, last_quote=L1Q, scale=Scale, depth=Depth} = DB)
    when is_tuple(L1Q), is_integer(Scale), is_integer(Depth) ->
  {NB, Bids, NewL1Q} = delta(L1Q, B2, Scale, Depth),
  {NA, Asks,_NewL1Q} = delta(L1Q, A2, Scale, Depth),
  TimeDelta  = TS2 - TS1,
  TimeDelta >= 0 orelse erlang:error({time_delta_negative, TS2, TS1}),
  {NB, NA, TS2 - TS1, Bids, Asks, DB#db{last_ts=TS2, last_quote= NewL1Q}}.

%% @doc Count delta when going from OldQuote to NewQuote.
%% The Level1 gets set to the delta of {price,qty} between OldQuote and NewQuote.
%% The subsequent levels are set to the price (or qty) difference between prior level
%% and current level. The result has the length of the NewQuote list.
delta({P0,V0}, [{P1,V1}|T], Scale, Depth) when Depth > 0 ->
  P = scale(P1, Scale),
  delta2({P,V1}, T, Scale, Depth, 1, [{P-P0, V1-V0}], {P,V1});
delta(L1, Quotes, _, Depth) when Quotes =:= []; Depth =:= 0 ->
  {0, [], L1}.

delta2(_, _, _Scale, N, N, Acc, FirstQ) ->
  {N, lists:reverse(Acc), FirstQ};
delta2({P1,V1}, [{P2,V2}|T2], Scale, N, I, Acc, FirstQ) ->
  P = scale(P2, Scale),
  delta2({P,V2}, T2, Scale, N, I+1, [{P-P1, V2-V1} | Acc], FirstQ);
delta2([], [{P2,V2}|T2], Scale, N, I, Acc, FirstQ) ->
  P = scale(P2, Scale),
  delta2({P,V2}, T2, Scale, N, I+1, [{P, V2} | Acc], FirstQ);
delta2(_,  [], _Scale, _, I, Acc, FirstQ) ->
  {I, lists:reverse(Acc), FirstQ}.

unscale_split(L, N, Scale) when is_list(L), is_integer(N), is_integer(Scale) ->
  split(L, N, 1, [], Scale).

split([{P,V}|_], N, N, Acc, Scale) -> {N, lists:reverse([{scale(P,Scale),V}|Acc])};
split([],        _, I, Acc,_Scale) -> {I, lists:reverse(Acc)};
split([{P,V}|T], N, I, Acc, Scale) -> split(T, N, I+1, [{scale(P,Scale),V}|Acc], Scale).

% This is Full MD or Trade event
get_timestamp(<<_:1, 1:1, _:14, Timestamp:64/unsigned, _/binary>>) ->
  Timestamp.

%% @doc serialize header value, used when writing header
format_header_value(date, {_,_,_} = Date) -> secdb_fs:date_to_list(Date);
format_header_value(symbol, Symbol)       -> atom_to_list(Symbol);
format_header_value(_, Value)             -> io_lib:print(Value).


%% @doc deserialize header value, used when parsing header
parse_header_value(depth,     Value)      -> list_to_integer(Value);
parse_header_value(scale,     Value)      -> list_to_integer(Value);
parse_header_value(window_sec,  Value)      -> list_to_integer(Value);
parse_header_value(version,   Value)      -> list_to_integer(Value);
parse_header_value(date,      DateStr)    -> secdb_fs:parse_date(DateStr);
parse_header_value(symbol,    SymbolStr)  -> list_to_atom(SymbolStr);
parse_header_value(_,         Value)      -> Value.

test0() ->
  secdb:events('NASDAQ.AAPL', "2012-01-15").
%secdb_appender_tests:append_verifier_test().

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------
-ifdef(EUNIT).

delta_test() ->
  ?assertEqual(
    {2,[{2,3},{3,3}]},
    secdb_format:delta({10,2}, [{12,5},{15,8}], 100, 2)
  ),
  ?assertEqual(
    {2,[{2,3},{3,3}]},
    secdb_format:delta({10,2}, [{12,5},{15,8}], 100, 2)
  ),
  ?assertEqual(
    {2,[{2,3},{3,5}]},
    secdb_format:delta({10,2}, [{12,5},{15,10}], 100, 2)
  ),
  ?assertEqual(
    {2,[{12,5},{3,5}]},
    secdb_format:delta([], [{12,5},{15,10}], 100, 2)
  ),
  ?assertEqual(
    {1,[{2,3}]},
    secdb_format:delta({10,2}, [{12,5}], 100, 2)
  ).
-endif.