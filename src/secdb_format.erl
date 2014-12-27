%%% @doc secdb_format: module that codes and decodes 
%%% actual data to/from binary representation.
%%% Format version: 2
%%% Here "changed" flags for delta fields are aggregated at
%%% packet start to improve code/decode performance by
%%% byte-aligning LEB128 parts

-module(secdb_format).
-author({"Danil Zagoskin", 'z@gosk.in'}).

-include_lib("eunit/include/eunit.hrl").
-include("../include/secdb.hrl").

-on_load(init_nif/0).

-export([encode_full_md/3, decode_full_md/2, test/0]).
-export([encode_delta_md/4, decode_delta_md/3]).
-export([encode_trade/2,    decode_trade/2]).
-export([format_header_value/2, parse_header_value/2]).

-export([decode_packet/2, decode_packet/3]).
-export([get_timestamp/1]).

init_nif() ->
  _Path = filename:dirname(code:which(?MODULE)) ++ "/../priv",
  % Testing: don't load the NIF
  %Load = erlang:load_nif(Path ++ "/secdb_format", 0),
  Load = {error, {testing, skipped}},
  case Load of
    ok -> ok;
    {error, {Reason,Text}} -> io:format("Load secdb_format failed. ~p:~p~n", [Reason, Text])
  end,
  ok.

%% @doc Encode full MD packet with given timestamp and (nested) bid/ask list

-spec encode_full_md(#md{}, Scale::integer(), Depth::integer()) -> binary().
%% @doc Accept #md{} and scale for high-level encoding
encode_full_md(#md{timestamp = Timestamp, bid = Bid, ask = Ask}, Scale, Depth) ->
  {NBids, Bids} = split(Bid, Depth),
  {NAsks, Asks} = split(Ask, Depth),
  <<1:1, 0:1, Timestamp:62/integer, NBids, NAsks,
    (<< <<(scale(P,Scale)):32/integer, V:32/unsigned>> || {P,V} <- Bids>>)/binary,
    (<< <<(scale(P,Scale)):32/integer, V:32/unsigned>> || {P,V} <- Asks>>)/binary>>.

-spec decode_full_md(Buffer::binary(), Scale::integer()) ->
  {#md{}, ByteCount::integer(), Tail::binary()}.
decode_full_md(<<1:1, 0:1, Timestamp:62/integer, BCnt, ACnt, Rest/binary>>, Scale) ->
  BNum = 2*4*BCnt,
  ANum = 2*4*ACnt,
  <<BBids:BNum/binary, BAsks:ANum/binary, Tail/binary>>    = Rest,
  Bids = [{unscale(P, Scale), V} || <<P:32/integer, V:32/unsigned>> <= BBids],
  Asks = [{unscale(P, Scale), V} || <<P:32/integer, V:32/unsigned>> <= BAsks],
  BidAskByteLen = BNum + ANum,
  {#md{timestamp=Timestamp, bid=Bids, ask=Asks}, 8 + 2 + BidAskByteLen, Tail}.

test() ->
  secdb:events('NASDAQ.AAPL', "2012-01-15").
  %secdb_appender_tests:append_verifier_test().

%% @doc Accept new md, previous md, scale for high-level encoding

-spec encode_delta_md(MD::#md{}, LastMD::#md{}, Scale::integer(), Depth::integer()) -> binary().
encode_delta_md(#md{} = MD, #md{} = PrevMD, Scale, Depth) when is_integer(Scale) ->
  {NBids, NAsks, #md{timestamp=TimeDelta, bid=DBid, ask=DAsk}} =
    compute_delta(PrevMD, MD, Scale, Depth),
  TimeDelta >= 0 orelse erlang:error({time_delta_negative,MD#md.timestamp,PrevMD#md.timestamp}),
  encode_delta_md2(TimeDelta, NBids, NAsks, DBid, DAsk).

%% @doc Encode delta MD packet with given timestamp delta and (nested) bid/ask delta list
-spec encode_delta_md2(TimeDelta::integer(), NBids::integer(), NAsks::integer(),
    BidDeltas::[{DPrice::integer(), DVolume::integer()}],
    AskDeltas::[{DPrice::integer(), DVolume::integer()}]) -> binary().
encode_delta_md2(TimeDelta, NBids, NAsks, BidDeltas, AskDeltas)
    when is_integer(TimeDelta), is_list(BidDeltas), is_list(AskDeltas) ->
  BidsMask = << <<(has_value(P,V)):2>> || {P, V} <- BidDeltas>>,
  AsksMask = << <<(has_value(P,V)):2>> || {P, V} <- AskDeltas>>,
  % Calculate mask bit padding.
  BMsSize  = bit_size(BidsMask) + bit_size(AsksMask),
  % Bit mask length is 4*Depth, so we align it to 8 bits
  BMPad    = 8 - (BMsSize band 7),
  <<0:1, 1:1,
    % Encode number of bids/asks
    NBids:7, NAsks:7,
    % Encode bitmasks of non-empty bids/asks
    BidsMask/bitstring, AsksMask/bitstring, 0:BMPad,
    % Encode timestamp
    (leb128:encode(TimeDelta))/binary,
    % Encode bids:
    (<< <<(encode_pv(PV))/binary>> || PV <- BidDeltas >>)/binary,
    % Encode asks:
    (<< <<(encode_pv(PV))/binary>> || PV <- AskDeltas >>)/binary
  >>.

has_value(0,0) -> 0;
has_value(0,_) -> 1;
has_value(_,0) -> 2;
has_value(_,_) -> 3.

encode_pv({0, 0}) -> <<>>;
encode_pv({0, V}) -> leb128:encode_signed(V);
encode_pv({P, 0}) -> leb128:encode_signed(P);
encode_pv({P, V}) -> <<(leb128:encode_signed(P))/binary, (leb128:encode_signed(V))/binary>>.

-spec decode_delta_md(Buffer::binary(), PrevMD::#md{}, Scale::integer()) ->
  {#md{}, ByteCount::integer()}.
decode_delta_md(<<0:1, 1:1, NBids:7, NAsks:7, _/bitstring>> = Bin,
                #md{timestamp = PrevTS, bid = PrevBids, ask = PrevAsks} = _PrevMD,
                Scale) ->
  % Calculate padding size
  BMBidsSz  = NBids*2,
  BMAsksSz  = NAsks*2,
  BMsSize   = BMBidsSz + BMAsksSz,
  BMPadSize = 8 - (BMsSize band 7),
  % Parse packet
  <<_:16, BidBitMask:BMBidsSz, AskBitMask:BMAsksSz, _:BMPadSize, DataTail/binary>> = Bin,
  {TimeDelta, DBA_Tail} = leb128:decode(DataTail),
  {Bid,        DA_Tail} = decode_pvs(DBA_Tail, BidBitMask, PrevBids, Scale, NBids, []),
  {Ask,           Tail} = decode_pvs(DA_Tail,  AskBitMask, PrevAsks, Scale, NAsks, []),
  ByteCount             = byte_size(Bin) - byte_size(Tail),
  {#md{timestamp = PrevTS + TimeDelta, bid = Bid, ask = Ask}, ByteCount}.

decode_pvs(DataBin, BitMask, PrevPVs0, Scale, Len, Acc) when Len < bit_size(BitMask) ->
  <<_:Len/bitstring, PF:1, VF:1, _/binary>> = BitMask,
  {P0,   VData} = get_delta_field(PF, DataBin),
  {V0,    Data} = get_delta_field(VF, VData),
  {PV, PrevPVs} = apply_delta(P0, V0, PrevPVs0, Scale),
  decode_pvs(Data, BitMask, PrevPVs, Scale, Len+2, [PV | Acc]);
decode_pvs(Tail, BitMask, _PrevPVs, _Scale, Len, Acc) when Len =:= bit_size(BitMask) ->
  {lists:reverse(Acc), Tail}.

get_delta_field(0, Data) -> {0, Data};
get_delta_field(1, Data) -> leb128:decode_signed(Data).

%% Utility: apply delta md to previous md (actually, just sum field-by-field)
apply_delta(P, V, [{P0, V0} | Tail] = _PrevPV, Scale) ->
  {{unscale(P+P0, Scale), V+V0}, Tail};
apply_delta(P, V, [], Scale) ->
  {{unscale(P, Scale), V}, []}.

-spec encode_trade(Timestamp::integer(), Pr::integer(), Vol::integer(),
    Aggr::boolean() | undefined) -> binary().
encode_trade(Timestamp, Pr, Vol, Aggr) when is_integer(Pr), is_integer(Vol), Vol >= 0 ->
  <<1:1, 1:1, (encode_aggr(Aggr)):2, Timestamp:60/integer, Pr:32/integer, Vol:32/unsigned>>.

%% @doc higher-level trade encoding
encode_trade(#trade{timestamp = Timestamp, price = P, volume = V, aggressor = Aggr}, Scale) ->
  encode_trade(Timestamp, scale(P, Scale), V, Aggr).

-spec decode_trade(Bin::binary(), Scale::integer()) -> {#trade{}, Size::integer()}.
decode_trade(<<1:1, 1:1, Aggr:2, TS:60/integer, P:32/integer, V:32/unsigned, _/binary>>, Scale) ->
  {#trade{timestamp = TS, price = unscale(P, Scale), volume = V, aggressor = decode_aggr(Aggr)}, 16}.


%% @doc Shorthand for decode_packet/3.
-spec decode_packet(Bin::binary(), Scale::integer()) ->
  {Packet::term(), Size::integer()} | {error, Reason::term()}.
decode_packet(Bin, Scale) ->
  decode_packet(Bin, #md{}, Scale).

%% @doc Main decoding function: takes binary and prev MD and returns packet and its size
-spec decode_packet(Bin::binary(), PrevMD::#md{}, Scale::integer()) ->
  {Packet::term(), Size::integer()} | {error, Reason::term()}.
decode_packet(Bin, PrevMD, Scale) ->
  try
    do_decode_packet(Bin, PrevMD, Scale)
  catch _:Message ->
      {error, Message}
  end.

% Proxy function to route the call either to the C or Erlang implementation
-spec do_decode_packet(Bin::binary(), PrevMD::#md{}, Scale::integer()) ->
  {Packet::#md{} | #trade{}, Size::integer()} | {error, Reason::term()}.
do_decode_packet(Bin, #md{} = PrevMD, Scale)
    when is_binary(Bin), is_integer(Scale) ->
  try
    do_decode_packet_erl(Bin, PrevMD, Scale)
  catch _:Message ->
    {error, Message}
  end.

do_decode_packet_erl(<<1:1, 0:1, _/bitstring>> = Bin, _PrevMD, Scale) ->
  decode_full_md(Bin, Scale);

do_decode_packet_erl(<<0:1, 1:0, _/bitstring>> = Bin, #md{} = PrevMD0, Scale) ->
  decode_delta_md(Bin, PrevMD0, Scale);

do_decode_packet_erl(<<1:1, 1:1, _/bitstring>> = Bin, _PrevMD, Scale) ->
  decode_trade(Bin, Scale).

%% Utility: scale bid/ask when serializing
scale(Price,       0)  -> Price;
scale(Price,   Scale)  -> round(Price*Scale).

%% Utility: unscale bid/ask when deserializing
unscale(Price,     0)  -> Price;
unscale(Price, Scale)  -> Price / Scale.

encode_aggr(undefined) -> 0;
encode_aggr(false)     -> 1;
encode_aggr(true)      -> 2.

decode_aggr(0)         -> 0;
decode_aggr(1)         -> 1;
decode_aggr(2)         -> 2.

%% Utility: get delta md where first argument is old value, second is new one
compute_delta(_Prev = #md{timestamp=TS1, bid=B1, ask=A1},
              _This = #md{timestamp=TS2, bid=B2, ask=A2}, PxScale, Depth)
    when is_integer(PxScale), is_integer(Depth) ->
  {NB, Bids} = delta(B1, B2, PxScale, Depth, 0, []),
  {NA, Asks} = delta(A1, A2, PxScale, Depth, 0, []),
  {NB, NA, #md{timestamp = TS2 - TS1, bid = Bids, ask = Asks}}.

%% @doc Count delta when going from OldQuote1 to NewQuote2 -> X = X2-X1, and truncate the
%% list of quotes to N elements.
%% If OldQuote's depth is greater than the NewQuote's - stop at NewQuote's depth
%% If OldQuote's depth is less than the NewQuote's - set X = X2-X1 for {Price,Value} pairs
%% of OldQuote's depth, and X = X2 for the remaining ones.
delta(_, _, _PxScale, N, N, Acc) ->
  {N, lists:reverse(Acc)};
delta([{P1,V1}|T1], [{P2,V2}|T2], PxScale, N, I, Acc) ->
  delta(T1, T2, PxScale, N, I+1, [{scale(P2-P1, PxScale), V2-V1} | Acc]);
delta([], [{P2,V2}|T2], PxScale, N, I, Acc) ->
  delta2(T2, PxScale, N, I+1, [{scale(P2, PxScale), V2} | Acc]);
delta(_,  [], _PxScale, _, I, Acc) ->
  {I, lists:reverse(Acc)}.

delta2([{P,V}|T], Scale, N, I, Acc) when N =/= I ->
  delta2(T, Scale, N, I+1, [{scale(P,Scale),V}|Acc]);
delta2(_,        _Scale, _, I, Acc) ->
  {I, lists:reverse(Acc)}.

split(L, N) when is_list(L), is_integer(N) ->
  split(L, N, 0, []).

split([H|_], N, N, Acc) -> {N, lists:reverse([H|Acc])};
split([],    _, I, Acc) -> {I, lists:reverse(Acc)};
split([H|T], N, I, Acc) -> split(T, N, I+1, [H|Acc]).

get_timestamp(<<1:1, _:3, Timestamp:60/integer, _/binary>>) ->
  Timestamp.

%% @doc serialize header value, used when writing header
format_header_value(date, {_,_,_} = Date) -> secdb_fs:date_to_list(Date);
format_header_value(symbol, Symbol)       -> atom_to_list(Symbol);
format_header_value(_, Value)             -> io_lib:print(Value).


%% @doc deserialize header value, used when parsing header
parse_header_value(depth,       Value)      -> list_to_integer(Value);
parse_header_value(scale,       Value)      -> list_to_integer(Value);
parse_header_value(chunk_size,  Value)      -> list_to_integer(Value);
parse_header_value(version,     Value)      -> list_to_integer(Value);
parse_header_value(have_candle, "true")     -> true;
parse_header_value(have_candle, "false")    -> false;
parse_header_value(date,        DateStr)    -> secdb_fs:parse_date(DateStr);
parse_header_value(symbol,      SymbolStr)  -> list_to_atom(SymbolStr);
parse_header_value(_,           Value)      -> Value.