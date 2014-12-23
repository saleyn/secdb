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

-export([encode_full_md/2,  decode_full_md/1]).
-export([encode_delta_md/3, decode_delta_md/2]).
-export([encode_trade/2, encode_trade/3, decode_trade/1]).
-export([format_header_value/2, parse_header_value/2]).

-export([decode_packet/2, decode_packet/4, decode_packet/5]).
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

-spec encode_full_md(Timestamp::integer(), BidAsk::[{Price::integer(), Volume::integer()}]) -> binary().
%% @doc Accept #md{} and scale for high-level encoding
encode_full_md(#md{} = MD, 0)     -> encode_full_md2(MD, fun(P) -> P end);
encode_full_md(#md{} = MD, Scale) -> encode_full_md2(MD, fun(P) -> round(Scale*P) end).

encode_full_md2(#md{timestamp = Timestamp, bid = Bid, ask = Ask}, Fun) ->
  <<1:1, 0:1, Timestamp:62/integer, (length(Bid)), (length(Ask)),
    (<< <<(Fun(P)):32/integer, V:32/unsigned>> || {P,V} <- Bid>>)/binary,
    (<< <<(Fun(P)):32/integer, V:32/unsigned>> || {P,V} <- Ask>>)/binary>>.

-spec decode_full_md(Buffer::binary()) ->
  {Timestamp::integer(),
    Bids :: [{Price::integer(), Volume::integer()}],
    Asks :: [{Price::integer(), Volume::integer()}], ByteCount::integer(), Tail::binary()}.
decode_full_md(<<1:1, 0:1, Timestamp:62/integer, BCnt, ACnt, Rest/binary>>) ->
  BNum = 2*4*BCnt,
  ANum = 2*4*ACnt,
  <<BBids:BNum/binary, BAsks:ANum/binary, Tail/binary>>    = Rest,
  Bids = [{Px, Vol} || <<Px:32/integer, Vol:32/unsigned>> <= BBids, not (Px =:= 0 andalso Vol =:= 0)],
  Asks = [{Px, Vol} || <<Px:32/integer, Vol:32/unsigned>> <= BAsks, not (Px =:= 0 andalso Vol =:= 0)],
  BidAskByteLen = BNum + ANum,
  {Timestamp, Bids, Asks, 8 + 2*BidAskByteLen, Tail}.

%% @doc Encode delta MD packet with given timestamp delta and (nested) bid/ask delta list
-spec encode_delta_md(TimeDelta::integer(),
    BidDeltas::[{DPrice::integer(), DVolume::integer()}],
    AskDeltas::[{DPrice::integer(), DVolume::integer()}]) -> binary().
encode_delta_md(TimeDelta, BidDeltas, AskDeltas) ->
  BidsMask = << <<(has_value(P)):1, (has_value(V)):1>> || {P, V} <- BidDeltas>>,
  AsksMask = << <<(has_value(P)):1, (has_value(V)):1>> || {P, V} <- AskDeltas>>,
  % Calculate mask bit padding
  BMSize   = bit_size(BidsMask) + bit_size(AsksMask),
  % Bit mask length is 4*Depth, so wee can align it to 4 bits, leaving extra space for future
  BMPad    = 8 - (BMSize rem 8) - 4,
  <<0:4/integer, BidsMask/bitstring, AsksMask/bitstring, 0:BMPad,
    % Encode timestamp
    (leb128:encode(TimeDelta))/binary,
    % Encode bids:
    (<< <<(encode_pv(PV))/binary>> || PV <- BidDeltas >>)/binary,
    % Encode asks:
    (<< <<(encode_pv(PV))/binary>> || PV <- AskDeltas >>)/binary
  >>.

has_value(0) -> 0;
has_value(_) -> 1.

encode_pv({0, 0}) -> <<>>;
encode_pv({0, V}) -> leb128:encode_signed(V);
encode_pv({P, 0}) -> leb128:encode_signed(P);
encode_pv({P, V}) -> <<(leb128:encode_signed(P))/binary, (leb128:encode_signed(V))/binary>>.

-spec decode_delta_md(Buffer::binary(), Depth::integer()) ->
  {TimeDelta::integer(),
    BidDelta::[{DPrice::integer(), DVolume::integer()}],
    AskDelta::[{DPrice::integer(), DVolume::integer()}], ByteCount::integer()}.
decode_delta_md(<<0:1, _/bitstring>> = Bin, Depth) ->
  % Calculate bitmask size
  HalfBMSize = Depth * 2,
  % Actually, Size - 4, but it will fail with zero depth
  BMPadSize = (2*HalfBMSize + 4) rem 8,
  % Parse packet
  <<_:4, BidBitMask:HalfBMSize/bitstring, AskBitMask:HalfBMSize/bitstring, _:BMPadSize/bitstring, DataTail/binary>> = Bin,
  {TimeDelta, DBA_Tail} = leb128:decode(DataTail),
  {DBid,       DA_Tail} = decode_pvs(DBA_Tail, BidBitMask),
  {DAsk,          Tail} = decode_pvs(DA_Tail,  AskBitMask),
  ByteCount             = byte_size(Bin) - byte_size(Tail),
  {TimeDelta, DBid, DAsk, ByteCount}.

decode_pvs(Bin, BitMask) ->
  decode_pvs(Bin, BitMask, 0, []).

decode_pvs(DataBin, BitMask, Len, Acc) when Len < bit_size(BitMask) ->
  <<_:Len/bitstring, PF:1, VF:1, _/binary>> = BitMask,
  {P, VData} = get_delta_field(PF, DataBin),
  {V,  Data} = get_delta_field(VF, VData),
  decode_pvs(Data, BitMask, Len+2, [{P, V} | Acc]);
decode_pvs(Tail, BitMask, Len, Acc) when Len =:= bit_size(BitMask) ->
  {lists:reverse(Acc), Tail}.

get_delta_field(0, Data) ->
  {0, Data};
get_delta_field(1, Data) ->
  leb128:decode_signed(Data).


-spec encode_trade(Timestamp::integer(), Price::integer(), Volume::integer()) -> iolist().
encode_trade(Timestamp, Price, Volume) when is_integer(Price) andalso is_integer(Volume) andalso Volume >= 0 ->
  <<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer>>.

%% @doc higher-level trade encoding
encode_trade(#trade{timestamp = Timestamp, price = Price, volume = Volume}, Scale) ->
  encode_trade(Timestamp, erlang:round(Price*Scale), Volume).

-spec decode_trade(Buffer::binary()) -> {Timestamp::integer(), Price::integer(), Volume::integer(), ByteCount::integer()}.
decode_trade(<<1:1, 1:1, Timestamp:62/integer, Price:32/signed-integer, Volume:32/unsigned-integer, _/binary>>) ->
  {Timestamp, Price, Volume, 16}.


%% @doc Univeral decoding function: takes binary and depth, returns packet and its size
-spec decode_packet(Bin::binary(), Depth::integer()) -> {ok, Packet::term(), Size::integer()}|{error, Reason::term()}.
decode_packet(Bin, Depth) ->
  try
    do_decode_packet(Bin, Depth)
  catch
    Type:Message ->
      {error, {Type, Message}}
  end.

do_decode_packet(Bin, Depth) ->
  do_decode_packet_erl(Bin, Depth).

do_decode_packet_erl(<<1:1, 0:1, _/bitstring>> = Bin, _Depth) ->
  {Timestamp, Bid, Ask, Size} = decode_full_md(Bin),
  {ok, #md{timestamp = Timestamp, bid = Bid, ask = Ask}, Size};

do_decode_packet_erl(<<0:1, _/bitstring>> = Bin, Depth) ->
  {TimeDelta, DBid, DAsk, Size} = decode_delta_md(Bin, Depth),
  {ok, {delta_md, TimeDelta, DBid, DAsk}, Size};

do_decode_packet_erl(<<1:1, 1:1, _/bitstring>> = Bin, _Depth) ->
  {Timestamp, Price, Volume, Size} = decode_trade(Bin),
  {ok, #trade{timestamp = Timestamp, price = Price, volume = Volume}, Size}.



%% @doc Main decoding function: takes binary and depth, returns packet type, body and size
-spec decode_packet(Bin::binary(), Depth::integer(), PrevMD::term(), Scale::integer()) ->
  {ok, Packet::term(), Size::integer()} | {error, Reason::term()}.
decode_packet(Bin, Depth, PrevMD, Scale) ->
  try
    do_decode_packet(Bin, Depth, PrevMD, Scale)
  catch
    Type:Message ->
      {error, {Type, Message}}
  end.

%% @doc Main decoding function
-spec decode_packet(Bin::binary(), Depth::integer(), PrevMD::term(), Scale::integer(), PrevTrade::term()) ->
  {ok, Packet::term(), Size::integer()} | {error, Reason::term()}.
decode_packet(Bin, Depth, PrevMD, Scale, PrevTrade) ->
  try
    do_decode_packet(Bin, Depth, PrevMD, Scale, PrevTrade)
  catch
    Type:Message ->
      {error, {Type, Message}}
  end.


do_decode_packet(Bin, Depth, PrevMD, Scale) ->
  do_decode_packet_erl(Bin, Depth, PrevMD, Scale).

do_decode_packet_erl(<<1:1, 0:1, _/bitstring>> = Bin, _Depth, _PrevMD, Scale) ->
  {Timestamp, Bid, Ask, Size} = decode_full_md(Bin),
  {ok, #md{timestamp = Timestamp, bid = unscale(Bid, Scale), ask = unscale(Ask, Scale)}, Size};

do_decode_packet_erl(<<0:1, _/bitstring>> = Bin, Depth, #md{} = PrevMD, Scale) ->
  {TimeDelta, DBid, DAsk, Size} = decode_delta_md(Bin, Depth),
  Result = apply_delta(PrevMD, #md{timestamp = TimeDelta, bid = unscale(DBid, Scale), ask = unscale(DAsk, Scale)}),
  {ok, Result, Size};

do_decode_packet_erl(<<1:1, 1:1, _/bitstring>> = Bin, _Depth, _PrevMD, Scale) ->
  {Timestamp, Price, Volume, Size} = decode_trade(Bin),
  {ok, #trade{timestamp = Timestamp, price = Price/Scale, volume = Volume}, Size}.


do_decode_packet(_Bin, _Depth, _PrevMD, _Scale, _PrevTrade) ->
  erlang:error(not_implemented).

%% Utility: scale bid/ask when serializing
%% scale(BidAsk, Scale) when is_list(BidAsk) andalso is_integer(Scale) ->
%%   lists:map(fun({Price, Volume}) ->
%%         {erlang:round(Price*Scale), Volume}
%%     end, BidAsk).

%% Utility: unscale bid/ask when deserializing
unscale(BidAsk, Scale) when is_list(BidAsk) ->
  lists:map(fun({Price, Volume}) ->
        {Price/Scale, Volume}
    end, BidAsk).

%% Utility: apply delta md to previous md (actually, just sum field-by-field)
apply_delta(#md{timestamp = TS1, bid = B1, ask = A1}, #md{timestamp = TS2, bid = B2, ask = A2}) ->
  #md{timestamp = TS1 + TS2, bid = apply_delta(B1, B2), ask = apply_delta(A1, A2)};

apply_delta(BidAsk1, BidAsk2) when is_list(BidAsk1) andalso is_list(BidAsk2) ->
  lists:zipwith(fun({P1, V1}, {P2, V2}) ->
        {P1 + P2, V1 + V2}
    end, BidAsk1, BidAsk2).

%% Utility: get delta md where first argument is old value, second is new one
compute_delta(#md{timestamp = TS1, bid = B1, ask = A1}, #md{timestamp = TS2, bid = B2, ask = A2}) ->
  #md{timestamp = TS2 - TS1, bid = compute_delta(B1, B2), ask = compute_delta(A1, A2)};

compute_delta(BidAsk1, BidAsk2) when is_list(BidAsk1) andalso is_list(BidAsk2) ->
  % Count delta when going from BidAsk1 to BidAsk2 -> X = X2 - X1
  lists:zipwith(fun({P1, V1}, {P2, V2}) -> {P2 - P1, V2 - V1} end, BidAsk1, BidAsk2).

get_timestamp(<<1:1, _:1/integer, Timestamp:62/integer, _/binary>>) ->
  Timestamp.


%% @doc serialize header value, used when writing header
format_header_value(date, {_,_,_} = Date) -> secdb_fs:date_to_list(Date);
format_header_value(symbol, Symbol)       -> atom_to_list(Symbol);
format_header_value(_, Value)             -> io_lib:print(Value).


%% @doc deserialize header value, used when parsing header
parse_header_value(depth, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(scale, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(chunk_size, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(version, Value) ->
  erlang:list_to_integer(Value);

parse_header_value(have_candle, "true") ->
  true;

parse_header_value(have_candle, "false") ->
  false;

parse_header_value(date, DateStr) ->
  [YS, MS, DS] = string:tokens(DateStr, "/-."),
  { erlang:list_to_integer(YS),
    erlang:list_to_integer(MS),
    erlang:list_to_integer(DS)};

parse_header_value(symbol, SymbolStr) ->
  erlang:list_to_atom(SymbolStr);

parse_header_value(_, Value) ->
  Value.
