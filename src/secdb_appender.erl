-module(secdb_appender).
-author('Max Lapshin <max@maxidoors.ru>').
-author('saleyn@gmail.com').

-include("../include/secdb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("secdb.hrl").
-include("log.hrl").

-export([open/2, open/3, append/2, close/1]).
-export([write_events/3, write_events/4]).

open(Symbol, Date, Opts) when is_atom(Symbol), is_tuple(Date), is_list(Opts) ->
  open(secdb_fs:path(Symbol, Date), [{symbol, Symbol}, {date, Date} | Opts]).

open(Path, Opts) ->
  case filelib:is_regular(Path) of
    true ->
      open_existing_db(Path, Opts);
    false ->
      create_new_db(Path, Opts)
  end.

close(#db{file = File, daycandle = DCandle, daycandle_pos = DCPos, candle = Candle}=DB) ->
  % Write last candle info
  CandleAddPos = DB#db.chunkmap_pos+?OFFSETLEN_BYTES,
  CandlePos    = secdb_helpers:chunk_idx_pos(DB#db.cur_chunk_num, CandleAddPos),
  write_candle(File, CandlePos, Candle),
  % Write daycandle info
  write_candle(File, DCPos,     DCandle),
  file:close  (File),
  ok.

write_events(Symbol, Date, Events, Options) ->
  {ok, DB} = open(Symbol, Date, [{symbol, Symbol}, {date, Date} | Options]),
  write_events2(DB, Events).

write_events(Path, Events, Options) when is_list(Path) ->
  {ok, DB} = open(Path, Options),
  write_events2(DB, Events).

write_events2(#db{} = DB, Events) when is_list(Events) ->
  S1 = lists:foldl(fun(Event, State) -> append(Event, State) end, DB, Events),
  ok = close(S1).

%% @doc Create a header for new DB
%% Structure of file is following:
%% #!/usr/bin/env secdb
%% header: value
%% header: value
%% header: value
%%
%% chunkmap of fixed size
%% rows
create_new_db(Path, Opts) ->
  filelib:ensure_dir(Path),
  {ok, File}        = file:open(Path, [binary,write,exclusive,raw,delayed_write]),
  {ok, 0}           = file:position(File, bof),
  ok                = file:truncate(File),

  {symbol, Sym}     = lists:keyfind(symbol, 1, Opts),
  {date, Date0}     = lists:keyfind(date, 1, Opts),
  Date              = secdb_fs:parse_date(Date0),
  Daystart          = secdb_helpers:daystart(Date),
  State             = #db{
    mode            = append,
    file            = File,
    version         = ?SECDB_VERSION,
    symbol          = Sym,
    date            = Date,
    daystart        = Daystart,
    next_chunk_time = Daystart,
    cur_chunk_num   = 0,
    sync            = not lists:member(nosync, Opts),
    path            = Path,
    depth           = proplists:get_value(depth, Opts, 1),
    scale           = proplists:get_value(scale, Opts, 100),
    window_sec      = proplists:get_value(window_sec, Opts, 5*60)
  },

  {ok, CandleOffs}  = write_header(State),
  DB                = secdb_helpers:set_db_offsets(State#db{daycandle_pos = CandleOffs}),

  % Write empty daycandle
  write_candle(File, CandleOffs, #candle{}),
  ok                = write_chunkmap(DB),
  % write BOF indicator, so that first chank's valid offset is 1
  % when counting from this position:
  ok                = file:write(File, <<16#FF>>),
  ?DBG("Database ~s initial size: ~w", [Path, element(2, file:position(File, cur))]),
  {ok, DB}.


open_existing_db(Path, _Opts) ->
  secdb_reader:open_existing_db(Path, [binary,write,read,raw,delayed_write]).


% Validate event and return {Type, Timestamp} if valid
validate_event(#md{timestamp = TS, bid = Bid, ask = Ask} = Event) ->
  valid_bidask(Bid) orelse  throw({?MODULE, bad_bid, Event}),
  valid_bidask(Ask) orelse  throw({?MODULE, bad_ask, Event}),
  is_integer(TS)    andalso TS > 0 orelse throw({?MODULE, bad_timestamp, Event}),
  TS;
validate_event(#trade{timestamp = TS, price = P, volume = V} = Event) ->
  is_number(P)   orelse  throw({?MODULE, bad_price, Event}),
  is_integer(V)  andalso V >= 0 orelse throw({?MODULE, bad_volume, Event}),
  is_integer(TS) andalso TS > 0 orelse throw({?MODULE, bad_timestamp, Event}),
  TS;
validate_event(Event) ->
  throw({?MODULE, invalid_event, Event}).

valid_bidask([{P,V}|_]) when is_number(P), is_integer(V), V >= 0 ->
  true;
valid_bidask(_) -> false.

append(Event, #db{mode = append, next_chunk_time=NCT, file=F, sync=Sync} = DB) when is_integer(NCT) ->
  Timestamp = validate_event(Event),
  if
    Timestamp >= NCT ->
      {ok, EOF}    = file:position(F, eof),
      State0       = append_event(full, Event, DB),
      sync(Sync, F),
      {ok, State1} = start_chunk(Timestamp, EOF, State0),
      sync(Sync, F),
      State1;
    true ->
      append_event(delta, Event, DB)
  end;
append(_Event, #db{mode = Mode}) ->
  throw({reopen_db_in_append_mode, Mode}).

-ifdef(DEBUG).
split(0, _)     -> [];
split(I, [H|T]) -> [H|split(I-1, T)];
split(_, [])    -> [].
-define(SCALE(I), round(math:log(I)/math:log(10))).
-endif.

append_event(Mode, #md{timestamp = _TS} = MD, #db{file = F, scale=_S, depth = _D} = DB) ->
  {Data,DB1} = secdb_format:encode(Mode, MD, DB),
  ?DBG("Write ~-5w md    at ~w: (~3w) ~299p (time: ~w)\n~*s(last=~w) bid=~s ask=~s",
    [Mode,element(2,file:position(F,eof)),size(Data),Data,_TS, 52, " ", DB1#db.last_quote,
     string:join(
       [io_lib:format("~w@~.*f", [V,?SCALE(_S),P]) || {P,V} <- split(_D, MD#md.bid)], ","),
     string:join(
       [io_lib:format("~w@~.*f", [V,?SCALE(_S),P]) || {P,V} <- split(_D, MD#md.ask)], ",")
    ]),
  ok = file:write(F, Data),
  DB1;

append_event(Mode, #trade{timestamp=_TS, side=Side, price=Price, volume=Qty} = Trade,
             #db{file = File, scale=S, daycandle=DCandle0, candle = Candle0} = DB)
    when is_number(Price), is_integer(Qty), is_atom(Side) ->
  P          = round (Price * S),
  DCandle    = candle(DCandle0, Side, P, Qty),
  Candle     = candle(Candle0,  Side, P, Qty),
  {Data,DB1} = secdb_format:encode(Mode, Trade, DB#db{daycandle = DCandle, candle = Candle}),
  {ok,   _N} = file:position(File, eof),
  ?DBG("Write ~-5w trade at ~w: (~3w) ~299p (time: ~w)\n~*s(last=~w) ~w",
    [Mode, _N, size(Data), Data, _TS, 52, " ", DB1#db.last_quote, Trade]),
  ok         = file:write(File, Data),
  DB1.


sync(true, File) -> file:sync(File);
sync(false,   _) -> ok.

write_header(#db{file = File, window_sec = I, date = Date, depth = Depth, scale = Scale,
                 symbol = Symbol, version = Version}) ->
  SymbolDBOpts = [{version,Version},{symbol,Symbol},{date,Date},
                  {window_sec, I},{depth,Depth},{scale,Scale}],
  MaxKeyLen    = lists:max([length(atom_to_list(K)) || {K,_} <- SymbolDBOpts]),
  {ok, 0}      = file:position(File, 0),
  ok           = file:write(File, <<"#!/usr/bin/env secdb\n">>),
  lists:foreach(fun
    ({Key, Value}) ->
      L  = io_lib:format("~.*s: ~s\n",
            [MaxKeyLen, Key, secdb_format:format_header_value(Key, Value)]),
      ok = file:write(File, [L])
    end, SymbolDBOpts),
  ok = file:write(File, "\n"),
  file:position(File, cur).

write_chunkmap(#db{file=File, window_sec=I, data_pos=_DStart, path=_Path}) ->
  %% Chunkmap is a list of ChunkCount entries:
  %% +-----------+------------+
  %% |ChunkOffset|PeriodCandle|
  %% +-----------+------------+
  %%   4 bytes     6*4 bytes

  ChunkCount  = ?NUMBER_OF_CHUNKS(I),
  BitSize     = 8 * ChunkCount * (?OFFSETLEN_BYTES + ?CANDLE_SIZE),
  ChunkMap    = <<0:BitSize>>,
  file:position(File, eof),
  ?DBG("Write chunkmap at ~w size=~w (data offset: ~w) file: ~s",
    [element(2,file:position(File,cur)), size(ChunkMap), _DStart, _Path]),
  ok = file:write(File, ChunkMap).

start_chunk(Timestamp, Offset,
    #db{daystart = Daystart, window_sec = WS, chunkmap = ChunkMap} = S)
    when is_integer(Timestamp), is_integer(Daystart)
       , Timestamp >= Daystart, (Timestamp - Daystart) < 86400000 ->
  ChunkSizeMs = timer:seconds(WS),
  ChunkNumber = (Timestamp - Daystart) div ChunkSizeMs,
  {ChunkOffset, S1} = write_chunk_index(ChunkNumber, Offset, S),

  % Next chunk time
  NCT = Daystart + ChunkSizeMs * (ChunkNumber + 1),

  ?DBG("Write chunk number ~w at index pos: ~w+~w=~w (data pos: ~w+~w=~w, time: ~w)",
    [ChunkNumber, S#db.chunkmap_pos, ChunkNumber*?OFFSETLEN_BYTES,
     S#db.chunkmap_pos + ChunkNumber*?OFFSETLEN_BYTES,
     S#db.data_pos, ChunkOffset, S#db.data_pos + ChunkOffset, Timestamp]),
  Chunk = {ChunkNumber, Timestamp, ChunkOffset},
  NCM   = secdb_cm:push(Chunk,ChunkMap),
  % ?D({new_chunk, Chunk}),
  S2    = S1#db{chunkmap = NCM, next_chunk_time = NCT, candle = undefined},
  write_candle(S2#db.file, S2#db.daycandle_pos, S2#db.daycandle),
  {ok, S2}.

write_chunk_index(ChunkNum, Offset, #db{file=F, chunkmap_pos=Start, data_pos=Data}=DB) ->
  % Write accumulated candle of the last chunk
  CandlePos   = secdb_helpers:chunk_idx_pos(DB#db.cur_chunk_num, Start+?OFFSETLEN_BYTES),
  write_candle(F, CandlePos, DB#db.candle),
  % Calculate offset of the newly formed chunk and write it to the chunk index
  ChunkOffset = Offset - Data,
  Pos         = secdb_helpers:chunk_idx_pos(ChunkNum, Start),
  Bin         = <<ChunkOffset:?OFFSETLEN_BITS/integer, (encode_candle(undefined))/binary>>,
  ok = file:pwrite(F, Pos, Bin),
  {ChunkOffset, DB#db{cur_chunk_num = ChunkNum, candle = undefined}}.

write_candle(_File, _Pos, undefined) ->
  ok;
write_candle(File, Pos, #candle{} = Candle) when is_integer(Pos) ->
  ok = file:pwrite(File, Pos, encode_candle(Candle)).

candle(undefined, buy,  P, V) -> #candle{open=P, high=P, low=P, close=P, vbuy=V, vsell=0};
candle(undefined, sell, P, V) -> #candle{open=P, high=P, low=P, close=P, vbuy=0, vsell=V};
candle(#candle{high=H}=C, Side, P, V) when P > H ->
  add_vol(Side, V, C#candle{high=P, close=P});
candle(#candle{low =L}=C, Side, P, V) when P < L ->
  add_vol(Side, V, C#candle{low=P,  close=P});
candle(#candle{}=C, Side, P, V) ->
  add_vol(Side, V, C#candle{close=P}).

add_vol(buy,  Qty, #candle{vbuy  = N} = C) -> C#candle{vbuy  = N+Qty};
add_vol(sell, Qty, #candle{vsell = N} = C) -> C#candle{vsell = N+Qty}.

encode_candle(undefined) ->
  <<0:32, 0:32, 0:32, 0:32, 0:32, 0:32>>;
encode_candle(#candle{open = O, high = H, low = L, close = C, vbuy = VB, vsell = VS}) ->
  <<1:1, O:31, H:32, L:32, C:32, VB:32, VS:32>>.