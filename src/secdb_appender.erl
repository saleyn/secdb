-module(secdb_appender).
-author('Max Lapshin <max@maxidoors.ru>').

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

close(#db{file = File} = State) ->
  write_candle(State),
  file:close(File),
  ok.

write_events(Symbol, Date, Events, Options) ->
  {ok, DB} = open(Symbol, Date, [{symbol, Symbol}, {date, Date} | Options]),
  write_events2(DB, Events).

write_events(Path, Events, Options) ->
  {ok, DB} = open(Path, Options),
  write_events2(DB, Events).

write_events2(DBState, Events) ->
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = append(Event, State),
        NextState
    end, DBState, Events),
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
  {ok, File} = file:open(Path, [binary,write,exclusive,raw,delayed_write]),
  {ok, 0} = file:position(File, bof),
  ok = file:truncate(File),

  {symbol, Symbol} = lists:keyfind(symbol, 1, Opts),
  {date, Date}     = lists:keyfind(date, 1, Opts),
  State            = #db{
    mode           = append,
    version        = ?SECDB_VERSION,
    symbol         = Symbol,
    date           = secdb_fs:parse_date(Date),
    sync           = not lists:member(nosync, Opts),
    path           = Path,
    have_candle    = proplists:get_value(have_candle, Opts, true),
    depth          = proplists:get_value(depth, Opts, 1),
    scale          = proplists:get_value(scale, Opts, 100),
    chunk_size     = proplists:get_value(chunk_size, Opts, 5*60)
  },

  {ok, CandleOffset0} = write_header(File, State),
  CandleOffset = case State#db.have_candle of
    true -> CandleOffset0;
    false -> undefined
  end,
  {ok, ChunkMapOffset} = write_candle(File, State),
  {ok, _CMSize} = write_chunkmap(File, State),

  {ok, State#db{
      file = File,
      candle_offset = CandleOffset,
      chunkmap_offset = ChunkMapOffset
    }}.


open_existing_db(Path, _Opts) ->
  secdb_reader:open_existing_db(Path, [binary,write,read,raw]).


% Validate event and return {Type, Timestamp} if valid
validate_event(#md{timestamp = TS, bid = Bid, ask = Ask} = Event) ->
  valid_bidask(Bid) orelse  throw({?MODULE, bad_bid, Event}),
  valid_bidask(Ask) orelse  throw({?MODULE, bad_ask, Event}),
  is_integer(TS)    andalso TS > 0 orelse throw({?MODULE, bad_timestamp, Event}),
  {md, TS};

validate_event(#trade{timestamp = TS, price = P, volume = V} = Event) ->
  is_number(P)   orelse  throw({?MODULE, bad_price, Event}),
  is_integer(V)  andalso V >= 0 orelse throw({?MODULE, bad_volume, Event}),
  is_integer(TS) andalso TS > 0 orelse throw({?MODULE, bad_timestamp, Event}),
  {trade, TS};

validate_event(Event) ->
  throw({?MODULE, invalid_event, Event}).


valid_bidask([{P,V}|_]) when is_number(P) andalso is_integer(V) andalso V >= 0 ->
  true;
valid_bidask(_) -> false.


append(_Event, #db{mode = Mode}) when Mode =/= append ->
  {error, reopen_in_append_mode};

append(Event, #db{next_chunk_time = NCT, file = File, last_md = LastMD, sync = Sync} = State) ->
  {Type, Timestamp} = validate_event(Event),
  if
    (Timestamp >= NCT orelse NCT == undefined) ->
      {ok, EOF}    = file:position(File, eof),
      {ok, State0} = append_first_event(Event, State),
      sync(Sync, File),
      {ok, State1} = start_chunk(Timestamp, EOF, State0),
      sync(Sync, File),
      {ok, State1};
    LastMD == undefined andalso Type == md ->
      append_full_md(Event, State);
    Type == md ->
      append_delta_md(Event, State);
    Type == trade ->
      append_trade(Event, State)
  end.

sync(true, File) -> file:sync(File);
sync(false,   _) -> ok.

append_first_event(Event = #md{},    State) ->
  append_full_md(Event, State);
append_first_event(Event = #trade{}, State) ->
  append_trade(Event, State#db{last_md = undefined}).

write_header(File, #db{chunk_size = CS, date = Date, depth = Depth, scale = Scale, symbol = Symbol, version = Version,
  have_candle = HaveCandle}) ->
  SymbolDBOpts = [{chunk_size,CS},{date,Date},{depth,Depth},{scale,Scale},{symbol,Symbol},{version,Version},{have_candle,HaveCandle}],
  {ok, 0} = file:position(File, 0),
  ok = file:write(File, <<"#!/usr/bin/env secdb\n">>),
  lists:foreach(fun
    ({have_candle,false}) ->
      ok;
    ({Key, Value}) ->
      ok = file:write(File, [io_lib:print(Key), ": ", secdb_format:format_header_value(Key, Value), "\n"])
    end, SymbolDBOpts),
  ok = file:write(File, "\n"),
  file:position(File, cur).


write_candle(File, #db{have_candle = true}) ->
  file:write(File, <<0:32, 0:32, 0:32, 0:32>>),
  file:position(File, cur);

write_candle(File, #db{have_candle = false}) ->
  file:position(File, cur).

write_chunkmap(File, #db{chunk_size = ChunkSize}) ->
  ChunkCount = ?NUMBER_OF_CHUNKS(ChunkSize),
  BitSize    = ChunkCount * ?OFFSETLEN_BITS,
  ChunkMap   = <<0:BitSize>>,
  {ok = file:write(File, ChunkMap), BitSize}.


start_chunk(Timestamp, Offset, #db{daystart = undefined, date = Date} = State) ->
  start_chunk(Timestamp, Offset, State#db{daystart = daystart(Date)});

start_chunk(Timestamp, Offset, #db{daystart = Daystart, chunk_size = ChunkSize,
    chunkmap = ChunkMap} = State) when Timestamp >= Daystart, (Timestamp - Daystart) < 86400000 ->

  ChunkSizeMs = timer:seconds(ChunkSize),
  ChunkNumber = (Timestamp - Daystart) div ChunkSizeMs,

  ChunkOffset = current_chunk_offset(Offset, State),
  write_chunk_offset(ChunkNumber, ChunkOffset, State),

  NextChunkTime = Daystart + ChunkSizeMs * (ChunkNumber + 1),

  %?debugFmt("Write chunk number ~w at offset: ~w (pos: ~w, time: ~w)",
  %  [ChunkNumber, State#db.chunkmap_offset + ChunkNumber*?OFFSETLEN_BYTES, ChunkOffset, Timestamp]),

  Chunk = {ChunkNumber, Timestamp, ChunkOffset},
  % ?D({new_chunk, Chunk}),
  State1 = State#db{
    chunkmap = secdb_cm:push(Chunk, ChunkMap),
    next_chunk_time = NextChunkTime},
  write_candle(State1),
  {ok, State1}.

write_candle(#db{have_candle = false}) ->  ok;
write_candle(#db{candle = undefined}) -> ok;
write_candle(#db{have_candle = true, candle_offset = CandleOffset, candle = {O,H,L,C}, file = File}) ->
  ok = file:pwrite(File, CandleOffset, <<1:1, O:31, H:32, L:32, C:32>>).


current_chunk_offset(Offset, #db{chunkmap_offset = ChunkMapOffset} = _State) ->
  Offset - ChunkMapOffset.

write_chunk_offset(ChunkNum, ChunkOffset, #db{file = F, chunkmap_offset = CMOffset} = _State) ->
  ok = file:pwrite(F, CMOffset + ChunkNum * ?OFFSETLEN_BYTES, <<ChunkOffset:?OFFSETLEN_BITS/integer>>).


append_full_md(#md{timestamp = Timestamp} = MD, #db{file = F, scale = Scale, depth = Depth} = State) ->
  Data = secdb_format:encode_full_md(MD, Scale, Depth),
  {ok, _EOF} = file:position(F, eof),
  %?debugFmt("Write full at offset ~w: (~w) ~299p (time: ~w)", [_EOF, size(Data), Data, Timestamp]),
  ok = file:write(F, Data),
  {ok, State#db{last_timestamp = Timestamp, last_md = MD}}.

append_delta_md(#md{timestamp = TS} = MD, #db{file = F, last_md = LastMD, scale = Scale, depth = Depth} = State) ->
  Data = secdb_format:encode_delta_md(MD, LastMD, Scale, Depth),
  {ok, _EOF} = file:position(F, eof),
  %?debugFmt("Write delta at offset ~w: (~w) ~299p (time: ~w)", [_EOF, size(Data), Data, TS]),
  ok = file:write(F, Data),
  {ok, State#db{last_timestamp = TS, last_md = MD}}.

append_trade(#trade{timestamp = Timestamp, price = Price} = Trade, 
  #db{file = File, scale = Scale, candle = Candle, have_candle = HaveCandle} = State) ->
  Data = secdb_format:encode_trade(Trade, Scale),
  {ok, _EOF} = file:position(File, eof),
  %?debugFmt("Write trade at offset ~w: (~w) ~299p (time: ~w)", [_EOF, size(Data), Data, Timestamp]),
  ok = file:write(File, Data),
  Candle1 = case HaveCandle of
    true  -> candle(Candle, round(Price*Scale));
    false -> Candle
  end,
  {ok, State#db{last_timestamp = Timestamp, candle = Candle1}}.


daystart(Date) ->
  DaystartSeconds = calendar:datetime_to_gregorian_seconds({Date, {0,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  DaystartSeconds * 1000.


candle(undefined, Price) -> {Price, Price, Price, Price};
candle({O,H,L,_C}, Price) when Price > H -> {O,Price,L,Price};
candle({O,H,L,_C}, Price) when Price < L -> {O,H,Price,Price};
candle({O,H,L,_C}, Price) -> {O,H,L,Price}.