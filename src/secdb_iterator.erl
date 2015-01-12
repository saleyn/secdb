%%% @doc SymbolDB iterator module
%%% It accepts secdb state and operates only with its buffer

-module(secdb_iterator).
-author({"Danil Zagoskin", 'z@gosk.in'}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("secdb.hrl").
-include("../include/secdb.hrl").

% Create new iterator from secdb state
-export([init/1]).

% Apply filter
-export([filter/2, filter/3]).

% Limit range
-export([seek_utc/2, set_range/2]).

% Access buffer
-export([read_event/1, all_events/1]).

% Restore last state
-export([restore_last_state/1]).

% May be useful
-export([foldl/3, foldl/4]).

-record(iterator, {
    db,
    data_start,
    position,
    last_utc
  }).

-record(filter, {
    source,
    ffun,
    state,
    buffer = []
  }).

%% @doc Initialize iterator. Position at the very beginning of data
init(#db{} = DB) ->
  {Offset, State} = first_chunk_offset(DB),
  {ok, #iterator{db = State, data_start = Offset, position = Offset}}.

%% @doc Filter source iterator, expposing same API as usual iterator
filter(Source, FilterFun) ->
  filter(Source, FilterFun, undefined).
filter(Source, FilterFun, State0) when is_atom(FilterFun) ->
  filter(Source, fun secdb_filters:FilterFun/2, State0);
filter(Source, FilterFun, State0) when is_function(FilterFun, 2) ->
  create_filter(Source, FilterFun, State0).

create_filter(Source, FilterFun, State0) when is_record(Source, iterator) orelse is_record(Source, filter) ->
  #filter{
    source = Source,
    ffun = FilterFun,
    state = State0}.

%% @doc replay last chunk and return finl state
restore_last_state(Iterator) ->
  #iterator{db = LastState} = seek_utc(eof, Iterator),
  % Drop buffer to free memory
  LastState#db{buffer = undefined}.


%% @doc get start of first chunk
first_chunk_offset(#db{chunkmap = ChunkMap} = DB) ->
  case secdb_cm:pop(ChunkMap) of
    {{_N, _T, Offset, _Candle}, CM} -> {Offset, DB#db{chunkmap=CM}};  % Offset from first chunk
    {undefined,                _CM} -> {undefined, DB}  % Empty chunk map -> undef offset
  end.

%% @doc Set position to given time
seek_utc(Time, #iterator{data_start = DataStart, db = #db{chunkmap = CM, date = Date} = DB} = It) ->
  UTC = time_to_utc(Time, Date),
  {ChunkMap, NormalCM} = secdb_cm:normalize(CM),
  ChunksBefore = case UTC of
    undefined                -> [];
    eof                      -> CM;
    Int when is_integer(Int) -> lists:takewhile(fun({_N, T, _O}) -> T =< UTC end, ChunkMap)
  end,
  {_N, _T, ChunkOffset} = case ChunksBefore of
    []    -> {-1, -1, DataStart};
    [_|_] -> lists:last(ChunksBefore)
  end,
  seek_until(UTC, It#iterator{db = DB#db{chunkmap = NormalCM}, position = ChunkOffset});

seek_utc(UTC, #filter{source = Source} = Filter) ->
  % For filter, seek in underlying source
  Filter#filter{source = seek_utc(UTC, Source)}.


set_last_utc(Time, #iterator{db = #db{date = Date}} = Iterator) ->
  UTC = time_to_utc(Time, Date),
  Iterator#iterator{last_utc = UTC};

set_last_utc(UTC, #filter{source = Source} = Filter) ->
  % For filter, set last_utc on underlying source
  Filter#filter{source = set_last_utc(UTC, Source)}.

time_to_utc({_H, _M, _S} = Time, Date) ->
  Seconds = calendar:datetime_to_gregorian_seconds({Date, Time}) - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
  Seconds * 1000;
time_to_utc(Time, _Date) ->
  Time.


%% @doc set time range
set_range({Start, End}, Iterator) ->
  set_last_utc(End, seek_utc(Start, Iterator)).


%% @doc Seek forward event-by-event while timestamp is less than given
seek_until(UTC, #iterator{} = Iterator) when is_integer(UTC) ->
  case read_event(Iterator) of
    {eof, NextIterator} ->
      % EOF. Just return what we have
      NextIterator;
    {_Event, NextIterator = #iterator{db = #db{last_ts = TS}}} when TS < UTC ->
      % Drop more
      seek_until(UTC, NextIterator);
    {_Event, _NextIterator} ->
      % Revert to state before getting event
      Iterator
  end;
seek_until(Time, Iterator) when Time=:=undefined; Time=:=eof ->
  Iterator.

%% @doc Pop first event from iterator, return {Event|eof, NewIterator}
read_event(#iterator{db=DB, position= P} = I) when P =:= undefined; size(DB#db.buffer) =:= P ->
  {eof, I};
read_event(#iterator{db = #db{buffer = Buf} = DB, position=Pos, last_utc = LastUTC} = I)
    when byte_size(Buf) > Pos ->
  {_Mode, Event, NewPos, NewDB} = secdb_format:decode_packet(Buf, Pos, DB),
  ?DBG("Read event at ~w (pos ~w, new pos ~w): ~w",
    [DB#db.data_pos+Pos, Pos, NewPos, Event]),
  case NewDB#db.last_ts of
    Before when LastUTC == undefined; Before == undefined; Before =< LastUTC ->
      % Event is before given limit or we cannot compare
      {Event, I#iterator{db = NewDB, position = NewPos}};
    _After ->
      {eof, I}
  end;

%% @doc read from filter: first, try to read from buffer
read_event(#filter{buffer = [Event|Tail]} = Filter) ->
  {Event, Filter#filter{buffer = Tail}};

%% @doc read from filter: empty buffer -> pass event from source and retry
read_event(#filter{buffer = [], source = Source, ffun = FFun, state = State} = Filter) ->
  {SrcEvent, NextSrc} = read_event(Source),
  {NewBuffer, State1} = FFun(SrcEvent, State),

  % Filter isn't meant to pass eof, so append it
  RealBuffer = case SrcEvent of
    eof -> NewBuffer ++ [eof];
    _   -> NewBuffer
  end,
  read_event(Filter#filter{buffer=RealBuffer, source=NextSrc, state=State1}).

%% @doc read all events from buffer until eof
all_events(Iterator) ->
  all_events(Iterator, []).

all_events(Iterator, RevEvents) ->
  case read_event(Iterator) of
    {eof, _} ->
      lists:reverse(RevEvents);
    {Event, NextIterator} ->
      all_events(NextIterator, [Event|RevEvents])
  end.

% Foldl: low-memory fold over entries
foldl(Fun, Acc0, Iterator) ->
  do_foldl(Fun, Acc0, Iterator).

% foldl_range: fold over entries in specified time range
foldl(Fun, Acc0, Iterator, {_Start, _End} = Range) ->
  do_foldl(Fun, Acc0, set_range(Range, Iterator)).

do_foldl(Fun, AccIn, Iterator) ->
  {Event, NextIterator} = read_event(Iterator),
  case Event of
    eof ->
      % Finish folding -- no more events
      AccIn;
    _event ->
      % Iterate
      AccOut = Fun(Event, AccIn),
      do_foldl(Fun, AccOut, NextIterator)
  end.