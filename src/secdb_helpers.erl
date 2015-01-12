-module(secdb_helpers).
-include("../include/secdb.hrl").
-include("secdb.hrl").
-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([set_db_offsets/1, candle/3, chunk_idx_pos/2]).
-export([timestamp/1, daystart/1]).
-export([md_at/2]).

-spec chunk_idx_pos(integer(), integer()) -> integer().
chunk_idx_pos(CurChunkNum, AddBytes) when is_integer(CurChunkNum), is_integer(AddBytes) ->
  CurChunkNum * (?OFFSETLEN_BYTES + ?CANDLE_SIZE) + AddBytes.

-spec set_db_offsets(#db{}) -> #db{}.
set_db_offsets(#db{window_sec = I, daycandle_pos = CandleOffset} = DB) ->
  CMOffset     = CandleOffset + ?CANDLE_SIZE,
  ChunkCount   = ?NUMBER_OF_CHUNKS(I),
  ChunkMapSize = ChunkCount*(?OFFSETLEN_BYTES + ?CANDLE_SIZE),
  DataOffset   = CMOffset + ChunkMapSize,
  DB#db{chunkmap_pos = CMOffset, data_pos = DataOffset}.

-spec candle(secdb:symbol(), secdb:date(), list(reader_option())) -> #candle{} | undefined.
candle(Symbol, Date, Options) when is_list(Options) ->
  try calculate_candle(Symbol, Date, Options) of
    Result -> Result
  catch
    Class:Error ->
      erlang:raise(Class, {Error, candle, Symbol, Date}, erlang:get_stacktrace())
  end.

calculate_candle(Symbol, Date, Options) ->
  {ok, Iterator} = secdb:init_reader
    (Symbol, Date, [{filter, candle, [{period, 24*3600*1000}]}|Options]),
  case secdb:events(Iterator) of
    []   -> #candle{};
    List ->
      Mid = fun
        (#md{bid = [{Bid,_}|_], ask = [{Ask,_}|_]}) -> (Bid + Ask) / 2;
        (#md{bid = [{Bid,_}|_], ask = []})          -> Bid;
        (#md{bid = [],          ask = [{Ask,_}|_]}) -> Ask
      end,
      lists:foldl(fun
        (#md{} = Event, #candle{open=undefined}) ->
          M  = Mid(Event),
          #candle{open=M, high=M, low=M, close=M};
        (#md{} = Event, #candle{high=H, low=L} = I) ->
          M  = Mid(Event),
          ML = dmin(M, L),
          MH = dmax(M, H),
          I#candle{high=MH, low=ML, close=M};
        (#trade{price=P}, #candle{high=H, low=L} = I) ->
          ML = dmin(P, L),
          MH = dmax(P, H),
          I#candle{high=MH, low=ML, close=P}
      end, #candle{open=undefined, high=undefined, low=undefined}, List)
  end.

dmin(undefined, P) -> P;
dmin(P1, P2)       -> min(P1,P2).

dmax(undefined, P) -> P;
dmax(P, undefined) -> P;
dmax(P1, P2)       -> min(P1,P2).

% Convert seconds to milliseconds
timestamp(UnixTime) when is_integer(UnixTime), UnixTime < 4000000000 ->
  UnixTime * 1000;

% No convertion needed
timestamp(UTC) when is_integer(UTC) ->
  UTC;

% Convert given {Date, Time} or {Megasec, Sec, Microsec} to millisecond timestamp
timestamp({{_Y,_Mon,_D} = Day,{H,Min,S}}) ->
  timestamp({Day, {H,Min,S, 0}});

timestamp({{_Y,_Mon,_D} = Day,{H,Min,S, Milli}}) ->
  GregSeconds_Zero = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds_Now  = calendar:datetime_to_gregorian_seconds({Day,{H,Min,S}}),
  (GregSeconds_Now - GregSeconds_Zero)*1000 + Milli;

timestamp({Megaseconds, Seconds, Microseconds}) ->
  (Megaseconds*1000000 + Seconds)*1000 + Microseconds div 1000.


date_time(Timestamp) ->
  Epoch = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds = Epoch + Timestamp div 1000,
  calendar:gregorian_seconds_to_datetime(GregSeconds).


%% @doc Return number of milliseconds since epoch till start of given date (in UTC)
-spec daystart(calendar:date()) -> integer().
daystart({_,_,_} = Date) ->
  DaystartSeconds = calendar:datetime_to_gregorian_seconds({Date, {0,0,0}})
    - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  DaystartSeconds * 1000.

% get MD of given symbol at given time
md_at(Symbol, Time) ->
  Timestamp = timestamp(Time),
  {Date, _} = date_time(Timestamp),
  [MD] = secdb:events(Symbol, Date, [{range, Timestamp - timer:minutes(15), Timestamp},
      {filter, drop, trade}, {filter, last, md}]),
  MD.
