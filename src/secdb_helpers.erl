-module(secdb_helpers).
-include("../include/secdb.hrl").
-include("secdb.hrl").
-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([candle/3]).
-export([timestamp/1]).
-export([md_at/2]).

-spec candle(secdb:symbol(), secdb:date(), list(reader_option())) -> {secdb:price(),secdb:price(),secdb:price(),secdb:price()}.
candle(Symbol, Date, Options) ->
  [{candle,Candle}] = secdb:info(Symbol, Date, [candle]),
  if Options == [] andalso Candle =/= undefined ->
    Candle;
    true -> try calculate_candle(Symbol, Date, Options) of
      Result -> Result
    catch
      Class:Error -> erlang:raise(Class, {Error, candle, Symbol, Date}, erlang:get_stacktrace())
    end
  end.

calculate_candle(Symbol, Date, Options) ->
  {ok, Iterator} = secdb:init_reader(Symbol, Date, [{filter, candle, [{period, 24*3600*1000}]}|Options]),
  % Events1 = secdb:events(Symbol, Date),
  % Events = [mid(E) || E <- Events1, element(1,E) == trade],
  Events = [mid(E) || E <- secdb:events(Iterator)],
  Open = lists:nth(1, Events),
  Close = lists:nth(length(Events), Events),
  High = lists:max(Events),
  Low = lists:min(Events),
  {Open, High, Low, Close}.


mid(#trade{price = Price}) -> Price;
mid(#md{bid = [{Bid,_}|_], ask = [{Ask,_}|_]}) -> (Bid + Ask) / 2.


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
  GregSeconds_Now = calendar:datetime_to_gregorian_seconds({Day,{H,Min,S}}),
  (GregSeconds_Now - GregSeconds_Zero)*1000 + Milli;

timestamp({Megaseconds, Seconds, Microseconds}) ->
  (Megaseconds*1000000 + Seconds)*1000 + Microseconds div 1000.


date_time(Timestamp) ->
  Epoch = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds = Epoch + Timestamp div 1000,
  calendar:gregorian_seconds_to_datetime(GregSeconds).


% get MD of given symbol at given time
md_at(Symbol, Time) ->
  Timestamp = timestamp(Time),
  {Date, _} = date_time(Timestamp),
  [MD] = secdb:events(Symbol, Date, [{range, Timestamp - timer:minutes(15), Timestamp},
      {filter, drop, trade}, {filter, last, md}]),
  MD.
