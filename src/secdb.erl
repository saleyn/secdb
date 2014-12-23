%%% @doc Symbol database
%%% Designed for continious writing of symbol data
%%% with later fast read and fast seek
-module(secdb).
-author({"Danil Zagoskin", 'z@gosk.in'}).
-include("../include/secdb.hrl").
-include("log.hrl").
-include("secdb.hrl").

-type secdb()       :: {secdb_pid, pid()} | term().

-type price()       :: float().
-type volume()      :: non_neg_integer().
-type quote()       :: {price(), volume()}.
-type timestamp()   :: non_neg_integer().
-type symbol()      :: atom().
-type date()        :: string().

-type datetime_ms() :: {calendar:date(), time_ms()}.
-type time_ms()     :: {calendar:hour(), calendar:minute(), calendar:second(), msec()}.
-type msec()        :: 0..999.

-type market_data() :: #md{}.
-type trade()       :: #trade{}.


-export_type([secdb/0, price/0, volume/0, quote/0, timestamp/0, symbol/0, date/0]).
-export_type([market_data/0, trade/0]).


%% Application configuration
-export([get_value/1, get_value/2]).

%% Querying available data
-export([symbols/0, symbols/1, dates/1, dates/2, common_dates/1, common_dates/2]).
%% Get information about symbol/date file
-export([info/1, info/2, info/3]).

%% Writing DB
-export([open_append/3, append/2, close/1, write_events/3, write_events/4]).
%% Reading existing data
-export([open_read/2, events/1, events/2, events/3, events/4]).
%% Iterator API
-export([init_reader/2, init_reader/3, read_event/1]).

%% Shortcut helpers
-export([timestamp/1, candle/2, candle/3]).

%% Run tests
-export([run_tests/0]).


%% @doc List of symbols in local database
-spec symbols() -> [symbol()].
symbols() -> secdb_fs:symbols().

%% @doc List of symbols in remote database
-spec symbols(Storage::term()) -> [symbol()].
symbols(Storage) -> secdb_fs:symbols(Storage).


%% @doc List of available dates for symbol
-spec dates(symbol()|{any(),symbol()}) -> [date()].
dates(Symbol) -> secdb_fs:dates(Symbol).

%% @doc List of available dates in remote database
-spec dates(Storage::term(), Symbol::symbol()) -> [date()].
dates(Storage, Symbol) -> secdb_fs:dates(Storage, Symbol).

%% @doc List dates when all given symbols have data
-spec common_dates([symbol()]) -> [date()].
common_dates(Symbols) -> secdb_fs:common_dates(Symbols).

%% @doc List dates when all given symbols have data, remote version
-spec common_dates(Storage::term(), [symbol()]) -> [date()].
common_dates(Storage, Symbols) -> secdb_fs:common_dates(Storage, Symbols).


%% @doc Open symbol for reading
-spec open_read(symbol()|{any(),symbol()}, date()) -> {ok, secdb()} | {error, Reason::term()}.  
open_read(Symbol, Date) ->
  secdb_reader:open(secdb_fs:path(Symbol, Date)).

%% @doc Open symbol for appending
-spec open_append(symbol(), date(), [open_option()]) -> {ok, secdb()} | {error, Reason::term()}.  
open_append(Symbol, Date, Opts) ->
  Path = secdb_fs:path(Symbol, Date),
  {db, RealSymbol, RealDate} = secdb_fs:file_info(Path),
  secdb_appender:open(Path, [{symbol,RealSymbol},{date,secdb_fs:parse_date(RealDate)}|Opts]).

%% @doc Append row to db
-spec append(secdb(), trade() | market_data()) -> {ok, secdb()} | {error, Reason::term()}.
append(Event, Symboldb) ->
  secdb_appender:append(Event, Symboldb).

-spec write_events(symbol(), date(), [trade() | market_data()]) -> ok | {error, Reason::term()}.
write_events(Symbol, Date, Events) ->
  write_events(Symbol, Date, Events, []).

-spec write_events(symbol(), date(), [trade() | market_data()], [open_option()]) -> ok | {error, Reason::term()}.
write_events(Symbol, Date, Events, Options) ->
  Path = secdb_fs:path(Symbol, Date),
  {db, RealSymbol, RealDate} = secdb_fs:file_info(Path),
  secdb_appender:write_events(Path, Events, [{symbol,RealSymbol},{date,secdb_fs:parse_date(RealDate)}|Options]).

%% @doc Fetch information from opened secdb
-spec info(secdb()) -> [{Key::atom(), Value::term()}].
info(Symboldb) ->
  secdb_reader:file_info(Symboldb).

%% @doc Fetch typical information about given Symbol/Date
-spec info(symbol(), date()) -> [{Key::atom(), Value::term()}].
info(Symbol, Date) ->
  secdb_reader:file_info(secdb_fs:path(Symbol, Date)).

%% @doc Fetch requested information about given Symbol/Date
-spec info(symbol(), date(), [Key::atom()]) -> [{Key::atom(), Value::term()}].
info(Symbol, Date, Fields) ->
  secdb_reader:file_info(secdb_fs:path(Symbol, Date), Fields).


%% @doc Get all events from filtered symbol/date
-spec events({node, node()}, symbol(), date(), [term()]) -> list(trade() | market_data()).
events({node, Node}, Symbol, Date, Filters) ->
  {ok, Iterator} = rpc:call(Node, secdb, init_reader, [Symbol, Date, Filters]),
  events(Iterator).


%% @doc Get all events from filtered symbol/date
-spec events(symbol(), date(), [term()]) -> list(trade() | market_data()).
events(Symbol, Date, Filters) ->
  case init_reader(Symbol, Date, Filters) of
    {ok, Iterator} ->
      events(Iterator);
    {error, nofile} ->
      []
  end.

%% @doc Read all events for symbol and date
-spec events(symbol(), date()) -> list(trade() | market_data()).
events(Symbol, Date) ->
  events(Symbol, Date, []).

%% @doc Just read all events from secdb
-spec events(secdb()|iterator()) -> list(trade() | market_data()).
events(#db{} = Symboldb) ->
  {ok, Iterator} = init_reader(Symboldb, []),
  events(Iterator);

events(Iterator) ->
  secdb_iterator:all_events(Iterator).

%% @doc Init iterator over opened secdb
% Options: 
%    {range, Start, End}
%    {filter, FilterFun, FilterArgs}
% FilterFun is function in secdb_filters
-spec init_reader(secdb(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(#db{} = Symboldb, Filters) ->
  case secdb_iterator:init(Symboldb) of
    {ok, Iterator} ->
      init_reader(Iterator, Filters);
    {error, _} = Error ->
      Error
  end;

init_reader(Iterator, Filters) ->
  {ok, apply_filters(Iterator, Filters)}.

%% @doc Shortcut for opening iterator on symbol-date pair
-spec init_reader(symbol(), date(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(Symbol, Date, Filters) ->
  case open_read(Symbol, Date) of
    {ok, Symboldb} ->
      init_reader(Symboldb, Filters);
    {error, _} = Error ->
      Error
  end.


apply_filter(Iterator, false) -> Iterator;
apply_filter(Iterator, {range, Start, End}) ->
  secdb_iterator:set_range({Start, End}, Iterator);
apply_filter(Iterator, {filter, Function, Args}) ->
  secdb_iterator:filter(Iterator, Function, Args).

apply_filters(Iterator, []) -> Iterator;
apply_filters(Iterator, [Filter|MoreFilters]) ->
  apply_filters(apply_filter(Iterator, Filter), MoreFilters).


%% @doc Read next event from iterator
-spec read_event(iterator()) -> {ok, trade() | market_data(), iterator()} | {eof, iterator()}.
read_event(Iterator) ->
  secdb_iterator:read_event(Iterator).

%% @doc close secdb
-spec close(secdb()) -> ok.
close(#db{file = F} = _Symboldb) ->
  case F of 
    undefined -> ok;
    _ -> file:close(F)
  end,
  ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec candle(symbol(), date()) -> {price(),price(),price(),price()}.
candle(Symbol, Date) ->
  candle(Symbol, Date, []).


-spec candle(symbol(), date(), list(reader_option())) -> {price(),price(),price(),price()}.
candle(Symbol, Date, Options) ->
  secdb_helpers:candle(Symbol, Date, Options).

% convert datetime or now() to millisecond timestamp
-spec timestamp(calendar:datetime()|datetime_ms()|erlang:timestamp()) -> timestamp().
timestamp(DateTime) ->
  secdb_helpers:timestamp(DateTime).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Configuration
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
%% @doc get configuration value with fallback to given default
get_value(Key, Default) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined   -> Default
  end.

%% @private
%% @doc get configuration value, raise error if not found
get_value(Key)  ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined   -> erlang:error({no_key,Key})
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
run_tests() ->
  eunit:test({application, secdb}).

