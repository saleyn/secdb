%%% @doc secdb_fs
%%% This module handles DB file structure

-module(secdb_fs).
-author({"Danil Zagoskin", 'z@gosk.in'}).

-include("../include/secdb.hrl").

% Configuration
-export([root/0]).

% Filename <-> {Symbol, Date} conversion
-export([path/2, path/3, file_info/1, parse_date/1, date_to_list/1]).

% Local querying
-export([symbols/0, dates/1, common_dates/1]).

% Remote querying
-export([symbols/1, dates/2, common_dates/2]).



%% @doc Return root dir of history
root() ->
  secdb:get_value(root, "db").


%% @doc Return path to DB file for given symbol and date
-spec path(secdb:symbol() | {any(), secdb:symbol()}, secdb:date()) -> file:name().
path(Symbol, Date) ->
  path(Symbol, Date, nocreate).

path(wildcard,Date,        Create)                      -> path({symbol, '*'},    Date, Create);
path(Symbol,  Date,        Create) when is_atom(Symbol) -> path({symbol, Symbol}, Date, Create);
path({path,   Path},_Date,_Create)                      -> Path;
path({_,_} =  Other, Date, Create) when is_tuple(Date)  -> path(Other, Date, date_to_list(Date), Create);
path({_,_} =  Other, Date, Create) when is_list(Date)   -> path(Other, parse_date(Date), Date, Create).

path({Type, Symbol}, {Y,M,_D}, Date, Create) when Create =:= create; Create =:= nocreate ->
  Base    = [Symbol, ".", filename_timestamp(Date), ".secdb"],
  Path    = filename:join([root(), Type, integer_to_list(Y), i2l(M), Base]),
  Create =:= create andalso
    (filelib:ensure_dir(Path) =:= ok orelse throw({cannot_create_dir, Path})),
  Path.

%% @doc List of symbols in local database
-spec symbols() -> [secdb:symbol()].
symbols() ->
  SymbolSet = lists:foldl(fun(DbFile, Set) ->
        {db, Symbol, _Date} = file_info(DbFile),
        sets:add_element(Symbol, Set)
    end, sets:new(), list_db()),
  lists:sort(sets:to_list(SymbolSet)).

%% @doc List of symbols in remote database
-spec symbols(Storage::term()) -> [secdb:symbol()].
symbols(_Storage) ->
  [].


%% @doc List of available dates for symbol
-spec dates(secdb:symbol()) -> [secdb:date()].
dates(Symbol) ->
  Dates = lists:map(fun(DbFile) ->
        {db, _Symbol, Date} = file_info(DbFile),
        Date
    end, list_db(Symbol)),
  lists:sort(Dates).

%% @doc List of available dates in remote database
-spec dates(Storage::term(), Symbol::secdb:symbol()) -> [secdb:date()].
dates(_Storage, _Symbol) ->
  [].


%% @doc List dates when all given symbols have data
-spec common_dates([secdb:symbol()]) -> [secdb:date()].
common_dates(Symbols) ->
  SymbolDates = lists:map(fun dates/1, Symbols),
  [Dates1|OtherDates] = SymbolDates,

  _CommonDates = lists:foldl(fun(Dates, CommonDates) ->
        Missing = CommonDates -- Dates,
        CommonDates -- Missing
    end, Dates1, OtherDates).

common_dates(_Storage, _Symbols) ->
  [].

%% @doc List all existing DB files
list_db() -> list_db(wildcard).

%% @doc List existing DB files for given symbol
list_db(Symbol) ->
  DbWildcard = path(Symbol, wildcard),
  filelib:wildcard(DbWildcard).

%% @doc return file information as {db, Symbol, Date} if possible
file_info(Path) ->
  try
    {ok, Info} = get_file_info(Path),
    Info
  catch _:_ ->
    % ?D(Other),
    undefined
  end.

%% internal use for file_info/1
get_file_info(Path) ->
  {match, [SymbolString, Date]} = re:run(
      filename:basename(Path),
      "^(.*)\.(\\d{4}-\\d{2}-\\d{2})\.secdb$", [{capture, [1, 2], list}]),
  {ok, {db, list_to_atom(SymbolString), Date}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%      Format utility functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc format given date for use in file name

filename_timestamp(wildcard)          ->  "????-??-??";       % Wildcard for FS querying
filename_timestamp({_, _, _} = Date)  ->  date_to_list(Date); % Format as YYYY-MM-DD
filename_timestamp(Date) when is_list(Date) -> date_to_list(parse_date(Date)).

%% @doc Parse string with date.
%% Argument may be in form YYYY-MM-DD or YYYY/MM/DD
parse_date({_,_,_} = Date) -> Date;
parse_date(DateStr) when is_list(DateStr) andalso length(DateStr) == 10 ->
  [Y, M, D] = [list_to_integer(I) || I <- string:tokens(DateStr, "-/.")],
  {Y, M, D}.

-spec date_to_list(calendar:date()) -> string().
date_to_list({Y,M,D}) -> lists:append([integer_to_list(Y), [$-], i2l(M), [$-], i2l(D)]).

i2l(I) when I < 10    -> [$0, $0 + I];
i2l(I)                -> integer_to_list(I).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%     TESTING
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("eunit/include/eunit.hrl").

path_test() ->
  application:set_env(secdb, root, "custom/path"),
  ?assertEqual("custom/path/symbol/MICEX.TEST.2012-08-03.secdb", path('MICEX.TEST', {2012, 08, 03})),
  ?assertEqual("custom/path/symbol/MICEX.TEST.2012-08-03.secdb", path('MICEX.TEST', "2012-08-03")),
  ?assertEqual("custom/path/symbol/MICEX.TEST.2012-08-03.secdb", path('MICEX.TEST', "2012/08/03")),

  ?assertEqual("custom/path/symbol/*-????-??-??.secdb", path(wildcard, wildcard)),

  ?assertEqual("custom/path/subdir/MICEX.TEST.2012-08-03.secdb", path({subdir, 'MICEX.TEST'}, "2012-08-03")),
  ?assertEqual("custom/path/sub/path/MICEX.TEST.2012-08-03.secdb", path({"sub/path", 'MICEX.TEST'}, "2012-08-03")),
  ?assertEqual("/random/file", path({path, "/random/file"}, none)),
  ok.

file_info_test() ->
  application:set_env(secdb, root,  "custom/path"),
  ?assertEqual({db, 'MICEX.TEST',   "2012-08-03"}, file_info("db/symbol/MICEX.TEST.2012-08-03.secdb")),
  ?assertEqual(undefined, file_info("db/symbol/MICEX.TEST.2012-08-03.secdb")),
  ?assertEqual(undefined, file_info("db/symbol/MICEX.TEST.2012.08.03.secdb")),
  ok.

symbols_test() ->
  application:set_env(secdb, root, code:lib_dir(secdb, test) ++ "/fixtures/fs"),
  ?assertEqual(lists:sort(['MICEX.TEST', 'LSEIOB.TEST', 'FX_TOM.USDRUB']), symbols()),
  ok.

dates_test() ->
  application:set_env(secdb, root, code:lib_dir(secdb, test) ++ "/fixtures/fs"),
  ?assertEqual(["2012-08-01", "2012-08-02", "2012-08-05"],
    dates('MICEX.TEST')),
  ?assertEqual(["2012-08-01", "2012-08-03", "2012-08-04", "2012-08-05", "2012-08-06"],
    dates('LSEIOB.TEST')),
  ?assertEqual(["2012-08-02", "2012-08-04", "2012-08-05"],
    dates('FX_TOM.USDRUB')),
  ok.

common_dates_test() ->
  application:set_env(secdb, root, code:lib_dir(secdb, test) ++ "/fixtures/fs"),
  ?assertEqual(["2012-08-05"], common_dates(['MICEX.TEST', 'LSEIOB.TEST', 'FX_TOM.USDRUB'])),
  ok.

