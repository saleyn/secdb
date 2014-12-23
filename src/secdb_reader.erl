%%% @doc secdb_reader
%%% Read-only API

-module(secdb_reader).
-author({"Danil Zagoskin", 'z@gosk.in'}).

-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("secdb.hrl").
-include("../include/secdb.hrl").

% Open DB, read its contents and close,
% returning self-sufficient state
-export([open/1, open_for_migrate/1, open_existing_db/2]).

-export([file_info/1, file_info/2]).

open(Path) ->
  case filelib:is_regular(Path) of
    true ->
      {ok, State1} = open_existing_db(Path, [read, binary, raw]),
      State2 = #db{file = F} = buffer_data(State1),
      file:close(F),
      {ok, State2#db{file = undefined}};
    false ->
      {error, nofile}
  end.

open_for_migrate(Path) ->
  {ok, State1} = open_existing_db(Path, [migrate, read, binary, raw]),
  State2 = #db{file = F} = buffer_data(State1),
  file:close(F),
  {ok, State2#db{file = undefined}}.

open_existing_db(Path, Modes) ->
  {ok, File} = try 
    case lists:member(write, Modes) of
      % Try accelerated read with emmap
      false -> emmap:open(Path, [read, shared, direct, nolock]);
      true -> file:open(Path, Modes -- [migrate])
    end
  catch
    error:undef ->
      % Fallback to file module
      file:open(Path, Modes -- [migrate])
  end,
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, AfterHeaderOffset} = read_header(File),

  {version, Version} = lists:keyfind(version, 1, SavedDBOpts),
  {symbol, Symbol} = lists:keyfind(symbol, 1, SavedDBOpts),
  {date, Date} = lists:keyfind(date, 1, SavedDBOpts),
  {scale, Scale} = lists:keyfind(scale, 1, SavedDBOpts),
  {depth, Depth} = lists:keyfind(depth, 1, SavedDBOpts),
  {chunk_size, ChunkSize} = lists:keyfind(chunk_size, 1, SavedDBOpts),
  HaveCandle = proplists:get_value(have_candle, SavedDBOpts, false),

  {CandleOffset, ChunkMapOffset} = case HaveCandle of
    true -> {AfterHeaderOffset, AfterHeaderOffset + 4*4};
    false -> {undefined, AfterHeaderOffset}
  end,
  
  State0 = #db{
    mode            = append,
    version         = Version,
    symbol          = Symbol,
    date            = Date,
    depth           = Depth,
    scale           = Scale,
    chunk_size      = ChunkSize,
    file            = File,
    path            = Path,
    have_candle     = HaveCandle,
    candle_offset   = CandleOffset,
    chunkmap_offset = ChunkMapOffset
  },

  State2 = read_chunkmap(read_candle(State0)),
  case Version of
    ?SECDB_VERSION ->
      ValidatedState = secdb_validator:validate(State2),
      {ok, ValidatedState};
    _Other ->
      case lists:member(migrate, Modes) of
        true ->
          {ok, State2};
        false ->
          erlang:error({need_to_migrate, Path})
      end
  end.



%% @doc read data from chunk map start to EOF
buffer_data(#db{file = File, chunkmap_offset = ChunkMapOffset} = State) ->
  % determine file size
  {ok, FileSize} = file:position(File, eof),
  % read all data from data start to file end
  {ok, Buffer} = file:pread(File, ChunkMapOffset, FileSize - ChunkMapOffset),
  % return state with buffer set
  State#db{buffer = Buffer}.


candle_info(undefined, _) -> [];
candle_info({O,H,L,C}, Scale) -> [{candle, {O/Scale,H/Scale,L/Scale,C/Scale}}].


%% @doc return some file_info about opened secdb
file_info(#db{symbol = Symbol, date = Date, path = Path, scale = Scale, candle = Candle}) ->
  candle_info(Candle,Scale) ++ [{path, Path},{symbol, Symbol}, {date, Date}];

file_info(FileName) ->
  file_info(FileName, [path, symbol, date, version, scale, depth, candle]).

%% @doc read file info
file_info(FileName, Fields) ->
  case filelib:is_regular(FileName) of
    true -> get_file_info(FileName, Fields);
    false -> undefined
  end.

get_file_info(FileName, Fields) ->
  {ok, F} = file:open(FileName, [read, binary]),
  {ok, 0} = file:position(F, bof),

  {ok, SavedDBOpts, AfterHeaderOffset} = read_header(F),
  {CMOffset,CandleOffset} = case proplists:get_value(have_candle,SavedDBOpts,false) of
    true -> {AfterHeaderOffset + 4*4, AfterHeaderOffset};
    false -> {AfterHeaderOffset, undefined}
  end,

  try
    lists:map(fun
      (presence) ->
        CSz       = proplists:get_value(chunk_size, SavedDBOpts),
        NZChunks  = read_nonzero_chunks(false, #db{file=F, chunkmap_offset=CMOffset, chunk_size=CSz}),
        Presence  = {?NUMBER_OF_CHUNKS(CSz), [N || {N, _} <- NZChunks]},
        {presence, Presence};
      (candle) when CandleOffset == undefined->
        {candle, undefined};
      (candle) when is_number(CandleOffset) ->
        Scale = proplists:get_value(scale, SavedDBOpts),
        {candle, read_candle(F,CandleOffset,Scale)};
      (Field) ->
        Value = proplists:get_value(Field, [{path, FileName} | SavedDBOpts]),
        {Field, Value}
    end, Fields)
  after
    file:close(F)
  end.



%% @doc Read header from file descriptor, return list of key:value pairs and position at chunkmap start
read_header(File) ->
  Options = read_header_lines(File, []),
  {ok, Offset} = file:position(File, cur),
  {ok, Options, Offset}.

%% @doc Helper for read_header -- read lines until empty line is met
read_header_lines(File, Acc) ->
  {ok, HeaderLine} = file:read_line(File),
  case parse_header_line(HeaderLine) of
    {Key, Value} ->
      read_header_lines(File, [{Key, Value}|Acc]);
    ignore ->
      read_header_lines(File, Acc);
    stop ->
      lists:reverse(Acc)
  end.

%% @doc Accept header line and return {Key, Value}, ignore (for comments) or stop
parse_header_line(HeaderLine) when is_binary(HeaderLine) ->
  % We parse strings, convert
  parse_header_line(erlang:binary_to_list(HeaderLine));

parse_header_line("#" ++ _Comment) ->
  % Comment. Ignore
  ignore;

parse_header_line("\n") ->
  % Empty line. Next byte is chunkmap
  stop;

parse_header_line(HeaderLine) when is_list(HeaderLine) ->
  % Remove trailing newline
  parse_header_line(string:strip(HeaderLine, right, $\n), nonewline).

parse_header_line(HeaderLine, nonewline) ->
  % Extract key and value
  [KeyRaw, ValueRaw] = string:tokens(HeaderLine, ":"),

  KeyStr = string:strip(KeyRaw, both),
  ValueStr = string:strip(ValueRaw, both),

  Key = erlang:list_to_atom(KeyStr),
  Value = secdb_format:parse_header_value(Key, ValueStr),

  {Key, Value}.


%% @doc read candle from file descriptor
read_candle(#db{have_candle = false} = State) ->
  State;

read_candle(#db{file = File, have_candle = true, candle_offset = CandleOffset} = State) ->
  State#db{candle = read_candle(File,CandleOffset)}.

read_candle(File, CandleOffset) ->
  case file:pread(File, CandleOffset, 4*4) of
    {ok, <<0:1, _O:31, _H:32, _L:32, _C:32>>} -> undefined;
    {ok, <<1:1, O:31, H:32, L:32, C:32>>} -> {O,H,L,C}
  end.

read_candle(File, CandleOffset, Scale) ->
  case read_candle(File, CandleOffset) of
    undefined -> undefined;
    {O,H,L,C} -> {O/Scale,H/Scale,L/Scale,C/Scale}
  end.


%% @doc Read chunk map and validate corresponding timestamps.
%% Result is saved to state
read_chunkmap(#db{} = State) ->
  State#db{chunkmap = read_nonzero_chunks(true, State)}.

%% @doc Read raw chunk map and return {Number, Offset} list for chunks containing data
read_nonzero_chunks(LookupTS, #db{file = File, chunk_size = ChunkSize, chunkmap_offset = CMOffset}) ->
  ChunkCount = ?NUMBER_OF_CHUNKS(ChunkSize),
  {ok, ChunkMap} = file:pread(File, CMOffset, ChunkCount*?OFFSETLEN_BYTES),
  Fun = fun
    Process(Bin, N, NN, Acc) ->
      case Bin of
        _ when byte_size(Bin) =:= NN ->  %% Reached the end of the offset list
          lists:reverse(Acc);
        <<_:NN/binary, Offset/unsigned, _/binary>> when Offset =/= 0 ->
          if LookupTS ->
            {ok, Header} = file:pread(File, CMOffset + Offset, 8),
            Timestamp    = secdb_format:get_timestamp(Header),
            Process(Bin, N+1, NN+2, [{N, Timestamp, Offset} | Acc]);
          true ->
            Process(Bin, N+1, NN+2, [{N, Offset} | Acc])
          end;
        _ ->
          Process(Bin, N+1, NN+2, Acc)
      end
    end,
  Fun(ChunkMap, 0, 0, []).