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
-export([open/1, open/2, open_for_migrate/1, open_existing_db/2]).

-export([file_info/1, file_info/2]).

open(Symbol, Date) ->
  open(secdb_fs:path(Symbol, Date)).

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
      true  -> file :open(Path, Modes -- [migrate])
    end
  catch
    error:undef ->
      % Fallback to file module
      ?DBG("open_existing_db: fallback to file:open/2", []),
      file:open(Path, Modes -- [migrate])
  end,
  {ok, 0} = file:position(File, bof),

  {ok, SavedDBOpts, CandleOffset} = read_header(File),

  {version,  Version}  = lists:keyfind(version,   1, SavedDBOpts),
  {symbol,   Symbol}   = lists:keyfind(symbol,    1, SavedDBOpts),
  {date,     Date}     = lists:keyfind(date,      1, SavedDBOpts),
  {scale,    Scale}    = lists:keyfind(scale,     1, SavedDBOpts),
  {depth,    Depth}    = lists:keyfind(depth,     1, SavedDBOpts),
  {window_sec, Win}    = lists:keyfind(window_sec,1, SavedDBOpts),

  State0 = secdb_helpers:set_db_offsets(
    #db{
      mode             = append,
      version          = Version,
      symbol           = Symbol,
      date             = Date,
      daystart         = secdb_helpers:daystart(Date),
      depth            = Depth,
      scale            = Scale,
      window_sec         = Win,
      file             = File,
      path             = Path,
      daycandle_pos    = CandleOffset
    }),

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
buffer_data(#db{file = File, data_pos = Start} = State) ->
  % determine file size
  {ok, FileSize} = file:position(File, eof),
  % read all data from data start to file end
  {ok, Buffer} = file:pread(File, Start, FileSize - Start),
  ?DBG("Read buffer at ~w size ~w", [Start, size(Buffer)]),
  % return state with buffer set
  State#db{buffer = Buffer}.


candle_info(undefined, _) -> [];
candle_info(#candle{open=O, high=H, low=L, close=C} = I, Scale) ->
  [I#candle{open=O/Scale, high=H/Scale, low=L/Scale, close=C/Scale}].


%% @doc return some file_info about opened secdb
file_info(#db{symbol = Symbol, date = Date, path = Path, scale = Scale, daycandle = Candle}) ->
  candle_info(Candle,Scale) ++ [{path, Path},{symbol, Symbol}, {date, Date}];

%% @doc return database file information
file_info(FileName) ->
  All = [path, version, symbol, date, scale, precision, depth, window,
         chunkmap_pos, data_pos, daycandle, candles],
  file_info(FileName, All).

%% @doc read file info
file_info(FileName, Fields) ->
  case filelib:is_regular(FileName) of
    true -> get_file_info(FileName, Fields);
    false -> undefined
  end.

get_file_info(FileName, Fields) ->
  {ok, DB} = open_existing_db(FileName, [read]),
  lists:map(fun
    (presence) ->
      ChunkCount     = ?NUMBER_OF_CHUNKS(DB#db.window_sec),
      Presence       = {ChunkCount, [element(1, Chunk) || Chunk <- DB#db.chunkmap]},
      {presence, Presence};
    (path)          -> {path,       FileName};
    (version)       -> {version,    DB#db.version};
    (symbol)        -> {symbol,     DB#db.symbol};
    (date)          -> {date,       DB#db.date};
    (scale)         -> {precision,  DB#db.scale};
    (precision)     -> {precision,  trunc(round(math:log(DB#db.scale) / math:log(10)))};
    (depth)         -> {depth,      DB#db.depth};
    (window)        -> {window,     DB#db.window_sec};
    (chunkmap_pos)  -> {chunkmap_pos, DB#db.chunkmap_pos};
    (data_pos)      -> {data_pos,   DB#db.data_pos};
    (daycandle)     -> {daycandle,  DB#db.daycandle};
    (candles)       -> {candles,    [element(4, Chunk) || Chunk <- DB#db.chunkmap]}
  end, Fields).


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
  [K, V] = string:tokens(HeaderLine, ":"),
  Key    = list_to_atom(string:strip(K)),
  Value  = secdb_format:parse_header_value(Key, string:strip(V)),
  {Key, Value}.


%% @doc read candle from file descriptor
read_candle(#db{file=F, daycandle_pos=CandleOffset, scale=Scale} = DB) ->
  case read_candle(F, CandleOffset) of
    undefined ->
      DB;
    #candle{open=O, high=H, low=L, close=C} = I ->
      DB#db{daycandle = I#candle{open=O/Scale, high=H/Scale, low=L/Scale, close=C/Scale}}
  end.

read_candle(File, CandleOffset) ->
  case file:pread(File, CandleOffset, ?CANDLE_SIZE) of
    {ok, <<0:1,_O:31,_H:32,_L:32,_C:32,_VB:32,_VS:32>>} ->
      undefined;
    {ok, <<1:1, O:31, H:32, L:32, C:32, VB:32, VS:32>>} ->
      #candle{open=O, high=H, low=L, close=C, vbuy=VB, vsell=VS}
  end.

%% @doc Read chunk map and validate corresponding timestamps.
%% Result is saved to state
read_chunkmap(#db{} = State) ->
  Chunks = read_nonzero_chunks(true, State),
  ?DBG("Chunkmap: ~p", [Chunks]),
  State#db{chunkmap = secdb_cm:new(Chunks)}.

%% @doc Read raw chunk map and return {Number, Offset} list for chunks containing data
read_nonzero_chunks(LookupTS, #db{file = File, window_sec = I, chunkmap_pos = CMOffset}) ->
  ChunkCount   = ?NUMBER_OF_CHUNKS(I),
  ChunkMapSize = ChunkCount*(?OFFSETLEN_BYTES + ?CANDLE_SIZE),
  DataOffset   = CMOffset + ChunkMapSize,
  Seq          = lists:seq(0, ChunkCount-1),
  {ok, CMap}   = file:pread(File, CMOffset, ChunkMapSize),
  ChunkOffsets = [{N, #candle{open=O, high=H, low=L, close=C, vbuy=VB, vsell=VS}}
                   || <<N:32/unsigned, _:1, O:31, H:32, L:32, C:32, VB:32, VS:32>> <= CMap],
  Fun          = fun
                   ({N, Offset, Candle}) when LookupTS ->
                     ChunkOffset  = DataOffset + Offset,
                     {ok, Header} = file:pread(File, ChunkOffset, 10),
                     ?DBG("Reading chunk#~w timestamp at offset ~w+~w=~w: ~999p",
                       [N, DataOffset, Offset, ChunkOffset, Header]),
                     TS           = secdb_format:get_timestamp(Header),
                     {N, TS, Offset, Candle};
                   (Other) ->
                     Other
                   end,
  [Fun({N,O,C}) || {N,{O,C}} <- lists:zip(Seq, ChunkOffsets), O =/= 0].
