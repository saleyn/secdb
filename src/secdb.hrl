-record(cm, {h = [] :: list(), t = [] :: list()}).

% Database state record
-record(db, {
    version,
    sync = true,
    file,
    path,
    mode                  :: read|append,
    buffer,
    buffer_end,
    symbol,
    date                  :: calendar:date(), %% UTC date
    depth,
    scale,
    window_sec,                               %% Number of seconds in a chunk
    cur_chunk_num = 0     :: integer(),       %% Current chunk number
    daycandle_pos         :: integer(),
    chunkmap_pos          :: integer(),
    data_pos              :: integer(),
    daycandle,         %% :: undefined | #candle{}
    candle,            %% :: undefined | #candle{}  % last chunk's candle
    daystart              :: integer(),
    last_ts         = 0   :: integer(),
    last_quote   = {0,0}  :: {integer(), integer()},
    next_chunk_time = 0,
    chunkmap        = #cm{}
  }).

-define(SECDB_VERSION, 100).
-define(DAY_USECS, 86400*1000000).            %% Microseconds in a day

-define(SECDB_OPTIONS, [
    {version,       ?SECDB_VERSION},
    {symbol,        undefined},
    {date,          utcdate()},
    {depth,         10},
    {scale,         100},
    {window_sec,      300} % seconds
  ]).

-define(OFFSETLEN_BYTES, 4).
-define(OFFSETLEN_BITS, 32).
-define(CANDLE_SIZE,   6*4).

-define(NUMBER_OF_CHUNKS(ChunkSize), (24*3600 div ChunkSize) + 1).

-define(assertEqualEps(Expect, Expr, Eps),
    ((fun (__X) ->
        case abs(Expr - Expect) of
        __Y when __Y < Eps -> ok;
        __V -> erlang:error({assertEqualEps_failed,
                      [{module, ?MODULE},
                       {line, ?LINE},
                       {expression, (??Expr)},
                       {epsilon, io_lib:format("~.5f", [Eps*1.0])},
                       {expected, io_lib:format("~.4f", [Expect*1.0])},
                       {value, io_lib:format("~.4f", [Expr*1.0])}]})
        end
      end)(Expect))).

-define(_assertEqualEps(Expect, Expr), ?_test(?assertEqualEps(Expect, Expr))).

-define(assertEqualMD(MD1, MD2), 
  secdb_test_helper:assertEqualMD(MD1, MD2, [{module,?MODULE},{line,?LINE},{md1,(??MD1)},{md2,(??MD2)}])).
