
-record(md, {
  timestamp :: secdb:timestamp(),
  bid       :: [secdb:quote()],
  ask       :: [secdb:quote()]
}).

-record(trade, {
  timestamp :: secdb:timestamp(),
  side      :: buy | sell,
  price     :: secdb:price(),
  volume    :: secdb:volume(),
  aggressor :: undefined | boolean()
}).

-record(candle, {
  open  = 0 :: integer(),
  high  = 0 :: integer(),
  low   = 0 :: integer(),
  close = 0 :: integer(),
  vbuy  = 0 :: integer(),   % Buy Volume
  vsell = 0 :: integer()    % Sell Volume
}).

-type open_option() :: {depth, non_neg_integer()} | {scale, non_neg_integer()} |
                       {interval, non_neg_integer()} |
                       {date, term()} | {symbol, secdb:symbol()}.

-type filter() :: candle | {candle, md | trade | both} | average.
-type reader_option() :: {filter, filter()} | {range, secdb:timestamp(), secdb:timestamp()}.


-type iterator() :: term().