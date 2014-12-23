
-record(md, {
  timestamp :: secdb:timestamp(),
  bid       :: [secdb:quote()],
  ask       :: [secdb:quote()]
}).

-record(trade, {
  timestamp :: secdb:timestamp(),
  price     :: secdb:price(),
  volume    :: secdb:volume(),
  aggressor :: undefined | boolean()
}).


-type open_option() :: {depth, non_neg_integer()} | {scale, non_neg_integer()} | {chunk_size, non_neg_integer()} |
                       {date, term()} | {symbol, secdb:symbol()}.

-type filter() :: candle | average.
-type reader_option() :: {filter, filter()} | {range, secdb:timestamp(), secdb:timestamp()}.


-type iterator() :: term().
