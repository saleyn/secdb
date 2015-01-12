-module(secdb_validator).
-include("secdb.hrl").
-include("../include/secdb.hrl").
-include("log.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([validate/1]).


validate(#db{path=Path, chunkmap = #cm{h=[],t=[]}, chunkmap_pos=CMOffset, window_sec=I} = DB) ->
  ChunkCount   = ?NUMBER_OF_CHUNKS(I),
  ChunkMapSize = ChunkCount*?OFFSETLEN_BITS div 8,
  GoodFileSize = CMOffset + ChunkMapSize + 1,  % 1 is for BOF marker
  case file:read_file_info(Path) of
    {ok, #file_info{size = Size}} when Size > GoodFileSize ->
      error_logger:error_msg("Empty database ~s is longer than required size, truncating all records~n", [Path]),
      {ok, F} = file:open(Path, [write,read,binary,raw]),
      file:position(F, GoodFileSize),
      file:truncate(F),
      file:close(F),
      DB;
    {ok, #file_info{size = Size}} when Size < GoodFileSize ->
      error_logger:error_msg("Empty database ~s is shorter and have broken chunk map, delete it~n", [Path]),
      DB;
    {ok, #file_info{size = GoodFileSize}} ->
      DB
  end;
  
validate(#db{path=Path, file=F, chunkmap=ChunkMap = #cm{}, window_sec= I, data_pos=Start} = DB) ->
  {Number, TS, Offset, Candle} = secdb_cm:last(ChunkMap),
  {ok, #file_info{size=Size}}  = file:read_file_info(Path),
  ChunkOffset                  = Start + Offset,
  ChunkLen                     = Size-ChunkOffset,
  {ok, EOCMmarker}             = file:pread(F, Start, 1),

  Candle =:= undefined orelse is_record(Candle, candle)
    orelse throw({invalid_last_candle, Number}),

  <<16#FF>> =:= EOCMmarker orelse
    throw({invalid_end_of_chunkmap_marker, Path}),

  {ok, LastChunk}  = file:pread(F, ChunkOffset, ChunkLen),
  
  State1 = case validate_chunk(LastChunk, DB, I, 0) of
    {ok, _DB} ->
      DB;
    {error, State1_, BadOffset} ->
      error_logger:error_msg("Database ~s is broken at offset ~B, truncating~n", [Path, BadOffset]),
      {ok, File} = file:open(Path, [write,read,binary,raw]),
      file:position(File, ChunkOffset + BadOffset),
      file:truncate(File),
      file:close(File),
      State1_
  end,
  
  Daystart = utc_to_daystart(TS),

  State1#db{
    daystart = Daystart,
    next_chunk_time = Daystart + timer:seconds(I) * (Number + 1)
  }.
  
utc_to_daystart(UTC) ->
  DayLength = timer:hours(24),
  DayTail = UTC rem DayLength,
  UTC - DayTail.


validate_chunk(Bin, DB, _Size, Offset) when byte_size(Bin) =:= 0; byte_size(Bin) =:= Offset->
  {ok, DB};
validate_chunk(Chunk, #db{} = DB, Size, Offset) ->
  % ?debugFmt("decode_packet ~B/~B ~B ~B ~p", [Offset, size(Chunk),Depth, Scale, MD]),
  case secdb_format:decode_packet(Chunk, Offset, DB) of
    {_Mode, _Packet, Offset1, #db{} = NewDB} when is_integer(Offset1) ->
      validate_chunk(Chunk, NewDB, Size, Offset1);
    {error, _Reason} ->
      {error, DB, Size - byte_size(Chunk)}
  end.
