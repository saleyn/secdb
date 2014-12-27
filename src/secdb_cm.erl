%%%-----------------------------------------------------------------------------
%%% @author Serge Aleynikov
%%% @copyright (C) 2014, Omnibius, LLC
%%% @doc Internal representation of chunkmap
%%% @end
%%%-----------------------------------------------------------------------------
%%% Created: 2014-12-25
%%%-----------------------------------------------------------------------------
-module(secdb_cm).
-author('saleyn@gmail.com').

%% API
-export([new/0, new/1, push/2, pop/1, normalize/1, last/1]).
-export_type([chunkmap/0]).

-include("secdb.hrl").

-type chunkmap() :: #cm{}.

-spec new() -> chunkmap().
new() -> #cm{}.

-spec new(Chunks::[{Num::integer(), Timestamp::integer(), Offset::integer()}]) -> chunkmap().
new(Chunks) -> #cm{t = Chunks}.

%% @doc Append a chunk to a chunkmap list or to prepend it to the reverse list
-spec push(Chunk::tuple(), ChunkMap::chunkmap()) -> NewChunkMap::chunkmap().
push(Chunk, #cm{h=H, t=T}) -> #cm{h=[Chunk | H], t=T}.

%% @doc Fetch first chunk from the chunkmap
-spec pop(ChunkMap::chunkmap()) ->
  {Chunk::undefined | tuple(), NewCM::chunkmap()}.
pop(#cm{h=L,  t=[H|T]}) -> {H, #cm{h=L, t=T}};
pop(#cm{h=[], t=[]}=CM) -> {undefined, CM};
pop(#cm{h=L,  t=[]})    -> [H|T] = lists:reverse(L), {H, #cm{h=[], t=T}}.

%% @doc Return the chunkmap in the proper order
-spec normalize(ChunkMap::chunkmap()) ->
  {Chunks::list(), NormalChunkMap::chunkmap()}.
normalize(#cm{h=[], t=TL} = CM)  -> {TL, CM};
normalize(#cm{h=HL, t=TL})       ->
  L = TL ++ lists:reverse(HL),
  {L, #cm{h=[], t=L}};
normalize(undefined) ->
  {[], #cm{}}.

%% @doc Get the last chunk from non-empty chunkmap
-spec last(chunkmap()) -> Chunk::tuple().
last(#cm{h=[],    t=L}) when L =/= [] -> lists:last(L);
last(#cm{h=[H|_], t=_})               -> H.
