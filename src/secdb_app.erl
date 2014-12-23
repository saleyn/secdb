-module(secdb_app).
-export([start/2, stop/1]).

start(_, _) ->
  secdb_sup:start_link().

stop(_) ->
  secdb_sup:stop().
