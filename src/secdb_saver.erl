-module(secdb_saver).

-include("secdb.hrl").
-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").


-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, 
  terminate/2, code_change/3]).

-export([identity/2]).
-export([add_handler/3, add_sup_handler/3]).

-record(saver, {
  db,
  symbol,
  date,
  transform
}).


add_handler(Handler, Symbol, Options) ->
  add_specific_handler(Handler, Symbol, Options, add_handler).

add_sup_handler(Handler, Symbol, Options) ->
  add_specific_handler(Handler, Symbol, Options, add_sup_handler).



add_specific_handler(Handler, Symbol, Options, Fun) ->
  Module = case proplists:get_value(id, Options) of
    undefined -> ?MODULE;
    Id -> {?MODULE, Id}
  end,
  case lists:member(Module, gen_event:which_handlers(Handler)) of
    true -> ok;
    false -> ok = gen_event:Fun(Handler, Module, [Symbol|Options])
  end.



identity(Event, _) -> Event.

init([Symbol|Options]) ->
  {Date, _} = calendar:universal_time(),
  {ok,DB} = secdb:open_append(Symbol, Date, Options),
  Transform = proplists:get_value(transform, Options, {?MODULE, identity, []}),
  {ok, #saver{db = DB, symbol = Symbol, date = Date, transform = Transform}}.


handle_event(RawEvent, #saver{transform = {M,F,A}, db = DB1} = Saver) ->
  Events = case M:F(RawEvent, A) of
    undefined -> [];
    List when is_list(List) -> List;
    Evt when is_tuple(Evt) -> [Evt]
  end,
  DB2 = lists:foldl(fun(Event, DB) -> secdb:append(Event, DB) end, DB1, Events),
  {ok, Saver#saver{db = DB2}}.


handle_info(_Msg, #saver{} = Saver) ->
  {ok, Saver}.


handle_call(Call, Saver) ->
  {ok, {unknown_call, Call}, Saver}.


terminate(_,#saver{db = DB} = _Saver) -> 
  secdb:close(DB).

code_change(_,Saver,_) -> {ok,Saver}.
