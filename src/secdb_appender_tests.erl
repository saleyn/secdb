-module(secdb_appender_tests).
-include_lib("eunit/include/eunit.hrl").

-include("../include/secdb.hrl").

-export([append_bm_test/2]).

-import(secdb_test_helper, [tempfile/1, tempdir/0, fixturefile/1, ensure_states_equal/2, write_events_to_file/2, append_events_to_file/2, ensure_packets_equal/2, chunk_content/1]).


file_create_test() ->
  check_creation_params([{symbol, 'TEST'}, {date, {2012,7,26}}, {depth, 10}, {scale, 100}, {window_sec, 300}]),
  check_creation_params([{symbol, 'TEST'}, {date, {2012,7,25}}, {depth, 15}, {scale, 200}, {window_sec, 600}]).

check_creation_params(DBOptions) ->
  File = tempfile("creation-test.temp"),
  file:delete(File),
  ok = filelib:ensure_dir(File),
  % ok = file:write_file(File, "GARBAGE"),

  {ok, S} = secdb_appender:open(File, DBOptions),
  ok = secdb_appender:close(S),
  ok = file:delete(File).

append_typed_symbol_test() ->
  application:load(secdb),
  application:set_env(secdb, root, tempdir()),
  Path = tempdir() ++ "/daily/2012/07/TEST.2012-07-25.secdb",
  file:delete(Path),
  {ok, DB} = secdb:open_append({daily, 'TEST'}, "2012-07-25", [{depth,3}]),
  Info = secdb:info(DB),
  ?assertEqual(Path, proplists:get_value(path, Info)),
  file:delete(Path),
  ok.

append_bm_test() ->
  append_bm_test(10000, {2012, 7, 25}).

append_bm_test(Count, Date) ->
  File = tempfile("append-bm-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  StartTS = (calendar:datetime_to_gregorian_seconds({Date,{6,0,0}}) - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}))*1000,
  {ok, S0} = secdb_appender:open(File, [nosync, {symbol, 'TEST'},{date, Date}, {depth, 1}, {scale, 100}]),
  T1 = erlang:now(),
  S1 = fill_records(S0, Count, 10, StartTS),
  T2 = erlang:now(),
  secdb_appender:close(S1),
  Delta = timer:now_diff(T2,T1),
  ?debugFmt("Append benchmark: ~B in ~B ms, about ~B us per row (file size=~.1f MB)",
    [Count, Delta div 1000, Delta div Count, filelib:file_size(File) / (1024*1024)]),
  file:delete(File),
  ok.

fill_records(S0, Count, _, _) when Count =< 0 ->
  S0;

fill_records(S0, Count, Step, TS) ->
  S1 = lists:foldl(fun(E, S) -> secdb:append(E, S) end, S0,
    [
      {md, TS+0*Step, [{43.15, 50}], [{45.15, 50}]},
      {md, TS+1*Step, [{43.09, 40}], [{45.05, 50}]},
      {md, TS+2*Step, [{42.50, 20}], [{44.95, 20}]},
      {md, TS+3*Step, [{42.05, 90}], [{43.15, 50}]},
      {md, TS+4*Step, [{42.45, 50}], [{44.15, 40}]},
      {md, TS+5*Step, [{41.55, 54}], [{44.15, 50}]},
      {md, TS+6*Step, [{42.12, 10}], [{45.15, 30}]},
      {md, TS+7*Step, [{42.80, 90}], [{43.15, 80}]},
      {md, TS+8*Step, [{43.12, 20}], [{44.15, 50}]},
      {md, TS+9*Step, [{43.45, 20}], [{45.15, 50}]}
    ]),
  fill_records(S1, Count - 10, Step, TS + 10*Step).



write_append_test() ->
  File = tempfile("write-append-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  {ok, S0} = secdb_appender:open(File, [{symbol, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {window_sec, 300}]),
  S1 = lists:foldl(fun(Event, State) -> secdb_appender:append(Event, State) end,
      S0, chunk_content('109') ++ [hd(chunk_content('110_1'))]),
  ok = secdb_appender:close(S1),

  {ok, S1_1} = secdb_appender:open(File, []),
  ensure_states_equal(S1, S1_1),

  S1_2 = lists:foldl(fun(Event, State) -> secdb_appender:append(Event, State) end,
    S1_1, tl(chunk_content('110_1'))),
  ok = secdb_appender:close(S1_2),

  {ok, S2} = secdb_appender:open(File, []),
  ensure_states_equal(S1_2, S2),
  
  S3 = lists:foldl(fun(Event, State) -> secdb_appender:append(Event, State) end,
    S2, chunk_content('110_2') ++ chunk_content('112')),
  ok = secdb_appender:close(S3),
  
  % {ok, S4_} = secdb_raw:open(File, Options ++ [read]),
  % {ok, S4} = secdb_raw:restore_state(S4_),
  % ensure_states_equal(S3, S4),
  % ok = secdb_raw:close(S4),

  FileEvents = secdb:events({path, File}, undefined),

  lists:zipwith(fun(Expected, Read) -> ensure_packets_equal(Expected, Read) end,
    chunk_content('109') ++ chunk_content('110_1') ++ chunk_content('110_2') ++ chunk_content('112'),
    FileEvents),
  ok = file:delete(File).


append_verifier_test() ->
  File = tempfile("append-verifier-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  {ok, S0} = secdb_appender:open(File, [{symbol, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {window_sec, 300}]),

  ?assertThrow({_, bad_timestamp, _}, secdb_appender:append(#trade{price=2.34, volume=29}, S0)),
  ?assertThrow({_, bad_price, _}, secdb_appender:append(#trade{timestamp=1350575093098, volume=29}, S0)),
  ?assertThrow({_, bad_volume, _}, secdb_appender:append(#trade{timestamp=1350575093098, price=2.34, volume=-29}, S0)),

  ?assertThrow({_, bad_timestamp, _}, secdb_appender:append({md, undefined, [{2.34, 29}], [{2.35, 31}]}, S0)),
  ?assertThrow({_, bad_bid, _}, secdb_appender:append({md, 1350575093098, [], [{2.35, 31}]}, S0)),
  ?assertThrow({_, bad_bid, _}, secdb_appender:append({md, 1350575093098, undefined, [{2.35, 31}]}, S0)),
  ?assertThrow({_, bad_ask, _}, secdb_appender:append({md, 1350575093098, [{2.34, 29}], [{2.35, -31}]}, S0)),
  ?assertThrow({_, bad_ask, _}, secdb_appender:append({md, 1350575093098, [{2.34, 29}], [{[], 31}]}, S0)),

  ?assertThrow({_, invalid_event, _}, secdb_appender:append({other, 1350575093098, [{2.34, 29}], [{12, 31}]}, S0)),

  ok = secdb_appender:close(S0),
  ok = file:delete(File).
