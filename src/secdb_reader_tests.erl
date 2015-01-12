-module(secdb_reader_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("../include/secdb.hrl").

-compile(export_all).

-import(secdb_test_helper, [tempfile/1, tempdir/0, chunk_content/1]).

file_info_test() ->
  File = tempfile("write-append-test.temp"),
  ok = filelib:ensure_dir(File),
  file:delete(File),

  secdb_appender:write_events(File, chunk_content('109') ++ chunk_content('110_1'), 
    [{symbol, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 100}, {window_sec, 300}]),

  ?assertEqual([{date,{2012,7,25}},{scale,200},{depth,3}, {presence,{289,[109,110]}}],
    secdb_reader:file_info(File, [date, scale, depth, presence])),
  ?assertEqual([{candle, #candle{open=12.33,high=12.45,low=12.23,close=12.445,vbuy=5990}}],
    secdb_reader:file_info(File, [candle])),
  file:delete(File).


candle_test() ->
  Root = application:get_env(secdb, root, undefined),
  ok   = application:set_env(secdb, root, tempdir()),
  file:delete(secdb_fs:path('TEST', "2012-07-25")),
  MD   = chunk_content('109') ++ chunk_content('110_1'),
  secdb:write_events('TEST', "2012-07-25", MD, [{scale, 1000}, {depth, 3}]),
  Candle = secdb:candle('TEST', "2012-07-25"),
  application:set_env(secdb, root, Root),
  file:delete(secdb_fs:path('TEST', "2012-07-25")),
  ?assertEqual(#candle{open=12.33,high=12.45,low=12.23,close=12.445, vbuy=0, vsell=0}, Candle),
  ok.
