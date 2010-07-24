%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is Toke.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(toke).

-export([new/1, delete/1, tune/5, set_cache/2, set_xm_size/2, set_df_unit/2,
         open/3, close/1, insert/3, insert_new/3, insert_concat/3,
         insert_async/3, delete/2, get/2, fold/3, stop/1, init/1]).

%% Tokyo Cabinet driver for Erlang

%% Only implements a skeleton of the hash table API.  All keys and
%% values must be binaries. Whilst the driver is robust, there is no
%% attempt to check state. Thus if you, eg, call tune after opening
%% the database, you will get an invalid state error from Tokyo
%% Cabinet rather than something more meaningful.

-define(LIBNAME, "libtoke").

-define(TOKE_NEW,           0). %% KEEP IN SYNC WITH TOKE.H
-define(TOKE_DEL,           1).
-define(TOKE_TUNE,          2).
-define(TOKE_SET_CACHE,     3).
-define(TOKE_SET_XM_SIZE,   4).
-define(TOKE_SET_DF_UNIT,   5).
-define(TOKE_OPEN,          6).
-define(TOKE_CLOSE,         7).
-define(TOKE_INSERT,        8).
-define(TOKE_INSERT_NEW,    9).
-define(TOKE_INSERT_CONCAT, 10).
-define(TOKE_INSERT_ASYNC,  11).
-define(TOKE_DELETE,        12).
-define(TOKE_GET,           13).
-define(TOKE_GET_ALL,       14).

%% KEEP IN SYNC WITH TOKE.H
-define(TUNE_KEYS,          [large, deflate, bzip, tcbs, excodec]).
-define(OPEN_KEYS,          [read, write, create, truncate, no_lock,
                             lock_no_block, sync_on_transaction]).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
init(_Config) ->
    erl_ddll:start(),
    {file, Path} = 
			case code:is_loaded(toke_drv) of
				false -> 
					code:load_file(toke_drv),
					code:is_loaded(toke_drv);
				Path1 ->
					Path1
			end,
    Dir = filename:join(filename:dirname(Path), "../priv"),
    ok = erl_ddll:load_driver(Dir, ?LIBNAME),
    Port = open_port({spawn_driver, ?LIBNAME}, [binary, stream]),
    {ok, Port}.

new(Port) ->
    port_command(Port, <<?TOKE_NEW/native>>),
    simple_reply(Port).

delete(Port) ->
    port_command(Port, <<?TOKE_DEL/native>>),
    simple_reply(Port).

%% int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts
tune(Port, BNum, APow, FPow, Opts) ->
    Opt = build_bit_mask(Opts, ?TUNE_KEYS),
    port_command(Port, <<?TOKE_TUNE/native,
                        BNum:64/signed-integer-native,
                        APow:8/signed-integer-native,
                        FPow:8/signed-integer-native,
                        Opt:8/native>>),
    simple_reply(Port).

%% int32_t rcnum
set_cache(Port, RecordCacheNum) ->
    port_command(Port, <<?TOKE_SET_CACHE/native,
                        RecordCacheNum:32/signed-integer-native>>),
    simple_reply(Port).

%% int64_t xmsiz
set_xm_size(Port, ExtraMappedMemory) ->
    port_command(Port, <<?TOKE_SET_XM_SIZE/native,
                        ExtraMappedMemory:64/signed-integer-native>>),
    simple_reply(Port).

%% int32_t dfunit
set_df_unit(Port, DefragStepUnit) ->
    port_command(Port, <<?TOKE_SET_DF_UNIT/native,
                        DefragStepUnit:32/signed-integer-native>>),
    simple_reply(Port).

open(Port, Path, Modes) ->
    Mode = build_bit_mask(Modes, ?OPEN_KEYS),
    port_command(Port, <<?TOKE_OPEN/native, (length(Path)):64/native,
                        (list_to_binary(Path))/binary, Mode:8/native>>),
    simple_reply(Port).

close(Port) ->
    port_command(Port, <<?TOKE_CLOSE/native>>),
    simple_reply(Port).

insert(Port, Key, Value) when is_binary(Key) andalso is_binary(Value) ->
    insert_sync(Port, ?TOKE_INSERT, Key, Value).

insert_new(Port, Key, Value) when is_binary(Key) andalso is_binary(Value) ->
    insert_sync(Port, ?TOKE_INSERT_NEW, Key, Value).

insert_concat(Port, Key, Value) when is_binary(Key) andalso is_binary(Value) ->
    insert_sync(Port, ?TOKE_INSERT_CONCAT, Key, Value).

delete(Port, Key) when is_binary(Key) ->
    KeySize = size(Key),
    port_command(Port, <<?TOKE_DELETE/native, KeySize:64/native, Key/binary>>),
    simple_reply(Port).

get(Port, Key) when is_binary(Key) ->
    KeySize = size(Key),
    port_command(Port, <<?TOKE_GET/native, KeySize:64/native, Key/binary>>),
    simple_reply(Port).

fold(Port, Fun, Init) ->
    port_command(Port, <<?TOKE_GET_ALL/native>>),
    Result = receive_all(Fun, Init),
    Result.

stop(Port) ->
    port_close(Port).

insert_async(Port, Key, Value) when is_binary(Key) andalso is_binary(Value) ->
    insert_async(Port, ?TOKE_INSERT_ASYNC, Key, Value),
    {noreply, Port}.

%%----------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------

build_bit_mask(Flags, Keys) ->
    {Int, _Index} =
        lists:foldl(fun (Key, {Acc, Index}) ->
                            {case proplists:get_bool(Key, Flags) of
                                 true  -> Acc bor (1 bsl Index);
                                 false -> Acc
                             end, 1 + Index}
                    end, {0, 0}, Keys),
    Int.

insert_async(Port, Command, Key, Value) ->
    KeySize = size(Key),
    ValueSize = size(Value),
    port_command(Port, <<Command/native, KeySize:64/native,
                        Key/binary, ValueSize:64/native, Value/binary>>),
    {noreply, Port}.

insert_sync(Port, Command, Key, Value) ->
    insert_async(Port, Command, Key, Value),
    simple_reply(Port).

simple_reply(_Port) ->
    receive 
      {toke_reply, Result} -> Result 
    end.

receive_all(Fun, Acc) ->
    receive
        {toke_reply, ok}         -> Acc;
        {toke_reply, Key, Value} -> receive_all(Fun, Fun(Key, Value, Acc))
    end.
