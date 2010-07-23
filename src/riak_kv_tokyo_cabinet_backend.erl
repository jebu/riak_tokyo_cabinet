%% -------------------------------------------------------------------
%%
%% riak_kv_tokyo_cabinet_backend: Tokyo Cabinet Driver for Riak
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% This is fully based on riak_kv_bitcask_backend
%% 
%% -------------------------------------------------------------------

-module(riak_kv_tokyo_cabinet_backend).
-behavior(riak_kv_backend).
-author('Jebu Ittiachen <jebu@iyottasoft.com>').

-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         drop/1,
         is_empty/1,
         callback/3]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MERGE_CHECK_INTERVAL, timer:minutes(3)).

start(Partition, _Config) ->
    DataDir = get_env(riak_tokyo_cabinet, data_root, "data/tokyo_cabinet"),

    TokyoCabinetRoot = filename:join([DataDir,
                                 integer_to_list(Partition)]),
    case filelib:ensure_dir(TokyoCabinetRoot) of
        ok ->
            ok;
        {error, Reason} ->
            error_logger:error_msg("Failed to create tokyo cabinet dir ~s: ~p\n",
                                   [TokyoCabinetRoot, Reason]),
            riak:stop("riak_kv_tokyo_cabinet_backend failed to start.")
    end,

    {ok, Toke} = toke:init(_Config),
    toke:new(Toke),
    toke:set_cache(Toke, get_env(riak_tokyo_cabinet, records_to_cache, 100000)),
    toke:tune(Toke, get_env(riak_tokyo_cabinet, bucket_size, 50000), 
                    get_env(riak_tokyo_cabinet, hdb_apow, 5), 
                    get_env(riak_tokyo_cabinet, hdb_fpow, 15),
                    get_env(riak_tokyo_cabinet, hdb_options, [large])),

    case toke:open(Toke, TokyoCabinetRoot, get_env(riak_tokyo_cabinet, hdb_open_options, [read, write, create])) of
        ok ->
            {ok, Toke};
        Error ->
            {error, Error}
    end.


stop(Toke) ->
    toke:close(Toke).


get(Toke, BKey) ->
    Key = term_to_binary(BKey),
    case toke:get(Toke, Key) of
        not_found  ->
            {error, notfound};
        Value ->
            {ok, Value}
    end.

put(Toke, BKey, Val) ->
    Key = term_to_binary(BKey),
    case toke:insert(Toke, Key, Val) of
        ok -> ok;
        Reason ->
            {error, Reason}
    end.

delete(Toke, BKey) ->
    case toke:delete(Toke, term_to_binary(BKey)) of
        ok -> ok;
        Reason ->
            {error, Reason}
    end.

list(Toke) ->
    KeysFun = fun(K,_,Acc) ->
      [binary_to_term(K) | Acc]
    end,
    case toke:fold(Toke, KeysFun, []) of
        KeyList when is_list(KeyList) ->
            KeyList;
        Other ->
            Other
    end.

list_bucket(State, {filter, Bucket, Fun}) ->
    [K || {B, K} <- ?MODULE:list(State),
          B =:= Bucket,
          Fun(K)];
list_bucket(State, '_') ->
    [B || {B, _K} <- ?MODULE:list(State)];
list_bucket(State, Bucket) ->
    [K || {B, K} <- ?MODULE:list(State), B =:= Bucket].


fold(Toke, Fun0, Acc0) ->
    toke:fold(Toke,
                 fun(K, V, Acc) ->
                         Fun0(binary_to_term(K), V, Acc)
                 end,
                 Acc0).

drop(Toke) ->
    toke:close(Toke),
    toke:delete(Toke),
    ok.

is_empty(Toke) ->
    %% Determining if a tokyo cabinet is empty requires us to find at least
    %% one value that is NOT a tombstone. Accomplish this by doing a fold
    %% that forcibly bails on the very first k/v encountered.
    F = fun(_K, _V, Acc) ->
          Acc + 1
        end,
    case toke:fold(Toke, F, 0) of
      0 -> true;
      _ -> false
    end.
%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================
get_env(App, ConfParam, Default) ->
  case application:get_env(App, ConfParam) of
    {ok, Val} -> Val;
    _ -> Default
  end.
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
standard_test(BackendMod, Config) ->
    {ok, S} = BackendMod:start(42, Config),
    ?assertEqual(ok, BackendMod:put(S,{<<"b1">>,<<"k1">>},<<"v1">>)),
    ?assertEqual(ok, BackendMod:put(S,{<<"b2">>,<<"k2">>},<<"v2">>)),
    ?assertEqual({ok,<<"v2">>}, BackendMod:get(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b1">>,<<"k3">>})),
    ?assertEqual([{<<"b1">>,<<"k1">>},{<<"b2">>,<<"k2">>}],
                 lists:sort(BackendMod:list(S))),
    ?assertEqual([<<"k2">>], BackendMod:list_bucket(S, <<"b2">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(S, <<"b1">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(
                               S, {filter, <<"b1">>, fun(_K) -> true end})),
    ?assertEqual([], BackendMod:list_bucket(
                       S, {filter, <<"b1">>, fun(_K) -> false end})),
    BucketList = BackendMod:list_bucket(S, '_'),
    ?assert(lists:member(<<"b1">>, BucketList)),
    ?assert(lists:member(<<"b2">>, BucketList)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b2">>, <<"k2">>})),
    ?assertEqual([{<<"b1">>, <<"k1">>}], BackendMod:list(S)),
    Folder = fun(K, V, A) -> [{K,V}|A] end,
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>}], BackendMod:fold(S, Folder, [])),
    ?assertEqual(ok, BackendMod:put(S,{<<"b3">>,<<"k3">>},<<"v3">>)),
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                  {{<<"b3">>,<<"k3">>},<<"v3">>}], lists:sort(BackendMod:fold(S, Folder, []))),
    ?assertEqual(false, BackendMod:is_empty(S)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b1">>,<<"k1">>})),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b3">>,<<"k3">>})),
    ?assertEqual(true, BackendMod:is_empty(S)),
    ok = BackendMod:stop(S).

simple_test() ->
    ?assertCmd("rm -rf test/tokyo-cabinet-backend"),
    application:load(riak_tokyo_cabinet),
    application:set_env(riak_tokyo_cabinet, data_root, "test/tokyo_cabinet-backend"),
    standard_test(?MODULE, []).

-endif.
