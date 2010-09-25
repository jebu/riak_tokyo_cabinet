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
-record(state, {base_dir = undefined}).

start(Partition, _Config) ->
    application:load(riak_tokyo_cabinet),
    DataDir = get_env(riak_tokyo_cabinet, data_root, "data/tokyo_cabinet"),
    TokyoCabinetBase = filename:join([DataDir, 
                                      integer_to_list(Partition)]),
    TokyoCabinetMarker = filename:join([TokyoCabinetBase,
                                      "0"]),
    case filelib:ensure_dir(TokyoCabinetMarker) of
        ok -> ok;
        {error, eexist} -> ok;
        {error, Reason} ->
            error_logger:error_msg("Failed to create tokyo cabinet dir ~s: ~p\n",
                                   [TokyoCabinetMarker, Reason]),
            riak:stop("riak_kv_tokyo_cabinet_backend failed to start.")
    end,
    {ok, #state{base_dir = TokyoCabinetBase}}.

stop(State) ->
    lists:map(fun({Bucket, Toke}) -> 
        toke:close(Toke),
        remove_toke_bucket(State, Bucket)
    end, get_all_toke_bucket_list(State)),
    ok.


get(State, {Bucket, Key}) ->
    Toke = get_toke_for_bucket(Bucket, State),
    case toke:get(Toke, Key) of
        not_found  ->
            {error, notfound};
        Value ->
            {ok, Value}
    end.

put(State, {Bucket, Key}, Val) ->
    Toke = get_toke_for_bucket(Bucket, State),
    case toke:insert(Toke, Key, Val) of
        ok -> ok;
        Reason ->
            {error, Reason}
    end.

delete(State, {Bucket, Key}) ->
    Toke = get_toke_for_bucket(Bucket, State),
    case toke:delete(Toke, Key) of
        ok -> ok;
        Reason ->
            {error, Reason}
    end.

list(State) ->
  % list all keys in all buckets
    Fun = fun(K,_,Acc) ->
      [K | Acc]
    end,
    case fold(State, Fun, []) of
        KeyList when is_list(KeyList) ->
            KeyList;
        _ ->
            []
    end.

list_bucket(State, {filter, Bucket, Fun}) ->
    Toke = get_toke_for_bucket(Bucket, State),
    toke:fold(Toke,
                fun(K, _, Acc) ->
                    case Fun(K) of
                        true -> [K | Acc];
                        false -> Acc
                    end
                end,
             []); 
list_bucket(State, '_') ->
    lists:foldl(fun({Bucket, _}, Acc) ->
      [Bucket | Acc]
    end, [], get_all_toke_bucket_list(State));
list_bucket(State, Bucket) ->
    Toke = get_toke_for_bucket(Bucket, State),
    toke:fold(Toke,
                fun(K, _, Acc) ->
                        [K | Acc]
                end,
             []). 

fold(State, Fun0, Acc0) ->
    lists:foldl(fun({Bucket, Toke}, Acc1) ->
        toke:fold(Toke,
                    fun(K, V, Acc2) ->
                         Fun0({Bucket, K}, V, Acc2)
                    end,
                 Acc1) 
     end, Acc0, get_all_toke_bucket_list(State)).

drop(State) ->
    lists:map(fun({Bucket, Toke}) -> 
        toke:close(Toke),
        toke:delete(Toke),
        remove_toke_bucket(State, Bucket)
    end, get_all_toke_bucket_list(State)).

is_empty(State) ->
    F = fun(_K, _V, Acc) ->
          Acc + 1
        end,
    case fold(State, F, 0) of
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
%
get_toke_for_bucket(Bucket, State) ->
  case erlang:get(Bucket) of
    undefined -> 
      Toke = init_tokyo_cabinet_for_bucket(Bucket, State),
      erlang:put(Bucket, Toke),
      erlang:put({Bucket, Toke}, toke_ports),
      Toke;
    Toke ->
      Toke
  end.
%
get_all_toke_bucket_list(_State) ->
  erlang:get_keys(toke_ports).
%
remove_toke_bucket(_State, Bucket) ->
  case erlang:get(Bucket) of
    undefined -> ok;
    Toke ->
      erlang:erase(Bucket),
      erlang:erase({Bucket, Toke})
  end.
%
init_tokyo_cabinet_for_bucket(Bucket, State) ->
  BucketStr = unicode:characters_to_list(Bucket) ++ ".tch",
  TokyoCabinetFile = filename:join([State#state.base_dir,
                                    BucketStr]),
  case filelib:ensure_dir(TokyoCabinetFile) of
      ok -> ok;
      {error, eexist} -> ok;
      {error, Reason} ->
          error_logger:error_msg("Failed to create tokyo cabinet dir ~s: ~p\n",
                                 [TokyoCabinetFile, Reason]),
          riak:stop("riak_kv_tokyo_cabinet_backend failed to create file.")
  end,

  {ok, Toke} = toke:init([]),
  toke:new(Toke),
  toke:set_cache(Toke, get_env(riak_tokyo_cabinet, records_to_cache, 100000)),
  toke:set_xm_size(Toke, get_env(riak_tokyo_cabinet, hdb_xm_size, 67108864)),
  toke:set_df_unit(Toke, get_env(riak_tokyo_cabinet, hdb_df_unit, 0)),
  toke:tune(Toke, get_env(riak_tokyo_cabinet, bucket_size, 50000), 
                  get_env(riak_tokyo_cabinet, hdb_apow, 5), 
                  get_env(riak_tokyo_cabinet, hdb_fpow, 15),
                  get_env(riak_tokyo_cabinet, hdb_options, [large])),

  case toke:open(Toke, TokyoCabinetFile, get_env(riak_tokyo_cabinet, hdb_open_options, [read, write, create])) of
      ok ->
          Toke;
      Error ->
          error_logger:error_msg("Failed to create tokyo cabinet file ~s: ~p\n",
                                 [TokyoCabinetFile, Error]),
          riak:stop("riak_kv_tokyo_cabinet_backend failed to create file.")
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
