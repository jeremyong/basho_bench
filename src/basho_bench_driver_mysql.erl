%% -------------------------------------------------------------------
%%
%% basho_bench_driver_mysql: Driver for mysql
%%
%% Copyright (c) 2013 Jeremy Ong
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
%% -------------------------------------------------------------------
-module(basho_bench_driver_mysql).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { }).

-define(TIMEOUT_GENERAL, 62*1000).              % Riak PB default + 2 sec

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(emysql) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,
    
    crypto:start(),
    emysql:start(),

    Ip  = basho_bench_config:get(mysql_ip, "127.0.0.1"),
    Port  = basho_bench_config:get(mysql_port, 3306),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    User  = basho_bench_config:get(mysql_user, "test"),
    Pass  = basho_bench_config:get(mysql_pass, "test"),
    Database  = basho_bench_config:get(mysql_database, "test"),
    Table  = basho_bench_config:get(mysql_table, "test"),
    
    case Id of
        1 ->
            emysql:add_pool(test_pool, 30, User, Pass, Ip, Port, Database, utf8);
        _ ->
            ok
    end,
    
    emysql:prepare(get, <<"SELECT * FROM test WHERE id = ?">>),
    emysql:prepare(put, <<"INSERT INTO test VALUES (?, ?)">>),
    emysql:prepare(upd, <<"REPLACE INTO test VALUES (?, ?)">>),
    emysql:prepare(del, <<"DELETE FROM test WHERE id = ?">>),

    {ok, #state{}}.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),

    case element(1, emysql:execute(test_pool, get, [Key])) of
        result_packet ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = base64:encode(ValueGen()),
    case element(1, emysql:execute(test_pool, put, [Key, Value])) of
        ok_packet ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = base64:encode(ValueGen()),
    case element(1, emysql:execute(test_pool, upd, [Key, Value])) of
        ok_packet ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case element(1, emysql:execute(test_pool, del, [Key])) of
        ok_packet ->
            {ok, State};
        Error ->
            {error, Error, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
