-module(basho_bench_driver_riakpool).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { bucket, r, w, dw, timeout_read, timeout_write, rw }).

-define(TIMEOUT_GENERAL, 62*1000).              % Riak PB default + 2 sec

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakpool) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,
    
    application:start(riakpool),

    Ip  = basho_bench_config:get(riakc_pb_ip, "127.0.0.1"),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    Replies = basho_bench_config:get(riakc_pb_replies, quorum),
    R = basho_bench_config:get(riakc_pb_r, Replies),
    W = basho_bench_config:get(riakc_pb_w, Replies),
    DW = basho_bench_config:get(riakc_pb_dw, Replies),
    RW = basho_bench_config:get(riakc_pb_rw, Replies),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),
    
    riakpool:start_pool(Ip, Port),

    {ok, #state{bucket = Bucket, rw = RW,
                r = R, w = W, dw = DW, timeout_read = 62000, timeout_write = 62000}}.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakpool:execute(fun(C) ->
                                  riakc_pb_socket:get(C, State#state.bucket, Key,
                                                      [{r, State#state.r}], State#state.timeout_read)
                          end) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(get_existing, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakpool:execute(fun(C) ->
                                  riakc_pb_socket:get(C, State#state.bucket, Key,
                                                      [{r, State#state.r}], State#state.timeout_read)
                          end) of
        {ok, {ok, _}} ->
            {ok, State};
        {ok, {error, notfound}} ->
            {error, {not_found, Key}, State};
        {ok, {error, Reason}} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
    Robj = riakc_obj:update_value(Robj0, base64:encode(ValueGen())),
    case riakpool:execute(fun(C) ->
                                  riakc_pb_socket:put(C, Robj, [{w, State#state.w},
                                                                {dw, State#state.dw}], State#state.timeout_write)
                          end) of
        {ok, ok} ->
            {ok, State};
        {ok, {error, Reason}} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakpool:execute(fun(C) ->
                                  riakc_pb_socket:get(C, State#state.bucket,
                                                      Key, [{r, State#state.r}], State#state.timeout_read)
                          end) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, base64:encode(ValueGen())),
            case riakpool:execute(fun(C) ->
                                          riakc_pb_socket:put(C, Robj2, [{w, State#state.w},
                                                                         {dw, State#state.dw}], State#state.timeout_write)
                                  end) of
                {ok, ok} ->
                    {ok, State};
                {ok, {error, Reason}} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj0 = riakc_obj:new(State#state.bucket, Key),
            Robj = riakc_obj:update_value(Robj0, base64:encode(ValueGen())),
            case riakpool:execute(fun(C) ->
                                          riakc_pb_socket:put(C, Robj, [{w, State#state.w},
                                                                        {dw, State#state.dw}], State#state.timeout_write)
                                  end) of
                {ok, ok} ->
                    {ok, State};
                {ok, {error, Reason}} ->
                    {error, Reason, State}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakpool:execute(fun(C) ->
                                  riakc_pb_socket:get(C, State#state.bucket,
                                                      Key, [{r, State#state.r}], State#state.timeout_read)
                          end) of
        {ok, {ok, Robj}} ->
            Robj2 = riakc_obj:update_value(Robj, base64:encode(ValueGen())),
            case riakpool:execute(fun(C) ->
                                          riakc_pb_socket:put(C, Robj2, [{w, State#state.w},
                                                                         {dw, State#state.dw}], State#state.timeout_write)
                                  end) of
                {ok, ok} ->
                    {ok, State};
                {ok, {error, Reason}} ->
                    {error, Reason, State}
            end;
        {ok, {error, notfound}} ->
            {error, {not_found, Key}, State};
        {ok, {error, Reason}} ->
            {error, Reason, State}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakpool:execute(fun(C) ->
                                  riakc_pb_socket:delete(C, State#state.bucket, KeyGen(),
                                                         [{rw, State#state.rw}], State#state.timeout_write)
                          end) of
        {ok, ok} ->
            {ok, State};
        {ok, {error, notfound}} ->
            {ok, State};
        {ok, {error, Reason}} ->
            {error, Reason, State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
