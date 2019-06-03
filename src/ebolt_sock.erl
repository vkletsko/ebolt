-module(ebolt_sock).
-author("Kletsko Vitali <v.kletsko@gmail.com>").

-behaviour(gen_server).

-include("ebolt.hrl").

%% API
-export([
    start_link/0,
    close/1,
%%    get_parameter/2,
    cancel/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).


-define(MAX_CHUNK_SIZE, 65535).

-define(USER_AGENT, << "EBolt/1.0" >>).
-define(HS_MAGIC,   << 16#60, 16#60, 16#B0, 16#17 >>).
-define(HS_VERSION, << 1:32, 0:32, 0:32, 0:32 >>).
-define(ZERO_CHUNK, << 0, 0 >>).

-define(SIG_INIT, 16#01).
-define(SIG_RUN, 16#10).
-define(SIG_PULL_ALL, 16#3F).
-define(SIG_ACK_FAILURE, 16#0E).
-define(SIG_RESET, 16#0F).
-define(SIG_DISCARD_ALL, 16#2F).

-define(SIG_SUCCESS, 16#70).
-define(SIG_RECORD, 16#71).
-define(SIG_IGNORED, 16#7E).
-define(SIG_FAILURE, 16#7F).

-record(neo4j_client, {
    sock,
    conn_state = disconnected :: disconnected | connected | authorized,
    timeout = undefined :: undefined,
    transport = undefined :: undefined | gen_tcp | ssl
%%    data = <<>>,
%%    backend,
%%    codec,
%%    queue = queue:new(),
%%    parameters = [],
%%    types = [],
%%    columns = [],
%%    rows = [],
%%    results = [],
%%    batch = [],
%%    txstatus

}).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link(?MODULE, [], []).

close(C) when is_pid(C) ->
    catch gen_server:cast(C, stop),
  ok.

%%get_parameter(C, Name) ->
%%  gen_server:call(C, {get_parameter, to_binary(Name)}, infinity).

cancel(S) ->
  gen_server:cast(S, cancel).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #neo4j_client{}} | {ok, State :: #neo4j_client{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #neo4j_client{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #neo4j_client{}) ->
  {reply, Reply :: term(), NewState :: #neo4j_client{}} |
  {reply, Reply :: term(), NewState :: #neo4j_client{}, timeout() | hibernate} |
  {noreply, NewState :: #neo4j_client{}} |
  {noreply, NewState :: #neo4j_client{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #neo4j_client{}} |
  {stop, Reason :: term(), NewState :: #neo4j_client{}}).
handle_call(Cmd, _From, State) ->
  cmd(Cmd, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #neo4j_client{}) ->
  {noreply, NewState :: #neo4j_client{}} |
  {noreply, NewState :: #neo4j_client{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #neo4j_client{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #neo4j_client{}) ->
  {noreply, NewState :: #neo4j_client{}} |
  {noreply, NewState :: #neo4j_client{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #neo4j_client{}}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #neo4j_client{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #neo4j_client{},
    Extra :: term()) ->
  {ok, NewState :: #neo4j_client{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
cmd({connect, Host, Port, UserName, Password, Opts}, NeoCl0) ->
  InitNeoCl = setup_neo4j_client(Opts, NeoCl0),
  case connect(Host, Port, InitNeoCl) of      %% @Todo ssl
    {ok, Sock} ->
      {ok, [{recbuf, RecBufSize}, {sndbuf, SndBufSize}]} = inet:getopts(Sock, [recbuf, sndbuf]),
      inet:setopts(Sock, [{buffer, max(RecBufSize, SndBufSize)}]),
      {ok, #neo4j_client{} = NeoCl1} =
        case handshake(Sock, InitNeoCl) of
          {ok, _NewNeoCl} = HandShResp ->
            HandShResp;
          {error, Reason1} ->
            {stop, Reason1, InitNeoCl}
        end,
      case init(UserName, Password, NeoCl1) of
        {ok, #neo4j_client{} = NeoCl2} ->
          {reply, connected, NeoCl2};
        {error, Reason} ->
          {stop, Reason, NeoCl1}
      end;
    {error, Reason} ->
      {stop, Reason, InitNeoCl}
  end;
cmd({run_statement, Statement, Params}, NeoCl) ->
  Res = run_statement(Statement, Params, NeoCl),
  {reply, {ok, Res}, NeoCl}.


connect(Host, Port, #neo4j_client{transport = Transport, timeout = Timeout}) ->
  SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}, {keepalive, true}],
  Transport:connect(Host, Port, SockOpts, Timeout).

setup_neo4j_client(Opts, NeoCl) ->
  Timeout = proplists:get_value(timeout, Opts, 5000),
  Transport = case proplists:get_value(ssl, Opts, false) of
                true -> ssl;
                false -> gen_tcp
              end,
  NeoCl#neo4j_client{transport = Transport, timeout = Timeout}.


handshake(Sock, #neo4j_client{transport = Transport, timeout = Timeout} = NeoCl) ->
  _Ok = Transport:send(Sock, [?HS_MAGIC, ?HS_VERSION]),
  case Transport:recv(Sock, 4, Timeout) of
    {ok, <<1:32>>} ->
      {ok, NeoCl#neo4j_client{conn_state = openned, sock = Sock}};
    Error ->
      error_logger:error_msg("Handshake failed. Received: ~p~n", [Error]),
      {error, handshake_failed}
  end.


init(UserName, Password, #neo4j_client{conn_state = openned} = NeoCl) ->
  InitParams = init_params(UserName, Password),
  ok = send_messages(NeoCl, [{[?USER_AGENT, InitParams], ?SIG_INIT}]),
  case receive_data(NeoCl) of
    {success, #{}} ->
      {ok, NeoCl#neo4j_client{conn_state = connected}};
    Error ->
      error_logger:error_msg("Init failed. Received: ~p~n", [Error]),
      {error, init_failed}
  end.


run_statement(Statement, Params, #neo4j_client{conn_state = connected} = NeoCl) ->
  ok = send_messages(NeoCl, [{[Statement, Params], ?SIG_RUN}]),
  case receive_data(NeoCl) of
    {success, _ResultAvialableMsg} ->
      ok = send_messages(NeoCl, [{[null], ?SIG_PULL_ALL}]),
      receive_data(NeoCl);
    Error ->
      Error
  end.


init_params(undefined, undefined) ->
  #{<<"sheme">> => <<"none">>};
init_params(UserName, Password) ->
  #{<<"scheme">> => <<"basic">>,
    <<"principal">> => UserName,
    <<"credentials">> => Password
  }.

send_messages(#neo4j_client{transport = Transport, sock = Sock}, Messages) ->
  BinMessages = encode_message(Messages),
  io:format("msg encoded = ~p~n", [BinMessages]),
  Chunks = gen_chunks(BinMessages),
  ok = lists:foreach(fun (Chunk) -> Transport:send(Sock, Chunk) end, Chunks).

encode_message(Data) ->
  Fun = fun ({Messages, Sign}) ->
          StructSize = length(Messages),
          ReducesMessages = ebolt_packstream:msg_to_bin(fun ebolt_packstream:encode/1, Messages),
          <<16#B:4, StructSize:4, Sign, ReducesMessages/binary>>
        end,
  lists:map(Fun, Data).

receive_data(#neo4j_client{} = NeoCl) ->
  receive_data(NeoCl, []).

receive_data(NeoCl, Prev) ->
  case unpack(do_receive_data(NeoCl)) of
    {record, _} = Data ->
      io:format("~p need this case data: ~p prev ~p~n", [?LINE, Data, Prev]),
      receive_data(NeoCl, [Data | Prev]);
    {Status, _} = Data
      when (Status =:= success orelse Status =:= ignored orelse Status =:= failure) andalso Prev =:= [] ->
      io:format("~p need this case data: ~p prev ~p~n", [?LINE, Data, Prev]),
      Data;
    {Status, _} = Data when Status =:= success orelse Status =:= ignored orelse Status =:= failure ->
      io:format("~p need this case data: ~p prev ~p~n", [?LINE, Data, Prev]),
      lists:reverse([Data | Prev])
  end.

do_receive_data(#neo4j_client{transport = Transport, sock = Sock, timeout = Timeout} = NeoCl) ->
    case Transport:recv(Sock, 2, Timeout) of
      {ok, <<Chunksize:16>>} ->
        do_receive_data(NeoCl, Chunksize);
      Error -> throw(Error)
    end.

do_receive_data(#neo4j_client{transport = Transport, sock = Sock, timeout = Timeout} = NeoCl, ChunkSize) ->
  case Transport:recv(Sock, ChunkSize, Timeout) of
    {ok, Data} ->
      case Transport:recv(Sock, 2, Timeout) of
        {ok, ?ZERO_CHUNK} ->
          Data;
        {ok, <<ChunkSize:16>>} ->
          <<Data/binary, (do_receive_data(NeoCl, ChunkSize))/binary>>;
        Error -> throw(Error)
      end;
    {error, timeout} ->
      {error, no_more_data_received};
    Error ->
      error_logger:error_msg("receive data failed. reason: ~p~n", [Error]),
      erlang:error("receive failed")
end.

unpack(<< 16#B:4, Packages:4, Status, Msg/binary >>) ->
  Response = ebolt_packstream:decode(Msg),
  error_logger:info_msg("packages ~p; msg: ~p~n", [Packages, Msg]),
  Response1 =
    case Packages =:= 1 of
      true ->
        erlang:hd(Response);
      false ->
        Response
    end,
  case Status of
    ?SIG_SUCCESS -> {success, Response1};
    ?SIG_RECORD  -> {record,  Response1};
    ?SIG_IGNORED -> {ignored, Response1};
    ?SIG_FAILURE -> {failure, Response1};
    Error        ->
      error_logger:error_msg("Couldn't decode data reason: ~p~n", [Error]),
      throw(couldnt_decode)
end;
unpack({error, _Err}) -> {error, _Err}.

gen_chunks(Messages) ->
  gen_chunks(Messages, [], <<>>).

gen_chunks([Msg | T], Chunks, CurrentChunk)
  when byte_size(<<CurrentChunk/binary, Msg/binary >>) =< ?MAX_CHUNK_SIZE ->
  MsgSize = byte_size(Msg),
  CurrentChunk1 = << CurrentChunk/binary, MsgSize:16, Msg/binary, ?ZERO_CHUNK/binary>>,
  gen_chunks(T, Chunks, CurrentChunk1);
gen_chunks([Chunk | T], Chunks, CurrentChunk) ->
  OversizedChunk = <<CurrentChunk/binary, Chunk/binary>>,
  {First, Rest}  = split_binary(OversizedChunk, ?MAX_CHUNK_SIZE),
  FirstSize = byte_size(First),
  RestSize = byte_size(Rest),
  CurrentChunk1 = <<CurrentChunk/binary, FirstSize:16, First/binary>>,
  NewChunk = << RestSize:16, Rest/binary>>,
  gen_chunks(T, [CurrentChunk1 | Chunks], NewChunk);
gen_chunks([], Chunks, CurrentChunk) ->
  lists:reverse([CurrentChunk | Chunks]).


