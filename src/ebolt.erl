-module(ebolt).
-author("Kletsko Vitali <v.kletsko@gmail.com>").

%% API
-export([
    connect/1,
    close/1,
    run/3
  ]).

-include("ebolt.hrl").

-type connection() :: pid().
-type host() :: inet:ip_address() | inet:hostname().
-type query() :: binary().
-type connect_error() :: #error{}.
-type query_error() :: #error{}.
-type connect_option() ::
    {port,     PortNum    :: inet:port_number()}   |
    {ssl,      IsEnabled  :: boolean() | required} |    % @Todo
    {ssl_opts, SslOptions :: [ssl:ssl_option()]}   |    % @Todo
    {timeout,  TimeoutMs  :: timeout()}.                % default: 5000 ms

-export_type([connection/0, query/0, connect_error/0, query_error/0]).

%% -- API --
-spec connect([connect_option()]) -> {ok, connection()}.
connect(Settings) ->
    Host = proplists:get_value(host, Settings, "localhost"),
    Username = proplists:get_value(username, Settings),
    Password = proplists:get_value(password, Settings),
    Port = proplists:get_value(port, Settings, 7687),
    connect(Host, Port, Username, Password, []).

connect(Host, Port, Username, Password, Opts) ->
    {ok, C} = ebolt_sock:start_link(),
    connect(C, Host, Port, Username, Password, Opts).

-spec connect(connection(), host(), integer(), string(), string(), [connect_option()]) ->
    {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
connect(C, Host, Port, Username, Password, Opts) ->
    case gen_server:call(C,
        {connect, Host, Port, Username, Password, Opts}, infinity) of
            connected ->
                {ok, C};
            Error = {error, _} ->
                Error
    end.

-spec run(connection(), query(), Params :: map()) ->
    {ok, Data :: list()} |  {error, Reason :: atom() | binary()}.
run(C, Statement, Params) ->
    case gen_server:call(C,
        {run_statement, Statement, Params}, infinity) of
        {ok, _Data} = Reply ->
            Reply;
        Error = {error, _} ->
            Error
    end.

-spec close(connection()) -> ok.
close(C) ->
    ebolt_sock:close(C).
