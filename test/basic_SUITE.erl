-module(basic_SUITE).
-author("Kletsko Vitali <v.kletsko@gmail.com>").

-define(APPS, [
	kernel,
	stdlib,
	ssl
]).

-include_lib("common_test/include/ct.hrl").

%% Test server callbacks
-export([
	suite/0,
	all/0,
	init_per_suite/1,
	end_per_suite/1,
	init_per_testcase/2,
	end_per_testcase/2
]).

-export([
	auth/1
]).

all() ->
	[
		auth
	].

suite() ->
	[{timetrap,{minutes,5}}].

init_per_suite(_Config) ->
	ok = start_apps(),
	[].

end_per_suite(_Config) ->
	ok = stop_apps(),
	ok.

init_per_testcase(_Case, Config) ->
	Config.

end_per_testcase(_Case, Config) ->
	Config.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
start_apps() ->
	[ok = application:start(X) || X <- ?APPS],
	ok.

stop_apps() ->
	[ok = application:stop(X) || X <- ?APPS],
	ok.


%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------
auth(Conf) ->
	Conf.