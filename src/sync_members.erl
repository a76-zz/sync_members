-module(sync_members).

-export([start/0]).

start() ->
	application:start(sync_members).