-module(sync_members_db).

-behaviour(gen_server).

-export([start_link/0, save/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

save(Key, Value) ->
    gen_server:call(?SERVER, {save, {Key, Value}}).

init([]) ->
    ibrowse:start(),
	{ok, []}.

terminate(_Reason, _State) ->
    ibrowse:stop(),
    ok.

handle_call(Request, _From, State) ->
    Reply = case Request of 
    	{save, {Key, Value}} ->
    		do_save(Key, Value)
    end,
	{reply, Reply, State}.

do_save(Key, Value) ->
    {ok, "204", _, _} = ibrowse:send_req(Key, [{"Content-Type", "application/json"}], put, jsx:encode(Value)),
    {ok, update}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.