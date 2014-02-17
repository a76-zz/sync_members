-module(sync_members_handler).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {connection, channel}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    Queue = <<"members_sync">>,
    {ok, Connection, Channel} = amqp:connect("localhost", Queue),
    amqp:basic_subscribe(Channel, Queue, self()),
    {ok, #state{connection = Connection, channel=Channel}}.

terminate(_Reason, State) ->
    amqp:disconnect(State#state.connection, State#state.channel),
    ok.

handle_info(Info, State) ->
    amqp:basic_handle(State#state.channel, Info, State, fun do_handle/2),
    {noreply, State}.

do_handle(Content, _State) ->
    io:format("handle~p~n", [Content]),
    case Content of 
        {selected, ColNames, Rows} ->
            handle_rows(ColNames, Rows);
        _ ->
            io:format("error~p~n", [Content])
    end,
    ok.

handle_rows(ColNames, []) ->
    ok;

handle_rows(ColNames, [Row|OtherRows]) ->
    {ok, Key, Value} = sync_members_transform:transform(ColNames, Row),
    {ok, _UpdateType} = sync_members_db:save(Key, Value),
    handle_rows(ColNames, OtherRows).


handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


