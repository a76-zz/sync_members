-module(sync_members_handler).

-behaviour(gen_server).

%% API
-export([start_link/0, 
    send/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {channel}).

-include_lib("deps/amqp_client/include/amqp_client.hrl").


%% API implementation

%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%% @spec send() -> ok
send(Message) ->
    gen_server:call(?SERVER, {send, Message}).

%% gen_server callbacks

%% @spec init(Args) -> 
%%  {ok, State} |
%%  {ok, State, Timeout} |
%%  ignore |
%%  {stop, Reason}
%% @doc Initiates the server

init([]) ->
    io:format("init self:~p~n", [self()]),
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
    {ok, Channel} = amqp_connection:open_channel(Connection),

    amqp_channel:call(Channel, #'queue.declare'{queue = <<"members_sync">>}),
    io:format(" [*] Waiting for messages.~n"),

    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 1}),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = <<"members_sync">>}, self()),
    {ok, #state{channel=Channel}}.


%% @spec handle_call(Request, From, State) -> 
%%  {reply, Reply, State} |
%%  {reply, Reply, State, Timeout} |
%%  {noreply, State} |
%%  {noreply, State, Timeout} |
%%  {stop, Reason, Reply, State} |
%%  {stop, Reason, State}
%% @doc Handling call messages
handle_call(_Request, _From, State) ->
    {noreply, State}.

%% @spec handle_cast(Msg, State) -> 
%%  {noreply, State} |
%%  {noreply, State, Timeout} |
%%  {stop, Reason, State}
%% @doc Handling cast messages

handle_cast(_Msg, State) ->
    {noreply, State}.


%% @spec handle_info(Info, State) -> 
%%  {noreply, State} |
%%  {noreply, State, Timeout} |
%%  {stop, Reason, State}
%% @doc Handling all non call/cast messages

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Body}}, State) ->
    Dots = length([C || C <- binary_to_list(Body), C == $.]),
    io:format(" [x] Received ~p~n", [Body]),
    receive
    after
        Dots*1000 -> ok
    end,
    io:format(" [x] Done~n"),
    amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State}.



%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.

%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


