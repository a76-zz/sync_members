-module(sync_members_handler).

-behaviour(gen_server).

%% API
-export([start_link/0, 
    send/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {channel, map}).

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
    DefaultMap = dict:from_list([{"member_id", <<"id_i">>},
            {"first_name", <<"first_name_s">>},
            {"last_name", <<"last_name_s">>}]),
    {ok, #state{channel=Channel, map=DefaultMap}}.


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
    Content = binary_to_term(Body),
    io:format(" received:~p~n", [Content]),
    save_content(Content, State#state.map, []),
    io:format(" [x] Done~n"),
    amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State}.



%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_selrver terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.

%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions
save_content(Content, Map, KeyMap) ->
    case Content of 
        {selected, ColNames, Rows} ->
            save_content(ColNames, Rows, Map, KeyMap)
    end.

save(Key, Value) -> 
    io:format("Saved:~p:~p~n", [Key, Value]).

get_key(Source, _KeyMap) ->
    {"member_id", Id} = lists:keyfind("member_id", 1, Source), 
    string:concat("http://localhost:8098/buckets/members/keys/", erlang:term_to_list(Id)).

save_content(ColNames, [Row|OtherRows], Map, KeyMap) ->
    Source = lists:zip(ColNames, erlang:tuple_to_list(Row)),
    Value = transform(Source, Map),
    Key = get_key(Source, KeyMap),
    save(Key, Value),
    save_content(ColNames, OtherRows, Map, KeyMap);

save_content(_ColNames, [], _Map, _KeyMap) ->
    ok.

transform(Source, Map) ->
    transform(Source, Map, []).


transform([{Key, Value}|Source], Map, Result) ->
    case dict:find(Key, Map) of 
        {ok, TKey} ->
            TValue = 
            if 
                is_list(Value) -> erlang:list_to_binary(Value);
                true -> Value
            end,
            transform(Source, Map, [{TKey, TValue}|Result]);    
        error ->
            transform(Source, Map, Result)
    end;

transform([], _Map, Result) ->
    Result.


