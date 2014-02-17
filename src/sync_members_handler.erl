-module(sync_members_handler).

-behaviour(gen_server).

%% API
-export([start_link/0, 
    send/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {connection, channel}).


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
    {ok, Connection, Channel} = amqp:connect("localhost"),
    ok = amqp:basic_subscribe(Channel, <<"members_sync">>, self()),
    {ok, #state{connection = Connection, channel=Channel}}.


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

handle_info(Info, State) ->
    amqp:basic_handle(State#state.channel, Info, State, fun do_handle/2),
    {noreply, State}.


%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_selrver terminates with Reason.
%% The return value is ignored.
terminate(_Reason, State) ->
    amqp:disconect(State#state.connection, State#state.channel),
    ok.

%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private functions
do_handle(_Content, _State) ->
    ok.

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


