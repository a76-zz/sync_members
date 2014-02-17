-module(sync_members_transform).

-behaviour(gen_server).

%% API
-export([start_link/0, transform/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {map, key}).

%% API implementation

%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server

start_link() ->
    gen_server:start_link(?MODULE, [], []).


transform(Pid, ColNames, Row) ->
    gen_server:call(Pid, {transform, {ColNames, Row}}).


%% gen_server callbacks

%% @spec init(Args) -> 
%%  {ok, State} |
%%  {ok, State, Timeout} |
%%  ignore |
%%  {stop, Reason}
%% @doc Initiates the server
init([]) ->
    Map = dict:from_list([
    	{"member_id", <<"id_i">>},
        {"first_name", <<"first_name_s">>},
        {"last_name", <<"last_name_s">>}
    ]),
    Key = "http://localhost:8098/buckets/members/keys/",
	{ok, #state{map=Map, key=Key}}.

%% @spec handle_call(Request, From, State) -> 
%%  {reply, Reply, State} |
%%  {reply, Reply, State, Timeout} |
%%  {noreply, State} |
%%  {noreply, State, Timeout} |
%%  {stop, Reason, Reply, State} |
%%  {stop, Reason, State}
%% @doc Handling call messages
handle_call(Request, _From, State) ->
    Reply = case Request of 
    	{transform, {ColNames, Rows}} ->
    		do_transform(ColNames, Rows, State)
    end,
    {reply, Reply, State}.

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

handle_info(_Info, State) ->
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

do_transform(ColNames, Row, State) ->
    Source = lists:zip(ColNames, erlang:tuple_to_list(Row)),
    Key = get_key(Source, State#state.key),
    Value = get_value(Source, State#state.map),
    {ok, Key, Value}.


get_value(Source, Map) ->
    get_value(Source, Map, []).

get_value([], _Map, Result) ->
    lists:reverse(Result);

get_value([{Key, Value}|Source], Map, Result) ->
    case dict:find(Key, Map) of 
        {ok, TKey} ->
            TValue = 
            if 
                is_list(Value) -> erlang:list_to_binary(Value);
                true -> Value
            end,
            get_value(Source, Map, [{TKey, TValue}|Result]);    
        error ->
            get_value(Source, Map, Result)
    end.

get_key(Source, Key) ->
    {"member_id", Id} = lists:keyfind("member_id", 1, Source), 
    string:concat(Key, erlang:integer_to_list(Id)).


-ifdef(TEST).

transform_test() ->
    {ok, Pid} = sync_members_transform:start_link(),
    {ok, Key, Value} = sync_members_transform:transform(Pid, ["member_id","first_name","last_name","time_stamp"],
            {1,"andrei","silchankau",{{2014,1,29},{14,28,14}}}),
        ?assertEqual(Key, "http://localhost:8098/buckets/members/keys/1"),
        ?assertEqual(Value, [{<<"id_i">>, 1}, {<<"first_name_s">>, <<"andrei">>}, {<<"last_name_s">>, <<"silchankau">>}]).
-endif.













