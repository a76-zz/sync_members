-module(sync_members_transform).

-behaviour(gen_server).

-export([start_link/0, transform/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state, {map, key}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


transform(ColNames, Row) ->
    gen_server:call(?SERVER, {transform, {ColNames, Row}}).

init([]) ->
    Map = dict:from_list([
    	{"member_id", <<"id_i">>},
        {"first_name", <<"first_name_s">>},
        {"last_name", <<"last_name_s">>}
    ]),
    Key = "http://localhost:8098/buckets/members/keys/",
	{ok, #state{map=Map, key=Key}}.

handle_call(Request, _From, State) ->
    Reply = case Request of 
    	{transform, {ColNames, Rows}} ->
    		do_transform(ColNames, Rows, State)
    end,
    {reply, Reply, State}.


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
    sync_members_transform:start_link(),
    {ok, Key, Value} = sync_members_transform:transform(["member_id","first_name","last_name","time_stamp"],
            {1,"andrei","silchankau",{{2014,1,29},{14,28,14}}}),
        ?assertEqual(Key, "http://localhost:8098/buckets/members/keys/1"),
        ?assertEqual(Value, [{<<"id_i">>, 1}, {<<"first_name_s">>, <<"andrei">>}, {<<"last_name_s">>, <<"silchankau">>}]),
    sync_members_transform:terminate(normal, []). 
-endif.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.













