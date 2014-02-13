-module(t).
-compile(export_all).

init() ->
	ibrowse:start().

create() ->
    Body = jsx:encode([{<<"first_name_s">>,<<"jain">>},{<<"last_name_s">>,<<"air">>}]),
	ibrowse:send_req("http://localhost:8098/buckets/members/keys/e101", [{"Content-Type", <<"application/json">>}], put, Body).

	% curl 'http://localhost:8098/solr/members/select?q=first_name_s:jain&wt=json'

% Sample data:
%{selected,["member_id","first_name","last_name",
%           "time_stamp"],
%          [{1,"andrei","silchankau",{{2014,1,29},{14,28,14}}}]}


transform(ColNames, Row, Map) ->
	Source = lists:zip(ColNames, erlang:tuple_to_list(Row)),
	transform_row(Source, Map, []).


transform_row([{Key, Value}|Source], Map, Result) ->
	case dict:find(Key, Map) of 
		{ok, TKey} ->
			TValue = 
			if 
				is_list(Value) -> erlang:list_to_binary(Value);
				true -> Value
			end,
			transform_row(Source, Map, [{TKey, TValue}|Result]); 	
		error ->
			transform_row(Source, Map, Result)
	end;

transform_row([], _Map, Result) ->
	Result.

transform_test() ->
	Data = transform(["member_id","first_name","last_name","time_stamp"],
		{1,"andrei","silchankau",{{2014,1,29},{14,28,14}}},
		dict:from_list([{"member_id", <<"id_i">>},
			{"first_name", <<"first_name_s">>},
			{"last_name", <<"last_name_s">>}])),
	jsx:encode(Data).


