-module(t).
-compile(export_all).

init() ->
	ibrowse:start().

create(Key, Value) ->
    DeleteResp = ibrowse:send_req(Key, [], delete, []),
    UpdateStatus = case DeleteResp of 
		{ok, "404", _, _} -> {ok, new};
		{ok, "204", _, _} -> {ok, update};
		_ -> {delete_error, DeleteResp}
	end,
	case UpdateStatus of 
		{ok, Status} ->
			CreateResp = ibrowse:send_req(Key, [{"Content-Type", "application/json"}], put, Value),
			case CreateResp of 
				{ok, "204", _, _} -> {ok, Status};
				_ -> {create_error, CreateResp}
			end;
		DeleteError ->
			DeleteError
	end.

t_create() ->
    Key = "http://localhost:8098/buckets/members/keys/1",
    Value = jsx:encode([{<<"first_name_s">>,<<"jain">>},{<<"last_name_s">>,<<"air">>}]),
    create(Key, Value).


get() ->
    ibrowse:send_req("http://localhost:8098/buckets/members/keys/1", [], get, []).


%% curl -v -XPUT http://localhost:8098/buckets/members/keys/e911?returnbody=true -H "Content-Type: application/json" -d '{"first_name_s":"Ryan", "last_name_s":"Zezeski"}'
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


