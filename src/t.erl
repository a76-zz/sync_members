-module(t).
-compile(export_all).

init() ->
	ibrowse:start().

create() ->
    Body = jsx:encode([{<<"first_name_s">>,<<"jain">>},{<<"last_name_s">>,<<"air">>}]),
	ibrowse:send_req("http://localhost:8098/buckets/members/keys/e101", [{"Content-Type", <<"application/json">>}], put, Body).

	% curl 'http://localhost:8098/solr/members/select?q=first_name_s:jain&wt=json'


