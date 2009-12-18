%%% Copyright 2009 Benoît Chesneau.
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%% @author Benoît Chesneau <benoitc@e-engura.org>
%% @copyright 2009 Benoît Chesneau.

-module(couchbeam_resource).
-author('Benoît Chesneau <benoitc@e-engura.org>').

-include("couchbeam.hrl").


-export([get/5, head/5, delete/5, post/6, put/6, request/1]).
-export([encode_query/1, make_uri/3]).


get(State, Path, Headers, Params, Opts) ->
    request(#http_req{couchdb=State, method=get,
        path=Path, headers=Headers, params=Params, options=Opts}).

head(State, Path, Headers, Params, Opts) ->
    request(#http_req{couchdb=State, method=head,
        path=Path, headers=Headers, params=Params, options=Opts}).
    
delete(State, Path, Headers, Params, Opts) ->
    request(#http_req{couchdb=State, method=delete,
        path=Path, headers=Headers, params=Params, options=Opts}).

post(State, Path, Headers, Params, Body, Opts) ->
    request(#http_req{couchdb=State, method=post,
        path=Path, headers=Headers, params=Params, body=Body, options=Opts}).


put(State, Path, Headers, Params, Body, Opts) ->
    request(#http_req{couchdb=State, method=put,
        path=Path, headers=Headers, params=Params, body=Body, options=Opts}).
    
request(#http_req{retries=0}) ->
    {error, request_failed};
    
request(Req) ->
                
    #http_req{
        couchdb=State,
        method=Method,
        path=Path,
        headers=Headers,
        params=Params,
        body=Body,
        options=Options,
        conn = Conn
    } = Req,
    
    #couchdb_params{url=Url, timeout=Timeout}=State,
    
    Url1 = make_uri(Url, Path, Params),
    Options1 = make_auth(State, Options),
    Headers1 = default_header("Content-Type", "application/json", Headers),
    Body1 = case Body of
        {Fun, InitialState} when is_function(Fun) ->
            {Fun, InitialState};
        nil -> [];
        [] -> [];
        _Else ->
            iolist_to_binary(Body)
    end,
    
    Resp = case Conn of
        nil ->
            ibrowse:send_req(Url1, Headers1, Method, Body1, Options1, Timeout);
        _ ->
            ibrowse:send_req_direct(Conn, Url1, Headers1, Method, Body1, Options1, Timeout)
    end,
    make_response(Resp, Req).
    
make_response({ok, Status, Headers, Body}, #http_req{method=Method}=Req) ->
    Code = list_to_integer(Status),
    if
        Code =:= 200; Code =:= 201 ->
            if
                Method == "HEAD" ->
                    {ok, Status};
                true ->
                    case is_pid(Body) of
                        true ->
                            {ok, Body};
                        false ->
                            try couchbeam:json_decode(Body) of
                                Resp1 -> 
                                    case Resp1 of
                                        {[{<<"ok">>, true}]} -> ok;
                                        {[{<<"ok">>, true}|Res]} -> {ok, {Res}};
                                        Obj -> {ok, Obj}
                                    end
                            catch
                                _:_ -> {ok, Body}
                            end
                    end
            end;
        Code >= 400, Code == 404 ->
            {error, not_found};
        Code >= 400, Code == 409 ->
            {error, conflict};
        Code >= 400, Code == 412 ->
            {error, precondition_failed};
        Status >= 400, Code < 500 ->
            {error, {unknown_error, Status}};
        Code =:= 500; Code =:= 502; Code =:= 503 ->
            #http_req{retries=Retries} = Req,
            request(Req#http_req{retries=Retries-1});
        true ->
            {error, {request_failed, {"unknow reason", Status}}}
    end;
    
make_response({ibrowse_req_id, Id}, _Req) ->
    {ibrowse_req_id, Id};
make_response({error, _Reason}, #http_req{retries=0}) ->
    {error, request_failed};
make_response({error, Reason}, Req) ->
    #http_req{retries=Retries} = Req,
    if 
        Reason == worker_is_dead ->
            C = spawn_link_worker_process(Req),
            request(Req#http_req{retries=Retries-1, conn=C});
        true ->
            request(Req#http_req{retries=Retries-1})
    end.

make_uri(Url, Path, Params) when is_binary(Url) ->
    make_uri(binary_to_list(Url), Path, Params);
make_uri(Url, Path, Params) ->
    Path1 = lists:append([Path, 
            case Params of
                [] -> [];
                Props -> "?" ++ encode_query(Props)
            end]),
    Url ++ Path1.

make_auth(#couchdb_params{username=nil, password=nil}, Options) ->
    Options;
make_auth(#couchdb_params{username=_UserName, password=nil}=State, Options) ->
    make_auth(State#couchdb_params{password=""}, Options);
make_auth(#couchdb_params{username=UserName, password=Password}, Options) ->
    [{basic_auth, {UserName, Password}}|Options].
    
encode_query(Props) ->
    RevPairs = lists:foldl(fun ({K, V}, Acc) ->
                                   [[couchbeam_util:quote_plus(K), $=, 
                                   encode_query_value(K, V)] | Acc]
                           end, [], Props),
    lists:flatten(couchbeam_util:revjoin(RevPairs, $&, [])).
  
encode_query_value(K,V) when is_atom(K) ->
    encode_query_value(atom_to_list(K), V);
encode_query_value(K,V) when is_binary(K) ->
    encode_query_value(binary_to_list(K), V);
encode_query_value(K,V) ->
    V1 = case K of
    "key"-> encode_value(V);
    "startkey" -> encode_value(V);
    "endkey" -> encode_value(V);
    _ -> 
        couchbeam_util:quote_plus(V)
    end,
    V1.

encode_value(V) ->
    V1 = couchbeam:json_encode(V),
    couchbeam_util:quote_plus(binary_to_list(iolist_to_binary(V1))).
    
    
default_header(K, V, H) ->
    case proplists:is_defined(K, H) of
    true -> H;
    false -> [{K, V}|H]
    end.
    
has_body("HEAD") ->
    false;
has_body("GET") ->
    false;
has_body("DELETE") ->
    false;
has_body(_) ->
    true.
    
default_content_length(B, H) ->
    default_header("Content-Length", integer_to_list(erlang:iolist_size(B)), H).

body_length(H) ->
    case proplists:get_value("Content-Length", H) of
        undefined -> false;
        _ -> true
    end.
    
spawn_link_worker_process(#http_req{couchdb=State}) ->
    Url = ibrowse_lib:parse_url(State#couchdb_params.url),
    {ok, Pid} = ibrowse_http_client:start_link(Url),
    Pid.