%%% -*- erlang -*-
%%%
%%% This file is part of couchbeam released under the MIT license. 
%%% See the NOTICE for more information.

-module(couchbeam_changes).

-include("couchbeam.hrl").
-include_lib("ibrowse/src/ibrowse.hrl").

-export([wait_for_change/1, continuous_acceptor/2]).


-export([stream_changes/2, stream_changes/3,
         changes_loop/3]).

-export([decode_row/1]).
-record(state, {
    partial_chunk = <<"">>
}).

-record(changes_args, {
        type = normal,
        http_options = []}).


-spec stream_changes(Db::db(), ClientPid::pid()) -> {ok, StartRef::term(),
        ChangesPid::pid()} | {error, term()}.
%% @equiv stream_changes(Db, ClientPid, []).
stream_changes(Db, ClientPid) ->
    stream_changes(Db, ClientPid, []).

-spec stream_changes(Db::db(), ClientPid::pid(),
    Options::changes_options()) -> {ok, StartRef::term(),
        ChangesPid::pid()} | {error, term()}.

stream_changes(#db{server=Server, options=IbrowseOpts}=Db,
        ClientPid, Options) ->
    Args = parse_changes_options(Options),
    Url = couchbeam:make_url(Server, [couchbeam:db_url(Db), "/_changes"],
        Args#changes_args.http_options),

    StartRef = make_ref(),

    UserFun = fun
        (done) ->
            LastSeq = get(last_seq),
            ClientPid ! {change, StartRef, {done, LastSeq}};
        ({error, Error}) ->
            LastSeq = get(last_seq),
            ClientPid ! {error, StartRef, LastSeq, Error};
        ({[{<<"last_seq">>, _}]}) ->
            ok;
        (Row) ->
            ClientPid ! {change, StartRef, Row},
            Seq = couchbeam_doc:get_value(<<"seq">>, Row),
            put(last_seq, Seq)
    end,
    
    Params = {Url, IbrowseOpts},

    ChangesPid = proc_lib:spawn_link(couchbeam_changes, changes_loop,
        [Args, UserFun, Params]),

    case couchbeam_httpc:request_stream({ChangesPid, once}, get, Url, IbrowseOpts) of
        {ok, ReqId} ->
            ChangesPid ! {ibrowse_req_id, ReqId},
            {ok, StartRef, ChangesPid};
        Error ->
            Error
    end.

changes_loop(Args, UserFun, Params) ->
    Callback = case Args#changes_args.type of
        continuous ->
            fun(200, _Headers, DataStreamFun) ->
                continuous_changes(DataStreamFun, UserFun)
            end;
        _ ->
            fun(200, _Headers, DataStreamFun) ->
                EventFun = fun(Ev) ->
                    changes_ev1(Ev, UserFun)
                end,
                couchbeam_json_stream:events(DataStreamFun, EventFun)
            end
    end,
    receive
        {ibrowse_req_id, ReqId} ->
            process_changes(ReqId, Params, UserFun, Callback)
    after ?DEFAULT_TIMEOUT ->
        UserFun({error, timeout}) 
    end.


process_changes(ReqId, Params, UserFun, Callback) ->
    receive
        {ibrowse_async_headers, IbrowseRef, Code, Headers} ->
            case list_to_integer(Code) of
                Ok when Ok =:= 200 ; Ok =:= 201 ; (Ok >= 400 andalso Ok < 500) ->
                    StreamDataFun = fun() ->
                        process_changes1(ReqId, UserFun, Callback)
                    end,
                    ibrowse:stream_next(IbrowseRef), 
                    Callback(Ok, Headers, StreamDataFun),
                    clean_mailbox_req(ReqId),
                    ok;
                R when R =:= 301 ; R =:= 302 ; R =:= 303 ->
                    do_redirect(Headers, UserFun, Callback, Params),
                    ibrowse:stream_close(reqId);
                Error ->
                    UserFun({error, {http_error, {status,
                                    Error}}})

            end;
        {ibrowse_async_response, ReqId, {error, _} = Error} ->
            UserFun({error, Error})
    end.

process_changes1(ReqId, UserFun, Callback) ->
    receive
    {ibrowse_async_response, ReqId, {error, Error}} ->
        UserFun({error, Error});
    {ibrowse_async_response, ReqId, <<>>} ->
        ibrowse:stream_next(ReqId),
        process_changes1(ReqId, UserFun, Callback);
    {ibrowse_async_response, ReqId, Data} ->
        ibrowse:stream_next(ReqId),
        {Data, fun() -> process_changes1(ReqId, UserFun, Callback) end};
    {ibrowse_async_response_end, ReqId} ->
        UserFun(done),
        done 
end.


clean_mailbox_req(ReqId) ->
    receive
    {ibrowse_async_response, ReqId, _} ->
        clean_mailbox_req(ReqId);
    {ibrowse_async_response_end, ReqId} ->
        clean_mailbox_req(ReqId)
    after 0 ->
        ok
    end.

do_redirect(Headers, UserFun, Callback, {Url, IbrowseOpts}) ->
    RedirectUrl = redirect_url(Headers, Url),
    Params = {RedirectUrl, IbrowseOpts},
    case couchbeam_httpc:request_stream({self(), once}, get, RedirectUrl, 
            IbrowseOpts) of
        {ok, ReqId} ->
            process_changes(ReqId, Params, UserFun, Callback);
        Error ->
            UserFun({error, {redirect, Error}})
    end.

redirect_url(RespHeaders, OrigUrl) ->
    MochiHeaders = mochiweb_headers:make(RespHeaders),
    Location = mochiweb_headers:get_value("Location", MochiHeaders),
    #url{
        host = Host,
        host_type = HostType,
        port = Port,
        path = Path,  % includes query string
        protocol = Proto
    } = ibrowse_lib:parse_url(Location),
    #url{
        username = User,
        password = Passwd
    } = ibrowse_lib:parse_url(OrigUrl),
    Creds = case is_list(User) andalso is_list(Passwd) of
    true ->
        User ++ ":" ++ Passwd ++ "@";
    false ->
        []
    end,
    HostPart = case HostType of
    ipv6_address ->
        "[" ++ Host ++ "]";
    _ ->
        Host
    end,
    atom_to_list(Proto) ++ "://" ++ Creds ++ HostPart ++ ":" ++
        integer_to_list(Port) ++ Path.


changes_ev1(object_start, UserFun) ->
    fun(Ev) -> changes_ev2(Ev, UserFun) end.

changes_ev2({key, <<"results">>}, UserFun) ->
    fun(Ev) -> changes_ev3(Ev, UserFun) end;
changes_ev2(_, UserFun) ->
    fun(Ev) -> changes_ev2(Ev, UserFun) end.

changes_ev3(array_start, UserFun) ->
    fun(Ev) -> changes_ev_loop(Ev, UserFun) end.

changes_ev_loop(object_start, UserFun) ->
    fun(Ev) ->
        couchbeam_json_stream:collect_object(Ev,
            fun(Obj) ->
                UserFun(Obj),
                fun(Ev2) -> changes_ev_loop(Ev2, UserFun) end
            end)
    end;
changes_ev_loop(array_end, UserFun) ->
    UserFun(done),
    fun(_Ev) -> changes_ev_done() end.

changes_ev_done() ->
    fun(_Ev) -> changes_ev_done() end.

continuous_changes(DataFun, UserFun) ->
    {DataFun2, _, Rest} = couchbeam_json_stream:events(
        DataFun,
        fun(Ev) -> parse_changes_line(Ev, UserFun) end),
    continuous_changes(fun() -> {Rest, DataFun2} end, UserFun).

parse_changes_line(object_start, UserFun) ->
    fun(Ev) ->
        couchbeam_json_stream:collect_object(Ev,
            fun(Obj) -> UserFun(Obj) end)
    end.

parse_changes_options(Options) ->
    parse_changes_options(Options, #changes_args{}).

parse_changes_options([], Args) ->
    Args;
parse_changes_options([continuous|Rest], #changes_args{http_options=Opts}) ->
    Opts1 = [{"feed", "continuous"}|Opts],
    parse_changes_options(Rest, #changes_args{type=continuous,
            http_options=Opts1});
parse_changes_options([longpoll|Rest], #changes_args{http_options=Opts}) ->
    Opts1 = [{"feed", "longpoll"}|Opts],
    parse_changes_options(Rest, #changes_args{type=longpoll,
            http_options=Opts1});
parse_changes_options([normal|Rest], Args) ->
    parse_changes_options(Rest, Args#changes_args{type=normal});
parse_changes_options([include_docs|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"include_docs", "true"}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([{since, Since}|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"since", Since}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([{timeout, Timeout}|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"timeout", Timeout}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([{heartbeat, Heartbeat}|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"heartbeat", Heartbeat}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([heartbeat|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"heartbeat", "true"}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([{filter, FilterName}|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"filter", FilterName}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([{filter, FilterName, FilterArgs}|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"filter", FilterName}|Opts] ++ FilterArgs,
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([{limit, Limit}|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"limit", Limit}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([conflicts|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"conflicts", "true"}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([{style, Style}|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"style", Style}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([descending|Rest], #changes_args{http_options=Opts} = Args) ->
    Opts1 = [{"descending", "true"}|Opts],
    parse_changes_options(Rest, Args#changes_args{http_options=Opts1});
parse_changes_options([_|Rest], Args) ->
    parse_changes_options(Rest, Args).


wait_for_change(Reqid) ->
    wait_for_change(Reqid, 200, []).

wait_for_change(Reqid, ReqStatus, Acc) ->
    receive
        {ibrowse_async_response_end, Reqid} ->
            Change = iolist_to_binary(lists:reverse(Acc)),
            try
                if ReqStatus >= 400 ->
                    {error, {ReqStatus, ejson:decode(Change)}};
                true ->
                    {ok, ejson:decode(Change)}
                end
            catch
            throw:{invalid_json, Error} ->
                {error, Error}
            end;
        {ibrowse_async_response, Reqid, {error,Error}} ->
            {error, Error};
        {ibrowse_async_response, Reqid, Chunk} ->
            ibrowse:stream_next(Reqid),
            wait_for_change(Reqid, ReqStatus, [Chunk|Acc]);
        {ibrowse_async_headers, Reqid, Status, _Headers} ->

            ibrowse:stream_next(Reqid), 
            wait_for_change(Reqid, list_to_integer(Status), Acc) 
    end.
    


%% @doc initiate continuous loop 
continuous_acceptor(Pid, PidRef) ->
    receive
        {ibrowse_req_id, PidRef, IbrowseRef} ->
            continuous_acceptor(Pid, PidRef, IbrowseRef,
                #state{})
    after ?DEFAULT_TIMEOUT ->
        Pid ! {PidRef, {error, {timeout, []}}}
    end.


%% @doc main ibrowse loop to receive continuous changes 
continuous_acceptor(Pid, PidRef, IbrowseRef, State) ->
    receive
        {ibrowse_async_response_end, IbrowseRef} ->
            Pid ! {PidRef, done};
        {ibrowse_async_response, IbrowseRef, {error,Error}} ->
            Pid ! {PidRef, {error, Error}};
        {ibrowse_async_response, IbrowseRef, Chunk} ->
            Messages = [M || M <- re:split(Chunk, ",?\n", [trim]), M =/= <<>>],
            {ok, State1} = handle_messages(Messages, Pid, PidRef, IbrowseRef, State),
            continuous_acceptor(Pid, PidRef, IbrowseRef, State1);
        {ibrowse_async_headers, IbrowseRef, Status, Headers} ->
            if Status =/= "200" ->
                    Pid ! {PidRef, {error, {Status, Headers}}};
                true ->
                    ibrowse:stream_next(IbrowseRef), 
                    continuous_acceptor(Pid, PidRef, IbrowseRef, State)
                    
            end 
    end.

handle_messages([], _Pid, _PidRef, IbrowseRef, State) ->
    ibrowse:stream_next(IbrowseRef),
    {ok, State};
handle_messages([<<"{\"last_seq\":", LastSeq/binary>>], Pid, PidRef,
        IbrowseRef, State) ->
    %% end of continuous response
    
    %% get last sequence
    L = size(LastSeq) - 1,
    <<LastSeq1:L/binary, _/binary>> = LastSeq,
    LastSeqInt = list_to_integer(binary_to_list(LastSeq1)),
    
    Pid ! {PidRef, {last_seq, LastSeqInt}},

    ibrowse:stream_next(IbrowseRef),
    {ok, State};
handle_messages([Chunk|Rest], Pid, PidRef, IbrowseRef, State) ->
    #state{partial_chunk=Partial}=State,
    NewState = try
        ChangeRow = decode_row(<<Partial/binary, Chunk/binary>>),
        Pid! {PidRef, {change, ChangeRow}},
        #state{}
    catch
    throw:{invalid_json, Bad} ->
        State#state{partial_chunk = Bad}
    end,
    handle_messages(Rest,  Pid, PidRef, IbrowseRef, NewState).

decode_row(<<",", Rest/binary>>) ->
    decode_row(Rest);
decode_row(Row) ->
    ejson:decode(Row).



