-module(layout_qc).

%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 VMware, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% See the README.md file for instructions for compiling & running.

-ifdef(PROPER).
%% Automagically import generator functions like choose(), frequency(), etc.
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
%% Automagically import generator functions like choose(), frequency(), etc.
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

-define(QUICK_MBOX, qc_java:quick_mbox_endpoint()).
-define(TIMEOUT, 15*1000).

-define(NO_PREPARED_RANK, -999).
-define(NO_PROPOSED_RANK, -999).

-include("qc_java.hrl").

-compile(export_all).

-record(layout, {
          epoch=-1,
          ls=[],
          ss=[],
          segs=[]
         }).

-record(state, {
          reset_p = false :: boolean(),
          endpoint :: string(),
          reg_names :: list(),
          %% For command generation use, we only need the rank number.
          %% To make model checking possible, we need rank number + clientID.
          prepared_rank={?NO_PREPARED_RANK,""} :: {integer(), string()},
          proposed_rank={?NO_PROPOSED_RANK,""} :: {integer(), string()},
          proposed_layout=layout_not_proposed :: 'layout_not_proposed' | #layout{},
          committed_layout=layout_not_committed :: 'layout_not_committed' | #layout{},
          last_epoch_set=0,    % Must match server's epoch after reset()!
          clientIDs=orddict:new()
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_mbox(#state{endpoint=Endpoint, reg_names=RegNames}) ->
    ?LET(RegName, oneof(RegNames),
         {RegName, qc_java:endpoint2nodename(Endpoint)} ).

gen_rank() ->
    %% So, it looks like negative ranks are accepted by the LayoutServer, cool.
    %% Don't allow a choice equal to ?NO_PROPOSED_RANK or ?NO_PREPARED_RANK.
    choose(-5, 100).

gen_rank(#state{prepared_rank={?NO_PREPARED_RANK,_}}) ->
    gen_rank();
gen_rank(#state{prepared_rank={PR,_}}) ->
    frequency([{10, PR},
               { 2, gen_rank()}]).

gen_c_epoch(#state{last_epoch_set=LastEpochSet}) ->
    LastEpochSet.

gen_epoch() ->
    choose(1, 100).

gen_epoch(#state{last_epoch_set=LastEpochSet}) ->
    oneof([LastEpochSet, gen_epoch()]).

gen_layout() ->
    ?LET(Epoch, oneof([5, 22, gen_epoch()]),
         gen_layout(Epoch)).

gen_layout(Epoch) ->
    #layout{epoch=Epoch}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    initial_state(qc_java:local_mboxes(), qc_java:local_endpoint()).

initial_state(Mboxes, Endpoint) ->
    Node = qc_java:endpoint2nodename(Endpoint),
    ClientIDs = [begin
                     M = {Mb, Node},
                     {M, hd(tl(getClientID(M, Endpoint)))}
                 end || Mb <- Mboxes],
    #state{endpoint=Endpoint, reg_names=Mboxes, clientIDs=ClientIDs}.

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, {call,_,prepare,[_,_,_,_Rank]}) ->
    S#state.reset_p;
precondition(S, {call,_,propose,[_,_,_,_Rank,Layout]}) ->
    S#state.reset_p andalso Layout /= layout_not_proposed;
precondition(S, {call,_,commit,[_,_,_,_Rank,Layout]}) ->
    S#state.reset_p andalso Layout /= layout_not_proposed;
precondition(S, _Call) ->
    S#state.reset_p.

command(S=#state{endpoint=Endpoint, reset_p=false}) ->
    {call, ?MODULE, reset, [gen_mbox(S), Endpoint]};
command(S=#state{endpoint=Endpoint, reset_p=true,
                 proposed_layout=ProposedLayout}) ->
    CommitLayout = oneof([ProposedLayout,
                          gen_layout(gen_epoch(S))]),
    frequency(
      [
       {5,  {call, ?MODULE, reboot,
             [gen_mbox(S), Endpoint]}},
       {20, {call, ?MODULE, query,
             [gen_mbox(S), Endpoint, gen_c_epoch(S)]}},
       {20, {call, ?MODULE, set_epoch,
             [gen_mbox(S), Endpoint, gen_epoch()]}},
       {20, {call, ?MODULE, prepare,
             [gen_mbox(S), Endpoint, gen_c_epoch(S), gen_rank()]}},
       {20, {call, ?MODULE, propose,
             [gen_mbox(S), Endpoint, gen_c_epoch(S), gen_rank(S), gen_layout()]}},
       {20, {call, ?MODULE, commit,
             [gen_mbox(S), Endpoint, gen_c_epoch(S), gen_rank(S), CommitLayout]}}
      ]).

postcondition(S, Call, Ret) ->
    try
        postcondition2(S, Call, Ret)
    catch X:Y ->
            io:format(user,
                      "Bad: ~p ~p @ ~p\n", [X, Y, erlang:get_stacktrace()])
    end.

postcondition2(_S, {call,_,RRR,[_Mbox, _EP]}, Ret)
  when RRR == reboot; RRR == reset ->
    case Ret of
        ["OK"] -> true;
        Else   -> {got, Else}
    end;
postcondition2(#state{committed_layout=CommittedLayout,
                      last_epoch_set=LastEpochSet},
               {call,_,query,[_Mbox, _EP, C_Epoch]}, Ret) ->
    case termify(Ret) of
        timeout ->
            false;
        {ok, _Props} when CommittedLayout == layout_not_committed ->
            %% We haven't committed anything.  Whatever default layout
            %% that the server has (e.g. after reset()) is ok.
            true;
        {ok, Props} ->
            JSON = proplists:get_value(layout, Props),
            JSON == layout_to_json(CommittedLayout);
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch
            orelse
            C_Epoch /= LastEpochSet;
        Else ->
            io:format(user, "Q ~p\n", [Else]),
            false
    end;

postcondition2(#state{committed_layout=CL,
                      last_epoch_set=LastEpochSet},
              {call,_,set_epoch,[_Mbox, _EP, Epoch]}, RetStr) ->
    CommittedEpoch = calc_committed_epoch(CL),
    case termify(RetStr) of
        ok ->
            %% Multiple cluster managers may be attempting to advance
            %% from epoch E_old to E_new; the server will allow
            %% multiple SET_EPOCH{epoch=E_new} commands to succeed as
            %% if each SET_EPOCH were the first to be received.
            Epoch >= LastEpochSet
            andalso
            Epoch >= CommittedEpoch;
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch == LastEpochSet
            orelse
            Epoch =< CommittedEpoch;
        Else ->
            M = {set_epoch, arg_epoch, Epoch, Else},
            io:format(user, "~p\n", [M]),
            false
    end;
postcondition2(#state{prepared_rank=PreparedRank,
                      proposed_layout=ProposedLayout,
                      last_epoch_set=LastEpochSet,
                      clientIDs=ClientIDs},
              {call,_,prepare,[Mbox, _EP, _C_Epoch, Rank]}, RetStr) ->
    ClientID = orddict:fetch(Mbox, ClientIDs),
    %% io:format(user, "compare rank: ~p ? ~p -> ~p\n", [{Rank, ClientID}, PreparedRank, compare_rank({Rank, ClientID}, PreparedRank)]),
    case termify(RetStr) of
        {ok, Props} ->
            case proplists:get_value(layout, Props) of
                undefined ->
                    compare_rank({Rank, ClientID}, PreparedRank) > 0;
                Layout_str1 ->
                    Layout_str2 = strip_whitespace(Layout_str1),
                    ProposedLayout_str2 =
                        strip_whitespace(layout_to_json(ProposedLayout)),

                    compare_rank({Rank, ClientID}, PreparedRank) > 0
                    andalso
                    Layout_str2 == ProposedLayout_str2
            end;
        {error, outrankedException, _ExceptionRank} ->
            compare_rank({Rank, ClientID}, PreparedRank) =< 0;
        {error, wrongEpochException, CorrectEpoch} ->
            %% io:format(user, "CorrectEpoch ~p LastEpochSet ~p\n", [CorrectEpoch, LastEpochSet]),
            CorrectEpoch == LastEpochSet;
        Else ->
            {prepare, {Rank, ClientID}, prepared_rank, PreparedRank, Else}
    end;
postcondition2(#state{prepared_rank=PreparedRank,
                      proposed_layout=ProposedLayout,
                      committed_layout=CL,
                      last_epoch_set=LastEpochSet,
                      clientIDs=ClientIDs},
              {call,_,propose,[Mbox, _EP, _C_Epoch, Rank, _Layout]}, RetStr) ->
    CommittedEpoch = calc_committed_epoch(CL),
    ClientID = orddict:fetch(Mbox, ClientIDs),
    case termify(RetStr) of
        ok ->
            %% NOTE: We cannot make assumptions about model state's
            %%
            %% proposed_layout here.  We may have had:
            %% prepare(rank=1), propose(rank=1,layout=L),
            %% prepare(rank=2), propose(rank=2,layout=L)
            %%
            %% ...and we are evaluating the 2nd propose.  Our model state
            %% already has a layout defined by the 1st propose.
            compare_rank({Rank, ClientID}, PreparedRank) == 0;
        {error, outrankedException, ExceptionRank} ->
            %% -1 = no prepare
            (ExceptionRank == -1 andalso PreparedRank == {?NO_PREPARED_RANK,""})
            orelse
            %% Technically, "outranked" should only mean "less than", but
            %% our implementaiton is actually comparing equality.
            compare_rank({Rank, ClientID}, PreparedRank) /= 0
            orelse
            %% Already proposed?  2x isn't permitted.
            ProposedLayout /= layout_not_proposed;
        %% SLF: As of commit fa2c7d2cb27a89908e5947b806bd940d7ede0d68,
        %% the following appears not possible?
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch == LastEpochSet orelse
                CorrectEpoch == CommittedEpoch;
        Else ->
            {propose, {Rank, ClientID}, prepared_rank, PreparedRank, Else}
    end;
postcondition2(#state{committed_layout=CL,
                      last_epoch_set=LastEpochSet},
               {call,_,commit,[_Mbox, _EP, _C_Epoch, _Rank, Layout]}, RetStr) ->
    CommittedEpoch = calc_committed_epoch(CL),
    case termify(RetStr) of
        ok ->
            %% According to the model, prepare & propose are optional.
            %% We could be in a quorum minority, didn't participate in
            %% prepare & propose, the decision was made without us, and
            %% committed is telling us the result.
            %% 
            %% After chatting with Dahlia, the model should separate
            %% rank checking from epoch checking.  In theory, the
            %% implementation could reset rank state after a new
            %% layout with bigger epoch has been committed.  We assume
            %% here that the implementation *does* reset rank upon
            %% commit -- that may change, pending more changes in PR
            %% #210 and perhaps elsewhere.
            %%
            %% Thus, no rank checking here, just epoch going not-backward.
            %% We assume non-Byzantine client behavior here.  Byzantine'ish
            %% includes this sequence:
            %%   layout_qc:commit(3, 0, {layout,3,[],[],[]})           -> ["OK"]
            %%   layout_qc:commit(3, 0, {layout,3,["yoo:9000"],[],[]}) -> ["OK"]
            %%   layout_qc:commit(3, 0, {layout,3,[],["yoo:9000"],[]}) -> ["OK"]
            %%
            %% ... which will *all* commit, despite the fact that they
            %% are three different layouts that happen to share the
            %% same epoch=3 value.
            Layout#layout.epoch >= LastEpochSet
            andalso
            Layout#layout.epoch >= CommittedEpoch;
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch == LastEpochSet
            orelse
            CorrectEpoch == CommittedEpoch
            orelse
            Layout#layout.epoch < LastEpochSet;
        Else ->
            {commit, layout, Layout, committed, LastEpochSet, Else}
    end.

next_state(S, _V, {call,_,reset,[_Mbox, _EP]}) ->
    S#state{reset_p=true};
next_state(S, _V, {call,_,reboot,[_Mbox, _EP]}) ->
    S;
next_state(S=#state{committed_layout=CL,
                    last_epoch_set=LastEpochSet}, _V,
           {call,_,set_epoch,[_Mbox, _EP, Epoch]}) ->
    CommittedEpoch = calc_committed_epoch(CL),
    if Epoch >= LastEpochSet
       andalso
       Epoch >= CommittedEpoch ->
            S#state{last_epoch_set=Epoch,
                    prepared_rank={?NO_PREPARED_RANK,""},
                    proposed_rank={?NO_PROPOSED_RANK,""},
                    proposed_layout=layout_not_proposed};
       true ->
            S
    end;
next_state(S=#state{prepared_rank=PreparedRank,
                    last_epoch_set=LastEpochSet,
                    clientIDs=ClientIDs}, _V,
           {call,_,prepare,[Mbox, _EP, C_Epoch, Rank]}) ->
    ClientID = orddict:fetch(Mbox, ClientIDs),
    GreaterRank = compare_rank({Rank, ClientID}, PreparedRank) > 0,
    if C_Epoch == LastEpochSet andalso GreaterRank ->
            %% Do not reset proposed_layout here.  We may have sequence of:
            %% prepare(rank=1), propose(rank=1,layout=L),
            %% prepare(rank=2), prepare(rank=3), ...
            %% and in each case, we still need to remember layout L.
            S#state{prepared_rank={Rank, ClientID},
                    proposed_rank={?NO_PROPOSED_RANK,""}};
       true ->
            S
    end;
next_state(S=#state{prepared_rank=PreparedRank,
                    proposed_rank=ProposedRank,
                    clientIDs=ClientIDs}, _V,
           {call,_,propose,[Mbox, _EP, _C_Epoch, Rank, Layout]}) ->
    ClientID = orddict:fetch(Mbox, ClientIDs),
    EqualRank = compare_rank({Rank, ClientID}, PreparedRank) > 0,
    if EqualRank andalso
       ProposedRank == ?NO_PROPOSED_RANK ->
            S#state{proposed_rank={Rank, ClientID},
                    proposed_layout=Layout};
       true ->
            S
    end;
next_state(S=#state{committed_layout=CL,
                    last_epoch_set=LastEpochSet}, _V,
           {call,_,commit,[_Mbox, _EP, _C_Epoch, _Rank, Layout]}) ->
    CommittedEpoch = calc_committed_epoch(CL),
    if Layout#layout.epoch >= LastEpochSet
       andalso
       Layout#layout.epoch >= CommittedEpoch ->
            S#state{prepared_rank={?NO_PREPARED_RANK,""},
                    proposed_rank={?NO_PROPOSED_RANK,""},
                    proposed_layout=layout_not_proposed,
                    committed_layout=Layout,
                    %% A committed layout also updates the server's
                    %% epoch in the same manner that the sealing
                    %% behavior of SET_EPOCH does.
                    last_epoch_set=Layout#layout.epoch};
       true ->
            S
    end;
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

reset(Mbox, Endpoint) ->
    %% io:format(user, "R", []),
    rpc(Mbox, reset, Endpoint).

reboot(Mbox, Endpoint) ->
    %% io:format(user, "r", []),
    rpc(Mbox, reboot, Endpoint).

getClientID(Mbox, Endpoint) ->
    rpc(Mbox, "getClientID", Endpoint, 0, []).

query(Mbox, Endpoint, C_Epoch) ->
    rpc(Mbox, "query", Endpoint, C_Epoch, []).

set_epoch(Mbox, Endpoint, C_Epoch) ->
    rpc(Mbox, "set_epoch", Endpoint, C_Epoch, []).

prepare(Mbox, Endpoint, C_Epoch, Rank) ->
    rpc(Mbox, "prepare", Endpoint, C_Epoch,
             ["-r", integer_to_list(Rank)]).

propose(Mbox, Endpoint, C_Epoch, Rank, Layout) ->
    JSON = layout_to_json(Layout),
    TmpPath = lists:flatten(io_lib:format("/tmp/layout.~w", [now()])),
    ok = file:write_file(TmpPath, JSON),
    try
        rpc(Mbox, "propose", Endpoint, C_Epoch,
            ["-r", integer_to_list(Rank), "-l", TmpPath])
    after
        file:delete(TmpPath)
    end.

commit(Mbox, Endpoint, C_Epoch, Rank, Layout) ->
    %% ["OK"].  %% intentional failure testing
    JSON = layout_to_json(Layout),
    TmpPath = lists:flatten(io_lib:format("/tmp/layout.~w", [now()])),
    ok = file:write_file(TmpPath, JSON),
    try
        rpc(Mbox, "committed", Endpoint, C_Epoch,
            ["-r", integer_to_list(Rank), "-l", TmpPath])
    after
        file:delete(TmpPath)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Quick human-friendly versions of RPC invocations to Java Land(tm).
%% Useful for developer exploration at the Erlang shell.

reset() ->
    apply(?MODULE, reset, ?QUICK_MBOX).

reboot() ->
    apply(?MODULE, reboot, ?QUICK_MBOX).

query(C_Epoch) ->
    apply(?MODULE, query, ?QUICK_MBOX ++ [C_Epoch]).

set_epoch(C_Epoch) ->
    apply(?MODULE, set_epoch, ?QUICK_MBOX ++ [C_Epoch]).

prepare(C_Epoch, Rank) ->
    apply(?MODULE, prepare, ?QUICK_MBOX ++ [C_Epoch, Rank]).

propose(C_Epoch, Rank, Layout) ->
    apply(?MODULE, propose, ?QUICK_MBOX ++ [C_Epoch, Rank, Layout]).

commit(C_Epoch, Rank, Layout) ->
    apply(?MODULE, commit, ?QUICK_MBOX ++ [C_Epoch, Rank, Layout]).

sanity() ->
    case reset() of
        ["OK"] ->
            ok;
        _Else1 ->
            io:format("\n"),
            io:format("reset() error: do you have corfu_server running\n"),
            io:format("on host ~s?\n", [qc_java:local_endpoint()]),
            io:format("\n"),
            throw(reset_error)
    end,
    case query(0) of
        ["OK", _] ->
            ok;
        _Else2 ->
            io:format("\n"),
            io:format("query(0) error: do you have corfu_server running\n"),
            io:format("on host ~s\n", [qc_java:local_endpoint()]),
            io:format("\n"),
            Host = qc_java:local_endpoint_host(),
            io:format("Also, please verify that the host ~s is resolvable\n",
                      [Host]),
            io:format("via DNS or /etc/hosts.  Here are 'ping' results:\n"),
            io:format("\n"),
            io:format("~s\n", [os:cmd("ping -c 1 " ++ Host)]),
            io:format("When /etc/hosts or DNS is fixed, please restart "
                      "the corfu_server process!\n"),
            io:format("\n"),
            throw(query_error)
    end,
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

termify(["OK"]) ->
    ok;
termify(["OK"|ProplistStrs]) ->
    {ok, parse_proplist(ProplistStrs)};
termify(["ERROR", "NACK"]) ->
    {error, nack};
termify(["ERROR", "Exception " ++ _E1, E2|Rest] = _L) ->
    case string:str(E2, "OutrankedException:") of
        I when I > 0 ->
            NewRank = parse_newrank(Rest),
            {error, outrankedException, NewRank};
        _ ->
            case string:str(E2, "WrongEpochException") of
                I2 when I2 > 0 ->
                    CorrectEpoch = parse_correctepoch(Rest),
                    {error, wrongEpochException, CorrectEpoch}
            end
    end;
termify(timeout) ->
    timeout.

parse_proplist([]) ->
    [];
parse_proplist([H|T]) ->
    {match, [_, K, V]} =
        re:run(H, "^([^:]+): (.*)$", [dotall,{capture,all,list}]),
    [{list_to_atom(K),V}|parse_proplist(T)].

parse_newrank(["newRank: " ++ NR|_]) ->
    list_to_integer(NR);
parse_newrank([_|T]) ->
    parse_newrank(T).

parse_correctepoch(["correctEpoch: " ++ NR|_]) ->
    list_to_integer(NR);
parse_correctepoch([_|T]) ->
    parse_correctepoch(T).

layout_to_json(#layout{ls=Ls, ss=Seqs, segs=Segs, epoch=Epoch}) ->
    "{\n  \"layoutServers\": " ++
        string_ify_list(Ls) ++
        ",\n  \"sequencers\": " ++
        string_ify_list(Seqs) ++
        ",\n  \"segments\": " ++
        string_ify_list(Segs) ++
        ",\n  \"epoch\": " ++
        integer_to_list(Epoch) ++
        "\n}";
layout_to_json(A) when is_atom(A) ->
    atom_to_list(A).

string_ify_list(L) ->
    "[" ++ string:join([[$\"] ++ X ++ [$\"] || X <- L], ",") ++ "]".

calc_committed_epoch(layout_not_committed) ->
    0; % Must match server's epoch after reset()!
calc_committed_epoch(#layout{epoch=Epoch}) ->
    Epoch.

strip_whitespace(Str) ->
    re:replace(Str, "[ \n\t]", "", [global,{return,list}]).

compare_rank({R1, ID1}, {R2, ID2}) ->
    if R1 /= R2 ->
            R1 - R2;
       ID1 < ID2 ->
            -1;
       ID1 == ID2 ->
            0;
       ID1 > ID2 ->
            1
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

prop() ->
    prop(1).

prop(MoreCmds) ->
    prop(MoreCmds, qc_java:local_mboxes(), qc_java:local_endpoint()).

prop(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    %% Hmmmm, more_commands() doesn't appear to work correctly with Proper.
    ?FORALL(Cmds, more_commands(MoreCmds,
                                commands(?MODULE,
                                         initial_state(Mboxes, Endpoint))),
            begin
                {H, S_or_Hs, Res} = run_commands(?MODULE, Cmds),
                aggregate(command_names(Cmds),
                measure(
                  cmds_length,
                  ?COMMANDS_LENGTH(Cmds),
                ?PRETTY_FAIL(
                  ?MODULE, Cmds, H,S_or_Hs,Res,
                  begin
                      Res == ok
                  end
                )))
            end).


prop_parallel() ->
    prop_parallel(1).

prop_parallel(MoreCmds) ->
    prop_parallel(MoreCmds, qc_java:local_mboxes(), qc_java:local_endpoint()).

% % EQC has an exponential worst case for checking {SIGH}
-define(PAR_CMDS_LIMIT, 6). % worst case so far @ 7 = 52 seconds!

prop_parallel(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    ?FORALL(Cmds,
            more_commands(MoreCmds,
                           non_empty(
                             parallel_commands(?MODULE,
                                      initial_state(Mboxes, Endpoint)))),
            ?WRAP_ALWAYS(5,
            begin
                {H, S_or_Hs, Res} = run_parallel_commands(?MODULE, Cmds),
                aggregate(command_names(Cmds),
                measure(
                  cmds_length,
                  ?COMMANDS_LENGTH(Cmds),
                ?PRETTY_FAIL(
                  ?MODULE, Cmds, H,S_or_Hs,Res,
                  begin
                      Res == ok
                  end
                )))
            end)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

rpc(Mbox, reset, Endpoint) ->
    AllArgs = ["corfu_layout", "reset", Endpoint],
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT);
rpc(Mbox, reboot, Endpoint) ->
    AllArgs = ["corfu_layout", "reboot", Endpoint],
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT).

rpc({_RegName, _NodeName} = Mbox, CmdName, Endpoint, C_Epoch, Args) ->
    AllArgs = ["corfu_layout", CmdName, Endpoint] ++
        %% -p = --quickcheck-ap-prefix
        ["-p", lists:flatten(io_lib:format("~w", [Mbox])) ] ++
        ["-e", integer_to_list(C_Epoch)] ++ Args,
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT).
