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

-include("qc_java.hrl").

-compile(export_all).

-record(state, {
          reset_p = false :: boolean(),
          endpoint :: string(),
          reg_names :: list(),
          prepared_rank=-1 :: non_neg_integer(),
          proposed_layout="" :: string(),
          committed_layout="",
          committed_epoch=0    % Must match server's epoch after reset()!
         }).

-record(layout, {
          epoch=-1,
          ls=[],
          ss=[],
          segs=[]
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_mbox(#state{endpoint=Endpoint, reg_names=RegNames}) ->
    noshrink( ?LET(RegName, oneof(RegNames),
                   {RegName, qc_java:endpoint2nodename(Endpoint)} )).

gen_rank() ->
    choose(1, 100).

gen_rank(#state{prepared_rank=0}) ->
    gen_rank();
gen_rank(#state{prepared_rank=PR}) ->
    frequency([{10, PR},
               { 2, gen_rank()}]).

gen_c_epoch(#state{committed_epoch=CommittedEpoch}) ->
    CommittedEpoch.

gen_epoch() ->
    choose(1, 100).

gen_layout() ->
    ?LET(Epoch, oneof([5, 22, gen_epoch()]),
         gen_layout(Epoch)).

gen_layout(Epoch) ->
    #layout{epoch=Epoch}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    initial_state(qc_java:local_mboxes(), qc_java:local_endpoint()).

initial_state(Mboxes, Endpoint) ->
    #state{endpoint=Endpoint, reg_names=Mboxes}.

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, {call,_,prepare,[_,_,_,Rank]}) ->
    S#state.reset_p andalso Rank > 0;
precondition(S, {call,_,propose,[_,_,_,Rank,Layout]}) ->
    S#state.reset_p andalso Rank > 0 andalso Layout /= "";
precondition(S, {call,_,commit,[_,_,_,Rank,Layout]}) ->
    S#state.reset_p andalso Rank > 0 andalso Layout /= "";
precondition(S, _Call) ->
    S#state.reset_p.

command(S=#state{endpoint=Endpoint, reset_p=false}) ->
    {call, ?MODULE, reset, [gen_mbox(S), Endpoint]};
command(S=#state{endpoint=Endpoint, reset_p=true,
                 proposed_layout=ProposedLayout}) ->
    frequency(
      [
       {5,  {call, ?MODULE, reboot,
             [gen_mbox(S), Endpoint]}},
       {20, {call, ?MODULE, query,
             [gen_mbox(S), Endpoint, gen_c_epoch(S)]}},
       {20, {call, ?MODULE, prepare,
             [gen_mbox(S), Endpoint, gen_c_epoch(S), gen_rank()]}},
       {20, {call, ?MODULE, propose,
             [gen_mbox(S), Endpoint, gen_c_epoch(S), gen_rank(S), gen_layout()]}},
       {20, {call, ?MODULE, commit,
             [gen_mbox(S), Endpoint, gen_c_epoch(S), gen_rank(S), ProposedLayout]}}
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
                      committed_epoch=CommittedEpoch},
               {call,_,query,[_Mbox, _EP, C_Epoch]}, Ret) ->
    case termify(Ret) of
        timeout ->
            false;
        {ok, _JSON} when CommittedLayout == "" ->
            %% We haven't committed anything.  Whatever default layout
            %% that the server has (e.g. after reset()) is ok.
            true;
        {ok, JSON} ->
            JSON == layout_to_json(CommittedLayout);
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch
            orelse
            C_Epoch /= CommittedEpoch;
        Else ->
            io:format(user, "Q ~p\n", [Else]),
            false
    end;
postcondition2(#state{prepared_rank=PreparedRank,
                      committed_epoch=CommittedEpoch},
              {call,_,prepare,[_Mbox, _EP, C_Epoch, Rank]}, RetStr) ->
    case termify(RetStr) of
        ok ->
            C_Epoch == CommittedEpoch andalso Rank > PreparedRank;
        {error, outrankedException, _ExceptionRank} ->
            Rank =< PreparedRank;
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch
            andalso
            CorrectEpoch == CommittedEpoch;
        Else ->
            {prepare, Rank, prepared_rank, PreparedRank, Else}
    end;
postcondition2(#state{prepared_rank=PreparedRank,
                      proposed_layout=ProposedLayout,
                      committed_epoch=CommittedEpoch},
              {call,_,propose,[_Mbox, _EP, C_Epoch, Rank, _Layout]}, RetStr) ->
    case termify(RetStr) of
        ok ->
            Rank == PreparedRank;
        {error, outrankedException, ExceptionRank} ->
            %% -1 = no prepare
            (ExceptionRank == -1 andalso PreparedRank == -1)
            orelse
            Rank /= PreparedRank
            orelse
            %% Already proposed?  2x isn't permitted.
            ProposedLayout /= "";
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch
            andalso
            CorrectEpoch == CommittedEpoch;
        Else ->
            {propose, Rank, prepared_rank, PreparedRank, Else}
    end;
postcondition2(#state{committed_epoch=CommittedEpoch},
               {call,_,commit,[_Mbox, _EP, C_Epoch, Rank, Layout]}, RetStr) ->
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
            %% Thus, no rank checking here, just epoch going forward.
            Layout#layout.epoch > CommittedEpoch;
        {error, wrongEpochException, CorrectEpoch} ->
            (CorrectEpoch /= C_Epoch
             andalso
             CorrectEpoch == CommittedEpoch)
            orelse
            Layout#layout.epoch =< CommittedEpoch;
        Else ->
            {commit, rank, Rank, layout, Layout,
             committed, CommittedEpoch, Else}
    end.

next_state(S, _V, {call,_,reset,[_Mbox, _EP]}) ->
    S#state{reset_p=true};
next_state(S=#state{prepared_rank=PreparedRank,
                    committed_epoch=CommittedEpoch}, _V,
           {call,_,prepare,[_Mbox, _EP, C_Epoch, Rank]}) ->
    if C_Epoch == CommittedEpoch andalso Rank > PreparedRank ->
            S#state{prepared_rank=Rank, proposed_layout=""};
       true ->
            S
    end;
next_state(S=#state{prepared_rank=PreparedRank,
                    committed_epoch=CommittedEpoch}, _V,
           {call,_,propose,[_Mbox, _EP, C_Epoch, Rank, Layout]}) ->
    if C_Epoch == CommittedEpoch andalso Rank == PreparedRank ->
            S#state{proposed_layout=Layout};
       true ->
            S
    end;
next_state(S=#state{committed_epoch=CommittedEpoch}, _V,
           {call,_,commit,[_Mbox, _EP, _C_Epoch, _Rank, Layout]}) ->
    if Layout#layout.epoch > CommittedEpoch ->
            S#state{prepared_rank=-1,
                    proposed_layout="",
                    committed_layout=Layout,
                    committed_epoch=Layout#layout.epoch};
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

query(Mbox, Endpoint, C_Epoch) ->
    rpc(Mbox, "query", Endpoint, C_Epoch, []).

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
termify(["OK", JSON_perhaps]) ->
    {ok, JSON_perhaps};
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
        "\n}".

string_ify_list(L) ->
    "[" ++ string:join([[$\"] ++ X ++ [$\"] || X <- L], ",") ++ "]".

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
