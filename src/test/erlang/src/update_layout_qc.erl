-module(update_layout_qc).

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
-define(TIMEOUT, 2*1000).

-include("qc_java.hrl").

-compile(export_all).

-record(state, {
          reset_p = false :: boolean(),
          endpoint :: string(),
          reg_names :: list(),
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

gen_rank(_S) ->
    1.

gen_c_epoch(#state{committed_epoch=CommittedEpoch}) ->
    CommittedEpoch.

gen_epoch() ->
    choose(1, 100).

gen_layout(S) ->
    ?LET(Epoch, oneof([1, 3, gen_epoch()]),
         gen_layout(S, Epoch)).

gen_layout(#state{endpoint=Endpoint}, Epoch) ->
    #layout{ls=[Endpoint], epoch=Epoch}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    initial_state(qc_java:local_mboxes(), qc_java:local_endpoint()).

initial_state(Mboxes, Endpoint) ->
    #state{endpoint=Endpoint, reg_names=Mboxes}.

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, _Call) ->
    S#state.reset_p.

command(S=#state{endpoint=Endpoint, reset_p=false}) ->
    {call, ?MODULE, reset, [gen_mbox(S), Endpoint]};
command(S=#state{endpoint=Endpoint, reset_p=true}) ->
    frequency(
      [
       {5,  {call, ?MODULE, reboot,
             [gen_mbox(S), Endpoint]}},
       {20, {call, ?MODULE, query,
             [gen_mbox(S), Endpoint, gen_c_epoch(S)]}},
       {20, {call, ?MODULE, update_layout,
             [gen_mbox(S), Endpoint, gen_c_epoch(S), gen_layout(S), gen_rank(S)]}}
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
            strip_ws(JSON) == strip_ws(layout_to_json(CommittedLayout));
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch
            orelse
            C_Epoch /= CommittedEpoch;
        Else ->
            io:format(user, "Q ~p\n", [Else]),
            false
    end;
postcondition2(#state{committed_epoch=CommittedEpoch},
              {call,_,update_layout,[_Mbox, _EP, C_Epoch, Layout, _Rank]}, RetStr) ->
    case termify(RetStr) of
        ok ->
            C_Epoch == CommittedEpoch andalso
                Layout#layout.epoch > CommittedEpoch;
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch
            andalso
            CorrectEpoch == CommittedEpoch;
        Else ->
            io:format("OUCH ~p\n", [{update_layout, Else}]),
            false
    end.

next_state(S, _V, {call,_,reset,[_Mbox, _EP]}) ->
    S#state{reset_p=true};
next_state(S=#state{committed_epoch=CommittedEpoch}, _V,
           {call,_,update_layout,[_Mbox, _EP, C_Epoch, Layout, _Rank]}) ->
    if C_Epoch == CommittedEpoch andalso
       Layout#layout.epoch > CommittedEpoch ->
            S#state{committed_layout=Layout,
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

update_layout(Mbox, Endpoint, C_Epoch, Layout, Rank) ->
    JSON = layout_to_json(Layout),
    TmpPath = lists:flatten(io_lib:format("/tmp/layout.~w", [now()])),
    ok = file:write_file(TmpPath, JSON),
    try
        rpc(Mbox, "update_layout", Endpoint, C_Epoch,
            ["-l", TmpPath, "-r", integer_to_list(Rank)])
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

update_layout(C_Epoch, Layout, Rank) ->
    apply(?MODULE, update_layout, ?QUICK_MBOX ++ [C_Epoch, Layout, Rank]).

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
    timeout;
termify(["ERROR", "exception", "NullPointerException"|_Rest] = _L) ->
    {error, nullPointerException}.

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

strip_ws(Str) ->
    re:replace(Str, "[\n\t ]", "", [global, {return, list}]).

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
            ?WRAP_ALWAYS(10,
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
