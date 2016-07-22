-module(layout_eqc).

%% To compile and run with Quviq's QuickCheck:
%%
%% $ erl -sname foo -pz ~/lib/eqc/ebin
%%
%% > c(layout_eqc, [{d, 'EQC'}]).
%% > eqc:quickcheck(layout_eqc:prop()).
%%
%% To compile and run with Proper:
%%
%% $ erl -sname foo -pz /Users/fritchie/src/erlang/proper/ebin
%%
%% > c(layout_eqc, [{d, 'PROPER'}]).
%% > proper:quickcheck(layout_eqc:prop()).

%% To run the corfu_server:
%% ./bin/corfu_server -Q -l /tmp/corfu-test-dir -s 8000 --cm-poll-interval=9999
%%
%% The --cm-poll-interval flag is optional: it can avoid spammy noise
%% when also using "-d TRACE" that is caused by config manager polling.

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

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
                   {RegName, endpoint2nodename(Endpoint)} )).

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
    initial_state(local_mboxes(), local_endpoint()).

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
postcondition2(#state{committed_layout=CommittedLayout},
               {call,_,query,[_Mbox, _EP, C_Epoch]}, Ret) ->
    case Ret of
        timeout ->
            false;
        ["OK", _JSON] when CommittedLayout == "" ->
            %% We haven't committed anything.  Whatever default layout
            %% that the server has (e.g. after reset()) is ok.
            true;
        ["OK", JSON] ->
            JSON == layout_to_json(CommittedLayout);
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch;
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
        {error, nack} ->
            %% TODO: verify that the epoch went backward.
            Layout#layout.epoch =< CommittedEpoch;
        {error, wrongEpochException, CorrectEpoch} ->
            CorrectEpoch /= C_Epoch
            andalso
            CorrectEpoch == CommittedEpoch;
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
           {call,_,commit,[_Mbox, _EP, C_Epoch, _Rank, Layout]}) ->
    if C_Epoch == CommittedEpoch andalso Layout#layout.epoch > CommittedEpoch ->
            S#state{prepared_rank=-1,
                    proposed_layout="",
                    committed_layout=Layout,
                    committed_epoch=Layout#layout.epoch};
       true ->
            S
    end;
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%

reset(Mbox, Endpoint) ->
    io:format(user, "R", []),
    java_rpc(Mbox, reset, Endpoint).

reboot(Mbox, Endpoint) ->
    %% io:format(user, "r", []),
    java_rpc(Mbox, reboot, Endpoint).

query(Mbox, Endpoint, C_Epoch) ->
    java_rpc(Mbox, "query", Endpoint, C_Epoch, []).

prepare(Mbox, Endpoint, C_Epoch, Rank) ->
    java_rpc(Mbox, "prepare", Endpoint, C_Epoch,
             ["-r", integer_to_list(Rank)]).

propose(Mbox, Endpoint, C_Epoch, Rank, Layout) ->
    JSON = layout_to_json(Layout),
    TmpPath = lists:flatten(io_lib:format("/tmp/layout.~w", [now()])),
    ok = file:write_file(TmpPath, JSON),
    Res = java_rpc(Mbox, "propose", Endpoint, C_Epoch,
                   ["-r", integer_to_list(Rank), "-l", TmpPath]),
    file:delete(TmpPath),
    Res.

commit(Mbox, Endpoint, C_Epoch, Rank, Layout) ->
    JSON = layout_to_json(Layout),
    TmpPath = lists:flatten(io_lib:format("/tmp/layout.~w", [now()])),
    ok = file:write_file(TmpPath, JSON),
    Res = java_rpc(Mbox, "committed", Endpoint, C_Epoch,
                   ["-r", integer_to_list(Rank), "-l", TmpPath]),
    file:delete(TmpPath),
    Res.

termify(["OK"]) ->
    ok;
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

local_mboxes() ->
    [cmdlet0, cmdlet1, cmdlet2, cmdlet3, cmdlet4,
     cmdlet5, cmdlet6, cmdlet7, cmdlet8, cmdlet9].

local_endpoint() ->
    "sbb5:8000".

endpoint2nodename(Endpoint) ->
    [HostName, Port] = string:tokens(Endpoint, ":"),
    list_to_atom("corfu-" ++ Port ++ "@" ++ HostName).

-ifdef(EQC).
my_run_always(AlwaysNum, Mod, Cmds, RunFun, CheckFun) ->
    ?ALWAYS(AlwaysNum,
            begin
                {H, S_or_Hs, Res} = RunFun(Mod, Cmds),
                aggregate(command_names(Cmds),
                measure(
                  cmds_length,
                  try length(Cmds) catch _:_ -> 0 end,
                pretty_commands(
                  ?MODULE, Cmds, {H,S_or_Hs,Res},
                %% ?WHENFAIL(
                %%   io:format("H: ~p~nS: ~p~nR: ~p~n", [H,S_or_Hs,Res]),
                  CheckFun(Cmds, H, S_or_Hs, Res)
                )))
            end).
-endif.
-ifdef(PROPER).
my_run_always(AlwaysNum, Mod, Cmds, RunFun, CheckFun) ->
    begin
        BigResList = [RunFun(Mod, Cmds) || _ <- lists:seq(1, AlwaysNum)],
        aggregate(command_names(Cmds),
        measure(
          cmds_length,
          try length(Cmds) catch _:_ -> 0 end,
          begin
              Chk_HSHsRes =
                  lists:zip([CheckFun(Cmds, H, S_or_Hs, Res) ||
                                {H, S_or_Hs, Res} <- BigResList],
                            BigResList),
              HSHsRes_failed = [X || X={Chk, _HSHsRes} <- Chk_HSHsRes,
                                     Chk /= true],
              case HSHsRes_failed of
                  [] ->
                      true;
                  [{Chk, {H, S_or_Hs, Res}}|_] ->
                  ?WHENFAIL(
                     io:format("H: ~p~nS: ~p~nR: ~p~n", [H,S_or_Hs,Res]),
                     Chk
                    )
              end
          end
         ))
    end.

-endif.

prop() ->
    prop(1).

prop(MoreCmds) ->
    prop(MoreCmds, local_mboxes(), local_endpoint()).

prop(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    %% Hmmmm, more_commands() doesn't appear to work correctly with Proper.
    ?FORALL(Cmds, more_commands(MoreCmds,
                                commands(?MODULE,
                                         initial_state(Mboxes, Endpoint))),
            my_run_always(1, ?MODULE, Cmds,
                          fun(Mod, TheCmds) ->
                                  run_commands(Mod, TheCmds)
                          end,
                          fun(_TheCmds, _H, _S_or_Hs, Res) ->
                                  Res == ok
                          end)
            ).

prop_parallel() ->
    prop_parallel(1).

prop_parallel(MoreCmds) ->
    prop_parallel(MoreCmds, local_mboxes(), local_endpoint()).

% % EQC has an exponential worst case for checking {SIGH}
-define(PAR_CMDS_LIMIT, 6). % worst case so far @ 7 = 52 seconds!

prop_parallel(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    ?FORALL(Cmds,
            more_commands(MoreCmds,
                           non_empty(
                             parallel_commands(?MODULE,
                                      initial_state(Mboxes, Endpoint)))),
            my_run_always(100, ?MODULE, Cmds,
                          fun(Mod, TheCmds) ->
                                  run_parallel_commands(Mod, TheCmds)
                          end,
                          fun(_TheCmds, _H, _S_or_Hs, Res) ->
                                  Res == ok
                          end)
            ).

seq_to_par_cmds(L) ->
    [Cmd || Cmd <- L,
            element(1, Cmd) /= init].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

java_rpc(Node, reset, Endpoint) ->
    AllArgs = ["corfu_layout", "reset", Endpoint],
    java_rpc_call(Node, AllArgs);
java_rpc(Node, reboot, Endpoint) ->
    AllArgs = ["corfu_layout", "reboot", Endpoint],
    java_rpc_call(Node, AllArgs).

java_rpc({_RegName, _NodeName} = Mbox, CmdName, Endpoint, C_Epoch, Args) ->
    AllArgs = ["corfu_layout", CmdName, Endpoint] ++
        ["-p", lists:flatten(io_lib:format("~w", [Mbox])) ] ++ %% --quickcheck-ap-prefix
        ["-e", integer_to_list(C_Epoch)] ++ Args,
    java_rpc_call(Mbox, AllArgs).

java_rpc_call(Mbox, AllArgs) ->
    ID = make_ref(),
    Mbox ! {self(), ID, AllArgs},
    receive
        {ID, Res} ->
            Res
    after 2*1000 ->
            timeout
    end.
