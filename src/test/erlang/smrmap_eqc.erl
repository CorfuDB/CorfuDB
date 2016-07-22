-module(smrmap_eqc).

%% To compile and run with Quviq's QuickCheck:
%%
%% $ erl -sname foo -pz ~/lib/eqc/ebin
%%
%% > c(smrmap_eqc, [{d, 'EQC'}]).
%% > eqc:quickcheck(smrmap_eqc:prop()).
%%
%% To compile and run with Proper:
%%
%% $ erl -sname foo -pz /Users/fritchie/src/erlang/proper/ebin
%%
%% > c(smrmap_eqc, [{d, 'PROPER'}]).
%% > proper:quickcheck(smrmap_eqc:prop()).

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

-define(NUM_LOCALHOST_CMDLETS, 16).

-compile(export_all).

-record(state, {
          reset_p = false :: boolean(),
          stream :: non_neg_integer(),
          d=orddict:new() :: orddict:orddict(),
          server_list=[] :: list()
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_key() ->
    oneof([[choose($a, $b)],                     % make it a list
           [choose($a, $z)]]).                   % make it a list

gen_val() ->
    oneof(["",
           "Hello-world!",                      % no spaces or commas!
           "Another-value",
           ?LET(L, choose(0, 50),
                vector(L, choose($a, $z)))]).

gen_svr(#state{server_list=Svrs}) ->
    noshrink(oneof(Svrs)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    initial_state([{cmdlet0, 'corfu@sbb5'}]).

initial_state(ServerList) ->
    #state{stream=42, server_list=ServerList}.
    %% #state{stream=random:uniform(999*999), server_list=ServerList}.

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, _Call) ->
    S#state.reset_p.

command(S=#state{stream=Stream, reset_p=false}) ->
    {call, ?MODULE, reset, [gen_svr(S), Stream]};
command(S=#state{stream=Stream, reset_p=true}) ->
    frequency(
      [
       {20, {call, ?MODULE, put, [gen_svr(S), Stream, gen_key(), gen_val()]}},
       { 5, {call, ?MODULE, get, [gen_svr(S), Stream, gen_key()]}},
       { 3, {call, ?MODULE, size, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, isEmpty, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, containsKey, [gen_svr(S), Stream, gen_key()]}},
       %% BOO.  Our ASCII-oriented protocol can't tell the difference
       %% between an arity 0 function and an arity 1 function with
       %% an argument of length 0.
       { 3, {call, ?MODULE, containsValue, [gen_svr(S),
                                            Stream, non_empty(gen_val())]}},
       { 5, {call, ?MODULE, remove, [gen_svr(S), Stream, gen_key()]}},
       { 3, {call, ?MODULE, clear, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, keySet, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, values, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, entrySet, [gen_svr(S), Stream]}}
      ]).

postcondition(_S, {call,_,reset,[_Svr, _Str]}, Ret) ->
    case Ret of
        ["OK"] -> true;
        Else   -> {got, Else}
    end;
postcondition(#state{d=D}, {call,_,put,[_Svr, _Str, Key, _Val]}, Ret) ->
    case Ret of
        timeout ->
            false;
        ["OK"] ->
            orddict:find(Key, D) == error;
        ["OK", Prev] ->
            case orddict:find(Key, D) of
                error                  -> Prev == [];
                {ok, V} when V == Prev -> true;
                {ok, Else}             -> {key, Key, expected, Else, got, Prev}
            end
    end;
postcondition(S, {call,_,get,[_Svr, Str, Key]}, Ret) ->
    %% get's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[_Svr, Str, Key, <<"get_post()">>]}, Ret);
postcondition(#state{d=D}, {call,_,size,[_Svr, _Stream]}, Res) ->
    case Res of
        ["OK", SizeStr] ->
            list_to_integer(SizeStr) == length(orddict:to_list(D));
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,isEmpty,[_Svr, _Stream]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_empty(D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsKey,[_Svr, _Stream, Key]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_key(Key, D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsValue,[_Svr, _Stream, Value]}, Res) ->
    case Res of
        ["OK", Bool] ->
            Val_in_d = case [V || {_K, V} <- orddict:to_list(D),
                                  V == Value] of
                           [] -> false;
                           _  -> true
                       end,
            list_to_atom(Bool) == Val_in_d;
        Else ->
            {got, Else}
    end;
postcondition(S, {call,_,remove,[_Svr, Str, Key]}, Ret) ->
    %% remove's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[_Svr, Str, Key, <<"remove_post()">>]}, Ret);
postcondition(_S, {call,_,clear,[_Svr, _Str]}, ["OK"]) ->
    true;
postcondition(#state{d=D}, {call,_,keySet,[_Svr, _Str]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ks = string:tokens(X2, ", "),
            lists:sort(Ks) == lists:sort([K || {K,_V} <- orddict:to_list(D)])
    end;
postcondition(#state{d=D}, {call,_,values,[_Svr, _Str]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Vs = string:tokens(X2, ", "),
            %% BOO.  Our ASCII protocol can't tell us the difference between
            %% an empty list and a list of length one that contains an
            %% empty string.
            lists:sort(Vs) == lists:sort([V || {_K,V} <- orddict:to_list(D),
                                               V /= ""])
    end;
postcondition(#state{d=D}, {call,_,entrySet,[_Svr, _Str]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ps = string:tokens(X2, ", "),
            KVs = [begin
                       case string:tokens(Pair, "=") of
                           [K, V] -> {K, V};
                           [K]    -> {K, ""}
                       end
                   end || Pair <- Ps],
            lists:sort(KVs) == lists:sort(orddict:to_list(D))
    end.

next_state(S, _V, {call,_,reset,[_Svr, _Str]}) ->
    S#state{reset_p=true};
next_state(S=#state{d=D}, _V, {call,_,put,[_Svr, _Str, Key, Val]}) ->
    S#state{d=orddict:store(Key, Val, D)};
next_state(S=#state{d=D}, _V, {call,_,remove,[_Svr, _Str, Key]}) ->
    S#state{d=orddict:erase(Key, D)};
next_state(S, _V, {call,_,clear,[_Svr, _Str]}) ->
    S#state{d=orddict:new()};
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%

reset(Node, Stream) ->
    io:format(user, "!R", []),
    java_rpc(Node, Stream, ["clear"]).
    %% java_rpc(Node, reset, Stream).

put(Node, Stream, Key, Val) ->
    java_rpc(Node, Stream, ["put", Key ++ "," ++ Val]).

get(Node, Stream, Key) ->
    java_rpc(Node, Stream, ["get", Key]).

size(Node, Stream) ->
    java_rpc(Node, Stream, ["size"]).

isEmpty(Node, Stream) ->
    java_rpc(Node, Stream, ["isEmpty"]).

containsKey(Node, Stream, Key) ->
    java_rpc(Node, Stream, ["containsKey", Key]).

containsValue(Node, Stream, Value) ->
    java_rpc(Node, Stream, ["containsValue", Value]).

remove(Node, Stream, Key) ->
    java_rpc(Node, Stream, ["remove", Key]).

%% %% putAll() can't be tested because our ASCII protocol can't represent
%% %% the needed map.

clear(Node, Stream) ->
    java_rpc(Node, Stream, ["clear"]).

keySet(Node, Stream) ->
    java_rpc(Node, Stream, ["keySet"]).

values(Node, Stream) ->
    java_rpc(Node, Stream, ["values"]).

entrySet(Node, Stream) ->
    java_rpc(Node, Stream, ["entrySet"]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

local_servers() ->
    [_, ShortNameRHS] = string:tokens(atom_to_list(node()), "@"),
    CorfuLocalNode = list_to_atom("corfu@" ++ ShortNameRHS),
    local_servers(?NUM_LOCALHOST_CMDLETS, CorfuLocalNode).

local_servers(NumCmdlets, CorfuLocalNode) ->
    [{list_to_atom("cmdlet" ++ integer_to_list(X)), CorfuLocalNode} ||
        X <- lists:seq(0, NumCmdlets - 1)].

prop() ->
    prop(1).

prop(MoreCmds) ->
    prop(MoreCmds, local_servers()).

prop(MoreCmds, ServerList) ->
    random:seed(now()),
    %% Hmmmm, more_commands() doesn't appear to work correctly with Proper.
    ?FORALL(Cmds, more_commands(MoreCmds,
                                commands(?MODULE, initial_state(ServerList))),
            begin
                {H,S,Res} = run_commands(?MODULE, Cmds),
                %% ["OK", []] = clear(S#state.stream),
                ?WHENFAIL(
                io:format("H: ~p~nS: ~w~nR: ~p~n", [H,S,Res]),
                aggregate(command_names(Cmds),
                collect(length(Cmds) div 10,
                        Res == ok)))
            end).

prop_parallel() ->
    prop_parallel(1).

prop_parallel(MoreCmds) ->
    prop_parallel(MoreCmds, local_servers()).

% % EQC has an exponential worst case for checking {SIGH}
-define(PAR_CMDS_LIMIT, 6). % worst case so far @ 7 = 52 seconds!

prop_parallel(MoreCmds, ServerList) ->
    random:seed(now()),
    %% Drat.  EQC 1.37.2's more_commands() is broken: the parallel
    %% commands lists aren't resized.  So, we're going to do it
    %% ourself, bleh.
    ?FORALL(NumPars,
            choose(1, 4), %% ?NUM_LOCALHOST_CMDLETS - 3),
    ?FORALL(NewCs,
            [more_commands(MoreCmds,
                           non_empty(
                             commands(?MODULE,
                                      initial_state(ServerList)))) ||
                _ <- lists:seq(1, NumPars)],
            begin
                [Init|Rest] = hd(NewCs),
                SeqList = case Rest of
                              [{set,_,{call,_,reset,_}}|_] ->
                                  %% io:format(user, "\n\n **** Keep original reset\n", []),
                                  hd(NewCs);
                              _ ->
                                  %% io:format(user, "\n\n **** INSERT reset\n", []),
                                  [Init,
                                   {set,{var,1},{call,?MODULE,reset,[hd(ServerList), 42]}}] ++ Rest
                          end,
                Cmds = {SeqList,
                        lists:map(fun(L) -> lists:sublist(seq_to_par_cmds(L),
                                                          ?PAR_CMDS_LIMIT) end,
                                  tl(NewCs))},
                {Seq, Pars} = Cmds,
                Len = length(Seq) +
                    lists:foldl(fun(L, Acc) -> Acc + length(L) end, 0, Pars),
                {Elapsed, {H,Hs,Res}} = timer:tc(fun() -> run_parallel_commands(?MODULE, Cmds) end),
                if Elapsed > 1*1000*1000 ->
                        io:format(user, "~w,~w", [length(Seq), lists:map(fun(L) -> length(L) end, Pars) ]),
                        io:format(user, "=~w sec,", [Elapsed / 1000000]);
                   true ->
                        ok
                end,
                ?WHENFAIL(
                io:format("H: ~p~nHs: ~p~nR: ~w~n", [H,Hs,Res]),
                aggregate(command_names(Cmds),
                collect(if Len == 0 -> 0;
                           true     -> (Len div 10) + 1
                        end,
                        Res == ok)))
            end)).

seq_to_par_cmds(L) ->
    [Cmd || Cmd <- L,
            element(1, Cmd) /= init,
            element(3, element(3, Cmd)) /= reset].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

java_rpc(Node, reset, Stream) ->
    clear(Node, Stream),
    AllArgs = ["corfu_smrobject", "reset"],
    java_rpc_call(Node, AllArgs);
java_rpc(Node, Stream, Args) ->
    StreamStr = integer_to_list(Stream),
    AllArgs = ["corfu_smrobject", "-c", "localhost:8000",
               "-s", StreamStr, "org.corfudb.runtime.collections.SMRMap"]
              ++ Args,
    java_rpc_call(Node, AllArgs).

java_rpc_call(Node, AllArgs) ->
    ID = make_ref(),
    Node ! {self(), ID, AllArgs},
    receive
        {ID, Res} ->
            Res
    after 2*1000 ->
            timeout
    end.

