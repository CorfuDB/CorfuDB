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

-define(QUICK_MBOX, qc_java:quick_mbox_endpoint()).

-ifdef(PROPER).

%% Automagically import generator functions like choose(), frequency(), etc.
-include_lib("proper/include/proper.hrl").

%% Proper doesn't like postcondition() return values that are not
%% boolean().  So, for non-true return values, we wrap in ?ELSE() so
%% that EQC QuickCheck can be slightly more helpful in reporting
%% postcondition() failures.
-define(ELSE(_X), false).

-define(WRAP_ALWAYS(Num, Test),
        conjunction([{list_to_atom("always"++integer_to_list(Xqq__)), (Test)} ||
                        Xqq__ <- lists:seq(1, Num)])).

-define(PRETTY_FAIL(Mod, _Cmds, H, S_or_Hs,Res, Check),
        ?WHENFAIL(io:format("History:~n~s~n~nState(s):~n~s~n~nResult:~n~s~n~n~s~n~s~n",
                            [
                             qc_java:pf(H, Mod),
                             qc_java:pf(S_or_Hs, Mod),
                             qc_java:pf(Res, Mod),
                             "To fetch Erlang counterexample: proper:counterexample().",
                             ("To pretty print counterexample: " ++ atom_to_list(Mod) ++ ":pp(proper:counterexample()).")
                            ]
                           ),
                  Check)).

-define(COMMANDS_LENGTH(Cmds),
        case Cmds of
            {SeqCmds, ParCmdsLists} ->
                length(SeqCmds) + lists:foldl(fun(L, Acc) ->
                                                      length(L) + Acc
                                              end, 0, ParCmdsLists);
            _ when is_list(Cmds) ->
                length(Cmds)
        end).

-endif.

-ifdef(EQC).

%% Automagically import generator functions like choose(), frequency(), etc.
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-define(ELSE(X), (X)).

-define(WRAP_ALWAYS(Num, Test),
        begin
            ?ALWAYS(Num, Test)
        end).

-define(PRETTY_FAIL(Mod, Cmds, H,S_or_Hs,Res, Check),
        pretty_commands(Mod, Cmds, {H,S_or_Hs,Res},
                        Check)).

-define(COMMANDS_LENGTH(Cmds),
        commands_length(Cmds)).

-endif.
