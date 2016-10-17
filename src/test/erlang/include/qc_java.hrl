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

-define(WRAP_ALWAYS(Num, Test),
        conjunction([{list_to_atom("always"++integer_to_list(Xqq__)), (Test)} ||
                        Xqq__ <- lists:seq(1, Num)])).

-define(PRETTY_FAIL(_Mod, _Cmds, H, S_or_Hs,Res, Check),
        ?WHENFAIL(io:format("H: ~p~nS: ~p~nR: ~p~n", [H,S_or_Hs,Res]),
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
