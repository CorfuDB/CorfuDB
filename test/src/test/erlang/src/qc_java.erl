-module(qc_java).

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

-include("qc_java.hrl").

-export([local_mboxes/0, local_endpoint/0,
         local_endpoint_port/0, local_endpoint_host/0,
         endpoint2nodename/1,
         quick_mbox_endpoint/0,
         %% pretty format, pretty print
         pf/2, pp/2,
         pretty_filter/2, pp_format/2,
         rpc_call/3]).

local_mboxes() ->
    [cmdlet0, cmdlet1, cmdlet2, cmdlet3, cmdlet4,
     cmdlet5, cmdlet6, cmdlet7, cmdlet8, cmdlet9].

local_endpoint() ->
    ShortName = local_endpoint_host(),
    Port = local_endpoint_port(),
    ShortName ++ ":" ++ integer_to_list(Port).

local_endpoint_host() ->
    case os:getenv("CORFU_HOST") of
        false ->
            %% confirm that we're using short names
            false = net_kernel:longnames(),
            [_, SN] = string:tokens(atom_to_list(node()), "@"),
            SN;
        Host ->
            Host
    end.

local_endpoint_port() ->
    case os:getenv("CORFU_PORT") of
        false -> 8000;
        Port  -> list_to_integer(Port)
    end.

endpoint2nodename(Endpoint) ->
    [HostName, Port] = string:tokens(Endpoint, ":"),
    list_to_atom("corfu-" ++ Port ++ "@" ++ HostName).

quick_mbox_endpoint() ->
    Endpoint = local_endpoint(),
    [{hd(local_mboxes()), endpoint2nodename(Endpoint)}, Endpoint].

pp(Term, Mod) ->
    io:format("~s\n", [pf(Term, Mod)]).

pf(Term, Mod) ->
    try
        lists:flatten(pp_format(pretty_filter(Term, Mod), Mod))
    catch _:_ ->
            lists:flatten(io_lib:format("~p", [Term]))
    end.

pretty_filter([{
                [{init,State}|Sequential],
                 ParCmds
               }], Mod) ->
    PrettyState = pretty_filter(State, Mod),
    PrettySequential = pretty_filter(Sequential, Mod),
    PrettyParallel = [pretty_filter(Seq, Mod) || Seq <- ParCmds],
    PrettyParallel2 = [{parallel, X} || X <- PrettyParallel, X /= []],
    [PrettyState, {seq, PrettySequential}] ++ PrettyParallel2;
pretty_filter(L, Mod) when is_list(L) ->
    [pretty_filter(X, Mod) || X <- L];
pretty_filter(T, Mod) when is_tuple(T) ->
    case Mod:pretty_filter(T) of
        T ->
            list_to_tuple(pretty_filter(tuple_to_list(T), Mod));
        Res ->
            Res
    end;
pretty_filter(Term, Mod) ->
    Mod:pretty_filter(Term).

pp_format(Term, Mod) ->
    {ok, Defs} = pp_record:read("./src/" ++ atom_to_list(Mod) ++ ".erl"),
    NewTerm = pretty_filter(Term, Mod),
    lists:flatten(io_lib:format("~s", [pp_record:print(NewTerm, Defs)])).

rpc_call(Mbox, AllArgs, Timeout) ->
    ID = make_ref(),
    Mbox ! {self(), ID, AllArgs},
    receive
        {ID, Res} ->
            Res
    after Timeout ->
            timeout
    end.
