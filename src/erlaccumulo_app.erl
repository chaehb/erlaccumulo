%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(erlaccumulo_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, StartArgs) ->
    erlaccumulo_sup:start_link(StartArgs).

stop(_State) ->
    ok.
