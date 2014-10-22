%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%		uncategorized functions of accumulo connector 
%%%		and some custom utility functions
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(erlaccumulo_util).

-export([worker_connection_index/2]).
-export([throw_exception/2]).

worker_connection_index(Worker,ConnLength) ->
	[{_,Index}] = ets:lookup(erlaccumulo,Worker),
	ets:insert(erlaccumulo,{Worker, 3 - ((Index+1) rem ConnLength ) }),
	Index.

throw_exception(Error,Reason) ->
	io:format("~nException :~p~n~p~n",[Error,Reason]).