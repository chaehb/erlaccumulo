%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%		data CRUD
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(data_worker).
-behaviour(gen_server).

-include("erlaccumulo.hrl").

-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
	ProxyConns = lists:map(
		fun(ProxyConfig) ->
			Host = proplists:get_value(host,ProxyConfig),
			Port = proplists:get_value(port,ProxyConfig),
			User = proplists:get_value(user,ProxyConfig),
			Password = dict:store("password",proplists:get_value(password,ProxyConfig),dict:new()),
			
			{ok,Conn0} = thrift_client_util:new(Host,Port,accumuloProxy_thrift,[{framed,true}]),
			{Conn,{ok,Login}} = thrift_client:call(Conn0,login,[User,Password]),
			
			#proxy_conn{
				connection=Conn,
				login = Login
			}
		end,
		Args
	),
	
	ets:insert(erlaccumulo, {?MODULE,1}),
    {ok, {length(ProxyConns),ProxyConns}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

