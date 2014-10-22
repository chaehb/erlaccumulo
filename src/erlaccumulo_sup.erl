%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(erlaccumulo_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
	
init(Args) ->
	try
		case Args of
			[] -> % use file in default path
				% {ok,App} = application:get_application(?MODULE),
				% Priv = code:priv_dir(App),
				% {ok, [DefaultConfig]} = file:consult(Priv++"/erlaccumulo.config"),
				{ok, [DefaultConfig]} = file:consult("priv/erlaccumulo.config"),
				setup(DefaultConfig);
			[ConfPath] ->
				{ok, [CustomConfig]} = file:consult(ConfPath),
				setup(CustomConfig);
			_ -> 
				throw(bad_args)
		end
	catch
		_:Reason ->
			exit(Reason,kill)
	end.

setup(Configs) ->
	Config = proplists:get_value(erlaccumulo,Configs),
	SupervisorConfig = proplists:get_value(supervisor,Config),
	MaxRestart = proplists:get_value(max_restart,SupervisorConfig),
	MaxTime = proplists:get_value(max_time,SupervisorConfig),
	
	ProxyServers = proplists:get_value(proxy_servers, Config),
	%% workers
	ChildWorkerSpecs = [
		%% data manipulation
		% {data_worker,
		% 	{data_worker,start_link,ProxyServers},
		% 	permanent, 5000,worker,
		% 	[data_worker]},
		%% controls for accumulo
		{accumulo_table_worker,
			{accumulo_table_worker,start_link,[ProxyServers]},
			permanent, 5000,worker,
			[accumulo_table_worker]},
		%%-------------------------------------------------------------
		%% not supported yet accumulo namespace functionality until v1.6.1
		%%-------------------------------------------------------------
		% {accumulo_namespace_worker,
		% 	{accumulo_namespace_worker,start_link,[ProxyServers]},
		% 	permanent, 5000,worker,
		% 	[accumulo_namespace_worker]},
		%%-------------------------------------------------------------
		{accumulo_security_worker,
			{accumulo_security_worker,start_link,[ProxyServers]},
			permanent, 5000,worker,
			[accumulo_security_worker]},
		{accumulo_instance_worker,
			{accumulo_instance_worker,start_link,[ProxyServers]},
			permanent, 5000,worker,
			[accumulo_instance_worker]}
	],
	ets:new(erlaccumulo,[set,named_table,public]),
    {ok, {{one_for_one, MaxRestart, MaxTime}, ChildWorkerSpecs}}.
