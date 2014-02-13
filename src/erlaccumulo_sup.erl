-module(erlaccumulo_sup).

-behaviour(supervisor).

-include("erlaccumulo.hrl").

%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(Pools) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Pools).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	%%{ok,Pools} = application:get_env(erlaccumulo,pools),
	{ok,[Props]}=file:consult("conf/erlaccumulo.config"),
	AccumuloConfigs=proplists:get_value(erlaccumulo,Props),
	Pools=proplists:get_value(pools,AccumuloConfigs),
	init(Pools);

init(Pools) ->
	ets:new(?ACCUMULO_CLIENT_POOLS_ETS,[set,named_table,public]),
	
    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
				  ets:insert(?ACCUMULO_CLIENT_POOLS_ETS,{Name,Name}),
				  PoolArgs = [{name, {local, Name}},
					      {worker_module, erlaccumulo_pool_worker}] ++ SizeArgs,
				  poolboy:child_spec(Name, PoolArgs, WorkerArgs)
			  end, Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.
