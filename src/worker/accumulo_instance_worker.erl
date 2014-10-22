%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(accumulo_instance_worker).
-behaviour(gen_server).

-include("accumulo_proxy/proxy_types.hrl").
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

%% Params = [TabletServerAddress::binary()]
%% return : {ok, [#activeCompaction{}]}
handle_call({?ACCUMULO_GET_ACTIVE_COMPACTIONS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_ACTIVE_COMPACTIONS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, ActiveCompactionList}} ->			
			{ok, ActiveCompactionList}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TabletServerAddress::binary()]
%% return : {ok, ActiveScans::[#activeScan{}]}
handle_call({?ACCUMULO_GET_ACTIVE_SCANS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_ACTIVE_SCANS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, ActiveScansList}} ->			
			{ok, ActiveScansList}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};


%% return : {ok, dict(SiteConfiguration::binary(), Value::binary())}
handle_call({?ACCUMULO_GET_SITE_CONFIGURATION}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_SITE_CONFIGURATION,[Conn#proxy_conn.login]) of
		{_,{ok, SiteConfDict}} ->			
			{ok, SiteConfDict}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% return : {ok, dict(SystemConfiguration::binary(), Value::binary())}
handle_call({?ACCUMULO_GET_SYSTEM_CONFIGURATION}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_SYSTEM_CONFIGURATION,[Conn#proxy_conn.login]) of
		{_,{ok, SystemConfDict}} ->			
			{ok, SystemConfDict}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% return : {ok, [TabletServer::binary()]}
handle_call({?ACCUMULO_GET_TABLET_SERVERS}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_TABLET_SERVERS,[Conn#proxy_conn.login]) of
		{_,{ok, TabletServersList}} ->			
			{ok, TabletServersList}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TabletServerAddress::binary() ]
handle_call({?ACCUMULO_PING_TABLET_SERVER, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_PING_TABLET_SERVER,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, pong}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ Property::binary(), Value::binary()]
handle_call({?ACCUMULO_SET_PROPERTY, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_SET_PROPERTY,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, set_property}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ Property::binary()]
handle_call({?ACCUMULO_REMOVE_PROPERTY, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_REMOVE_PROPERTY,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, remove_property}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ ClassName::binary(), AsTypeName::binary() ]
%% return : {ok, true | false }
handle_call({?ACCUMULO_TEST_CLASS_LOAD, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_TEST_CLASS_LOAD,[Conn#proxy_conn.login]++Params) of
		{_,{ok, LoadTestResultBoolean}} ->			
			{ok, LoadTestResultBoolean}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

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

