%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(accumulo_security_worker).
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

%% Params : [User::binary(),Properties::dict(Key::binary(),Value::binary())]
%% return : {ok, Connecton}
handle_call({?ACCUMULO_AUTHENTICATE_USER, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_AUTHENTICATE_USER,[Conn#proxy_conn.login]++Params) of
		{Connecton,{ok, _AuthenticateBoolean}} ->			
			{ok, Connecton}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [User::binary(),Password::binary()]
handle_call({?ACCUMULO_CHANGE_LOCAL_USER_PASSWORD, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CHANGE_LOCAL_USER_PASSWORD,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, change_local_user_password}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [User::binary(),Authorizations::set(Authorization::binary())]
handle_call({?ACCUMULO_CHANGE_USER_AUTHORIZATIONS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CHANGE_USER_AUTHORIZATIONS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, change_user_authorizations}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [User::binary(),Password::binary()]
handle_call({?ACCUMULO_CREATE_LOCAL_USER, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CREATE_LOCAL_USER,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, create_local_user}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [User::binary()]
handle_call({?ACCUMULO_DROP_LOCAL_USER, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_DROP_LOCAL_USER,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, drop_local_user}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [User::binary()]
%% return : {ok, Authorizations::List(Authorization::binary())}
handle_call({?ACCUMULO_GET_USER_AUTHORIZATIONS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_USER_AUTHORIZATIONS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, AuthorizationsList}} ->			
			{ok, AuthorizationsList}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ User::string(), Permission::int32() ]
handle_call({?ACCUMULO_GRANT_SYSTEM_PERMISSION, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GRANT_SYSTEM_PERMISSION,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, grant_system_permission}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [User::string(),TableName::string(), Permission::int32()]
handle_call({?ACCUMULO_GRANT_TABLE_PERMISSION, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GRANT_TABLE_PERMISSION,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, grant_table_permission}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};


%% Params : [ User::string(), Permission::int32() ]
%% return : {ok, boolean()}
handle_call({?ACCUMULO_HAS_SYSTEM_PERMISSION, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_HAS_SYSTEM_PERMISSION,[Conn#proxy_conn.login]++Params) of
		{_,{ok, HasPermissionBoolean}} ->			
			{ok, HasPermissionBoolean}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};


%% Params : [User::string(),TableName::string(), Permission::int32()]
%% return : {ok, boolean()}
handle_call({?ACCUMULO_HAS_TABLE_PERMISSION, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_HAS_TABLE_PERMISSION,[Conn#proxy_conn.login]++Params) of
		{_,{ok, HasPermissionBoolean}} ->			
			{ok, HasPermissionBoolean}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};


%% Params : [ User::string(), Permission::int32() ]
handle_call({?ACCUMULO_REVOKE_SYSTEM_PERMISSION, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_REVOKE_SYSTEM_PERMISSION,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, revoke_system_permission}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [User::string(),TableName::string(), Permission::int32()]
handle_call({?ACCUMULO_REVOKE_TABLE_PERMISSION, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_REVOKE_TABLE_PERMISSION,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, revoke_table_permission}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% return : {ok, Users::set(User::binary())}
handle_call({?ACCUMULO_LIST_LOCAL_USERS}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_LIST_LOCAL_USERS,[Conn#proxy_conn.login]) of
		{_,{ok, UsersSet}} ->			
			{ok, UsersSet}
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

