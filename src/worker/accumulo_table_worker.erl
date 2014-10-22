%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%		
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(accumulo_table_worker).
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
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args,[]).

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

%% return : {ok,set(binary())}	
handle_call({?ACCUMULO_LIST_TABLES}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_LIST_TABLES,[Conn#proxy_conn.login]) of
		{_,{ok, TableListSet}} ->			
			{ok,TableListSet}
	catch
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};


%% Params :: [TableName::binary()]
%% return : {ok, true | false}	
handle_call({?ACCUMULO_TABLE_EXISTS, [TableName]}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_TABLE_EXISTS,[Conn#proxy_conn.login, TableName]) of
		{_,{ok, TableExistBoolean}} ->			
			{ok,TableExistBoolean}
	catch
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};


%% return {ok,dict(binary(),binary())}
handle_call({?ACCUMULO_TABLE_ID_MAP}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_TABLE_ID_MAP,[Conn#proxy_conn.login]) of
		{_,{ok, TableIdMapDict}} ->			
			{ok,TableIdMapDict}
	catch
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params=[TableName::binary(),Versioning::boolean(),TimeType::int32()]
handle_call({?ACCUMULO_CREATE_TABLE, [TableName,Versioning,TimeType]}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CREATE_TABLE,[Conn#proxy_conn.login, TableName,Versioning,TimeType]) of
		{_,{ok, _}} ->			
			{ok, create_table};
		{_,{_,{Response,_,_}}} ->			
			{proxy_exception,Response}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

handle_call({?ACCUMULO_DELETE_TABLE, [TableName]}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_DELETE_TABLE,[Conn#proxy_conn.login, TableName]) of
		{_,{ok, _}} ->			
			{ok, delete_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

handle_call({?ACCUMULO_RENAME_TABLE, [TableName,NewTableName]}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_RENAME_TABLE,[Conn#proxy_conn.login, TableName,NewTableName]) of
		{_,{ok, _}} ->			
			{ok, rename_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TableName::binary(),NewTableName::binary(),Flush::boolean(),PropertiesToSet::dict(binary(),binary()),PropertiesToExclude::set(binary())]
handle_call({?ACCUMULO_CLONE_TABLE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CLONE_TABLE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, clone}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TableName::binary(),ExportDirectory::binary()]
handle_call({?ACCUMULO_EXPORT_TABLE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_EXPORT_TABLE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, export_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TableName::binary(),ImportDirectory::binary()]
handle_call({?ACCUMULO_IMPORT_TABLE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_IMPORT_TABLE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, import_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TableName::binary(),ImportDirectory::binary(),FailureDirectory::binary(),SetTime::boolean()]
handle_call({?ACCUMULO_IMPORT_DIRECTORY, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_IMPORT_DIRECTORY,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, import_directory}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TableName::binary(), StartRow::binary()|undefined, EndRow::binary()|undefined, Wait::boolean()]
handle_call({?ACCUMULO_FLUSH_TABLE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_FLUSH_TABLE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, flush_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params=[TableName::binary(), StartRow::binary()|undefined, EndRow::binary()|undefined, Iterators::[#iteratorSetting{}], Flush::boolean(), Wait::boolean()]
handle_call({?ACCUMULO_COMPACT_TABLE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_COMPACT_TABLE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, compact_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params =[TableName::binary()]
handle_call({?ACCUMULO_CANCEL_COMPACTION, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CANCEL_COMPACTION,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, cancel_compaction}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary()]
%% return : {ok, dict(binary(),set(binary()))}
handle_call({?ACCUMULO_GET_LOCALITY_GROUPS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_LOCALITY_GROUPS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, LocalityGroupsDict}} ->			
			{ok, LocalityGroupsDict}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(),LocalityGroups::dict(binary(),set(binary()))]
handle_call({?ACCUMULO_SET_LOCALITY_GROUPS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_SET_LOCALITY_GROUPS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, set_locality_groups}} ->			
			{ok, set_locality_groups}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TableName::binary(), StartRow::binary()|undefined, EndRow::binary()|undefined]
handle_call({?ACCUMULO_DELETE_ROWS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_DELETE_ROWS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, delete_rows}} ->			
			{ok, delete_rows}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params = [TableName::binary(), Auths::set(binary()), StartRow::binary(), StartInclusive::boolean(), EndRow::binary(), EndInclusive::boolean()]
%% return : {ok, RowId::binary()}
handle_call({?ACCUMULO_GET_MAX_ROW, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_MAX_ROW,[Conn#proxy_conn.login]++Params) of
		{_,{ok, MaxRowId}} ->			
			{ok, MaxRowId}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params=[TableName::binary()]
%% return : {ok, dict(binary(), binary())}
handle_call({?ACCUMULO_GET_TABLE_PROPERTIES, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_TABLE_PROPERTIES,[Conn#proxy_conn.login]++Params) of
		{_,{ok, PropertiesDict}} ->			
			{ok, PropertiesDict}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(), Property::binary(), Value::binary()]
handle_call({?ACCUMULO_SET_TABLE_PROPERTY, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_SET_TABLE_PROPERTY,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, set_table_property}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(), Property::binary()]
handle_call({?ACCUMULO_REMOVE_TABLE_PROPERTY, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_REMOVE_TABLE_PROPERTY,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, remove_table_property}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(), StartRow::binary(), EndRow::binary()]
handle_call({?ACCUMULO_MERGE_TABLETS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_MERGE_TABLETS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, merge_tablets}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(),Wait::boolean()]
handle_call({?ACCUMULO_OFFLINE_TABLE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_OFFLINE_TABLE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, offline_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(),Wait::boolean()]
handle_call({?ACCUMULO_ONLINE_TABLE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_ONLINE_TABLE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, online_table}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary()]
%% return : {ok,dict(binary(),int32())}
handle_call({?ACCUMULO_LIST_CONSTRAINTS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_LIST_CONSTRAINTS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, ConstraintsDict}} ->			
			{ok, ConstraintsDict}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(),ConstraintClassName::binary()]
%% return : {ok, int32()}
handle_call({?ACCUMULO_ADD_CONSTRAINT, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_ADD_CONSTRAINT,[Conn#proxy_conn.login]++Params) of
		{_,{ok, ConstraintId}} ->			
			{ok, ConstraintId}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(),Constraint::int32()]
handle_call({?ACCUMULO_REMOVE_CONSTRAINT, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_REMOVE_CONSTRAINT,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, remove_constraint}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [TableName::binary(),MaxSplits::int32()]
%% return : {ok, [binary()}
handle_call({?ACCUMULO_LIST_SPLITS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_LIST_SPLITS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, SplitsList}} ->			
			{ok, SplitsList}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary(), Splits::set(binary()) ]
handle_call({?ACCUMULO_ADD_SPLITS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_ADD_SPLITS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, add_splits}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary(), Range::#range{}, MaxSplits::int32() ]
%% return : {ok, set(#range{})}
handle_call({?ACCUMULO_SPLIT_RANGE_BY_TABLETS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_SPLIT_RANGE_BY_TABLETS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, RangesSet}} ->			
			{ok, RangesSet}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params :: [ TableName::binary() ]
%% return : {ok, dict(IteratorName::binary(), Scopes::set(Scope::int32()))}
handle_call({?ACCUMULO_LIST_ITERATORS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_LIST_ITERATORS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, IteratorsDict}} ->			
			{ok, IteratorsDict}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary(), IteratorName::binary(), Scope::int32()]
%% return : {ok, #iteratorSetting{}}
handle_call({?ACCUMULO_GET_ITERATOR_SETTING, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_ITERATOR_SETTING,[Conn#proxy_conn.login]++Params) of
		{_,{ok, IteratorSetting}} ->			
			{ok, IteratorSetting}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary(), IteratorSetting::#iteratorSetting{}, Scopes::set(Scope::int32()) ]
handle_call({?ACCUMULO_CHECK_ITERATOR_CONFLICTS, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CHECK_ITERATOR_CONFLICTS,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, check_iterator_conflicts}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary(), Setting::#iteratorSetting{}, Scopes::set(Scope::int32()) ]
handle_call({?ACCUMULO_ATTACH_ITERATOR, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_ATTACH_ITERATOR,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, attach_iterator}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary(), IteratorName::binary(), Scopes::set(Scope::int32()) ]
handle_call({?ACCUMULO_REMOVE_ITERATOR, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_REMOVE_ITERATOR,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, remove_iterator}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary() ]
handle_call({?ACCUMULO_CLEAR_LOCATOR_CACHE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_CLEAR_LOCATOR_CACHE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, _}} ->			
			{ok, clear_locator_cache}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableNames::[binary()] ]
%% return : {ok, #diskUsage{}}
handle_call({?ACCUMULO_GET_DISK_USAGE, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_GET_DISK_USAGE,[Conn#proxy_conn.login]++Params) of
		{_,{ok, DiskUsage}} ->			
			{ok, DiskUsage}
	catch
		_:{_,{_,{Reason,_}}} ->
			{exception,Reason};
		Error:Reason ->
			{Error,Reason}
	end,
    {reply, Reply, State};

%% Params : [ TableName::binary(), ClassName::binary(), AsTypeName::binary() ]
%% return : {ok, true | false }
handle_call({?ACCUMULO_TEST_TABLE_CLASS_LOAD, Params}, _From, State = {ConnLength, Conns}) ->
	Conn = lists:nth(erlaccumulo_util:worker_connection_index(?MODULE,ConnLength), Conns),
	Reply = try thrift_client:call(Conn#proxy_conn.connection,?ACCUMULO_TEST_TABLE_CLASS_LOAD,[Conn#proxy_conn.login]++Params) of
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

