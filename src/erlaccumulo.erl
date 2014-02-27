-module(erlaccumulo).

-include("erlaccumulo.hrl").

%-export([start/0]).
-export([default_pool/0]).

-export([
	list_tables/1,
	table_exists/2, table_id_map/1,
	create_table/2, delete_table/2, rename_table/2,
	clone_table/2, export_table/2, import_table/2, import_directory/2,
	flush_table/2, get_active_compactions/2, compact_table/2, cancel_compaction/2,
	get_locality_groups/2, set_locality_groups/2,
	delete_rows/2, get_max_row/2,
	get_table_properties/2, set_table_property/2, remove_table_property/2,
	has_table_permission/2, grant_table_permission/2, revoke_table_permission/2,
	merge_tablets/2, offline_table/2, online_table/2,
	list_constraints/2,add_constraint/2, remove_constraint/2,
	list_splits/2, add_splits/2, split_range_by_tablets/2,
	list_iterators/2, get_iterator_setting/2, check_iterator_conflicts/2, attach_iterator/2, remove_iterator/2
]).

-export([
	get_active_scans/2,
	create_scanner/2,create_batch_scanner/2,
	has_next/2, next_entry/2, next_k/2,
	close_scanner/2
]).

-export([
	create_writer/2,close_writer/2,
	update/2,flush/2,update_and_flush/2,
	get_row_range/2,get_following/2
]).

-export([
	list_local_users/1,
	create_local_user/2, drop_local_user/2, change_user_password/2,
	get_user_authorizations/2, 
	authenticate_user/2,
	change_user_authorizations/2
]).

-export([
		clear_locator_cache/2,
		get_site_configuration/2,
		get_system_configuration/2,
		get_tablet_servers/2,
		
		set_property/2,
		remove_property/2,
		
		has_system_permission/2, grant_system_permission/2, revoke_system_permission/2,
		
		ping_tablet_server/2,
		test_class_load/2,
		test_table_class_load/2
]).

-export([
	get_disk_usage/2,
	update_row_conditionally/2,
	create_conditional_writer/2,
	update_rows_conditionally/2,
	close_conditional_writer/2
]).

%start()-> ok.
	%application:start(thrift),
	%application:start(erlaccumulo).

%%================================================
%% table management
%%================================================

%% Params : []
%% return : {ok,Tables::set(Table::string{})}	
list_tables(PoolName) ->
	% case do_request(PoolName,?ACCUMULO_LIST_TABLES,[],true) of
	% 	{ok,Response} ->
	% 		Tables=lists:subtract(sets:to_list(Response),?SYSTEM_TABLES),
	% 		{ok,Tables};
	% 	Reply ->
	% 		Reply
	% end.
	case do_request(PoolName,?ACCUMULO_LIST_TABLES,[],true) of
		{ok, Response} ->
			%{ok,sets:subtract(Response,sets:from_list(?SYSTEM_TABLES))};
			{ok,Response};
		{_, Reply} ->
			{error, Reply}
	end.

%% Params : [TableName::string()]
%% retern : {ok, TableExists::boolean()}
table_exists(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_TABLE_EXISTS,Params,true).

%% Params : []
%% return {ok,dict(binary(),binary())}
table_id_map(PoolName) ->
	do_request(PoolName,?ACCUMULO_TABLE_ID_MAP,[],true).
		
%% Params=[TableName::string(),VersionIterator::boolean(),Type::int32]
%% return {ok,ok}
create_table(PoolName, Params) ->
	do_request(PoolName,?ACCUMULO_CREATE_TABLE,Params,true).

%% Params=[TableName::string()]
%% return {ok,ok}
delete_table(PoolName, Params) ->
	do_request(PoolName,?ACCUMULO_DELETE_TABLE,Params,true).

%% Params=[OldTableName::string(),NewTableName::string()]
%% return {ok,ok}
rename_table(PoolName, Params) ->
	do_request(PoolName,?ACCUMULO_RENAME_TABLE,Params,true).

%% Params =[TableName::string(), NewTableName::string(), Flush::boolean(), PropertiesToSet::dict(string(),string()), PropertiesToExclude::set(string())]
%% return {ok,ok}
clone_table(PoolName, Params) ->
	do_request(PoolName,?ACCUMULO_CLONE_TABLE, Params, true).
	
%% Params = [TableName::string(),ExportDirectory::string()]
%% return {ok,ok}
export_table(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_EXPORT_TABLE, Params, true).

%% Params = [TableName::string(),ImportDirectory::string()]
%% return {ok,ok}
import_table(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_IMPORT_TABLE, Params, true).

%% Params = [TableName::string(),ImportDirectory::string(),FailureDirectory::string(),SetTime::boolean()]
%% return {ok,ok}
import_directory(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_IMPORT_DIRECTORY, Params, true).

%% Params = [TableName::string(), StartRow::string()|undefined, EndRow::string()|undefined, Wait::boolean()]
%% return {ok,ok}
flush_table(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_FLUSH_TABLE, Params, true).

%% Params=[TabletServerAddress::string()]
%% return {ok,ActiveCompactions::list(ActiveCompaction::#activeCompaction{})}
get_active_compactions(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_ACTIVE_COMPACTIONS, Params, true).
	
%% Params=[TableName::string(), StartRow::string()|undefined, EndRow::string()|undefined, Iterators::list(Iterator#iteratorSetting{}), Flush::boolean(), Wait::boolean()]
%% return {ok,ok}
compact_table(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_COMPACT_TABLE, Params, true).
%% Params =[Tablename::string()]
%% return {ok,ok}
cancel_compaction(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CANCEL_COMPACTION, Params, true).
	
%% Params : [TableName::string()]
%% return : {ok, LocalityGroups::dict(string(),set(string()))}
get_locality_groups(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_GET_LOCALITY_GROUPS,Params,true).

%% Params : [TableName::string(),LocalityGroups::dict(string(),set(string()))]
%% return : {ok,ok}
set_locality_groups(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_SET_LOCALITY_GROUPS,Params,true).

%% Params = [TableName::string(), StartRow::string()|undefined, EndRow::string()|undefined]
%% return {ok,ok}
delete_rows(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_DELETE_ROWS,Params,true).

%% Params = [TableName, Auths, StartRow, StartInclusive, EndRow, EndInclusive]
%% return {ok,string()}
get_max_row(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_GET_MAX_ROW,Params,true).

%% Params=[TableName::string()]
%% return : {ok, Properties::dict(Property::string(),Value::string())}
get_table_properties(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_TABLE_PROPERTIES,Params,true).

%% Params : [TableName::string(), Property::string(), Value::string()]
%% return : {ok,ok}
set_table_property(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_SET_TABLE_PROPERTY,Params,true).
	
%% Params : [TableName::string(), Property::string()]
%% return : {ok,ok}
remove_table_property(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_REMOVE_TABLE_PROPERTY,Params,true).

%% Params : [User::string(),TableName::string(), Permission::int32()]
%% return : {ok,Reply::boolean()}
has_table_permission(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_HAS_TABLE_PERMISSION,Params,true).

%% Params : [User::string(),TableName::string(), Permission::int32()]
%% return : {ok,ok}
grant_table_permission(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GRANT_TABLE_PERMISSION,Params,true).

%% Params : [User::string(),TableName::string(), Permission::int32()]
%% return : {ok,ok}
revoke_table_permission(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_REVOKE_TABLE_PERMISSION,Params,true).

%% Params : [TableName::string(), StartRow::string(), EndRow::string()]
%% return : {ok,ok}
merge_tablets(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_MERGE_TABLETS,Params,true).

%% add wait parameter since 1.6
%% Params : [TableName::string(),Wait::boolean()]
%% return : {ok,ok}
offline_table(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_OFFLINE_TABLE,Params,true).

%% add wait parameter since 1.6
%% Params : [TableName::string(),Wait::booean()]
%% return : {ok,ok}
online_table(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_ONLINE_TABLE,Params,true).

%% Params : [TableName::string()]
%% return : {ok,Constraints::dict(ConstraintClassName::string(),Constraint::int32())}
list_constraints(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_LIST_CONSTRAINTS,Params,true).

%% Params : [TableName::string(),ConstraintClassName::string()]
%% return : {ok,Constraint::int32()}
add_constraint(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_ADD_CONSTRAINT,Params,true).

%% Params : [TableName::string(),Constraint::int32()]
%% return : {ok,ok}
remove_constraint(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_REMOVE_CONSTRAINT,Params,true).

%% Params : [TableName::string(),MaxSplits::int32]
%% return : {ok,Splits::list(Split::string())}
list_splits(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_LIST_SPLITS,Params,true).

%% Params : [ TableName::string(), Splits::set(Split::string()) ]
%% return : {ok,ok}
add_splits(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_ADD_SPLITS,Params,true).

%% Params : [ TableName::string(), Range::#range{},MaxSplits::int32() ]
%% return : {ok, SplitRange::set(Range::#range{})}
split_range_by_tablets(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_SPLIT_RANGE_BY_TABLETS,Params,true).

%% Params :: [ TableName::string() ]
%% return : {ok,Iterators::dict(IteratorName::string(), Scopes::set(Scope::int32()))}
list_iterators(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_LIST_ITERATORS,Params,true).

%% Params : [ TableName::string(), IteratorName::string(), Scope::int32()]
%% return : {ok, IteratorSetting::#iteratorSetting{}}
get_iterator_setting(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_ITERATOR_SETTING,Params,true).

%% Params : [ TableName::string(), Setting::#iteratorSetting{}, Scopes::set(Scope::int32()) ]
%% return : {ok, ok}
check_iterator_conflicts(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CHECK_ITERATOR_CONFLICTS,Params,true).

%% Params : [ TableName::string(), Setting::#iteratorSetting{}, Scopes::set(Scope::int32()) ]
%% return : {ok, ok}
attach_iterator(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_ATTACH_ITERATOR,Params,true).

%% Params : [ TableName::string(), IteratorName::string(), Scopes::set(Scope::int32()) ]
%% return : {ok,ok}
remove_iterator(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_REMOVE_ITERATOR,Params,true).


%%================================================
%% Scanner
%%================================================

%% Params = [TabletServerAddress::string()]
%% {ok, ActiveScans::list(ActiveScan::#activeScan{})}
get_active_scans(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_ACTIVE_SCANS,Params,true).

%% Params = [TableName::string(),ScanOptions::#scanOptions{}]
%% {ok,Scanner::string()}
create_scanner(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CREATE_SCANNER,Params,true).

%% Params = [TableName::string(),BatchScanOptions::#batchScanOptions{}]
%% {ok,BatchScanner::string()}
create_batch_scanner(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CREATE_BATCH_SCANNER,Params,true).

%% Params = [ Scanner::string()]
%% {ok,HasNext::boolean()}
has_next(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_HAS_NEXT,Params,false).

%% Params = [ Scanner::string()]
%% {ok,NextEntry::#keyValueAndPeek{}}
next_entry(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_NEXT_ENTRY,Params,false).
	
%% Params = [ Scanner::string(),K::int32()]
%% {ok,NextK::#scanREsult{}}
next_k(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_NEXT_K,Params,false).

%% Params = [ Scanner::string()]
%% {ok,ok}
close_scanner(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_CLOSE_SCANNER,Params,false).
	
%%================================================
%% Writer
%%================================================
%% Params=[
%% 	  TableName::string(), 
%%	  Options::#writerOptions{}
%%	]
%% return : {ok,Writer::string()}
create_writer(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CREATE_WRITER,Params,true).

%% Params = [Writer::string()]
%% return : {ok,ok}
close_writer(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_CLOSE_WRITER,Params,false).

%% Params = [Writer::string(),
%%           	Cells::dict(
%%					RowID::string(),
%%					list(Cell#columnUpdate{})
%%				)
%%          ]
%% return : {ok,ok}
update(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_UPDATE,Params,false).
	

%% Params = [Writer::string()]
%% return : {ok, ok}
flush(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_FLUSH,Params,false).

%% Params = [TableName::string, Cells]
%% return : { ok, ok }
update_and_flush(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_UPDATE_AND_FLUSH,Params,true).

%% Params = [Row::string()]
%% return : Range#range{}
get_row_range(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_GET_ROW_RANGE,Params,false).

%% Params = [ Key::#key{},Part::int32]
%% return : {ok, FollowingKey#key{}}
get_following(PoolName,Params)->
	do_request(PoolName,?ACCUMULO_GET_FOLLOWING,Params,false).


%%================================================
%% Authentication & Account Mangement
%%================================================

%% Params :
%% return : {ok, Users::list(User::string())}
list_local_users(PoolName) ->
	case do_request(PoolName,?ACCUMULO_LIST_LOCAL_USERS,[],true) of
		{ok,Response} ->
			Users=lists:subtract(sets:to_list(Response),?SYSTEM_ACCOUNTS),
			{ok,Users};
		Reply ->
			Reply
	end.

%% Params : [User::string(),Password::string()]
%% return : {ok, ok}
create_local_user(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CREATE_LOCAL_USER,Params,true).

%% Params : [User::string()]
%% return : {ok, ok}
drop_local_user(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_DROP_LOCAL_USER,Params,true).

%% Params : [User::string(),Password::string()]
%% return : {ok, ok}
change_user_password(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CHANGE_LOCAL_USER_PASSWORD,Params,true).

%% Params : [User::string()]
%% return : {ok, Authorizations::List(Authorization::string())}
get_user_authorizations(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_USER_AUTHORIZATIONS,Params,true).

%% Params : [User::string(),Properties::dict(Key::string(),Value::string())]
%% return : {ok, Authenticate::boolean()}
authenticate_user(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_AUTHENTICATE_USER,Params,true).

%% Params : [User::string(),Authorizations::set(Authorization::string())]
%% return : {ok, ok}
change_user_authorizations(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CHANGE_USER_AUTHORIZATIONS,Params,true).

%%================================================
%% System management & configurations
%%================================================

%% Params : [ TableName::string() ]
%% return : {ok,ok}
clear_locator_cache(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_CLEAR_LOCATOR_CACHE,Params,true).

%% Params : [ ]
%% return : {ok,SiteConfigurations::dict(SiteConfiguration::string(), Value::string())}
get_site_configuration(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_SITE_CONFIGURATION,Params,true).

%% Params : [ ]
%% return : {ok,SystemConfigurations::dict(SystemConfiguration::string(), Value::string())}
get_system_configuration(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_SYSTEM_CONFIGURATION,Params,true).

%% Params : []
%% return : {ok, TabletServers::list(TabletServer::string())}
get_tablet_servers(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GET_TABLET_SERVERS,Params,true).

%% Params : [ Property::string(), Value::string()]
%% return : {ok,ok}
set_property(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_SET_PROPERTY,Params,true).

%% Params : [ Property::string() ]
%% return : {ok,ok}
remove_property(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_REMOVE_PROPERTY,Params,true).

%% Params : [ User::string(), Permission::int32() ]
%% return : {ok, HasPermission::boolean()}
has_system_permission(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_HAS_SYSTEM_PERMISSION,Params,true).

%% Params : [ User::string(), Permission::int32() ]
%% return : {ok, ok}
grant_system_permission(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_GRANT_SYSTEM_PERMISSION,Params,true).

%% Params : [ User::string(), Permission::int32() ]
%% return : {ok, ok}
revoke_system_permission(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_REVOKE_SYSTEM_PERMISSION,Params,true).

%% Params : [ TabletServerAddress::string() ]
%% return : {ok, ok}
ping_tablet_server(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_PING_TABLET_SERVER,Params,true).

%% Param : [ ClassName::string(), AsTypeName::string() ]
%% return : {ok, Result::boolean()}
test_class_load(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_TEST_CLASS_LOAD,Params,true).

%% Param : [ TableName::string(), ClassName::string(), AsTypeName::string() ]
%% return : {ok, Result::boolean()}
test_table_class_load(PoolName,Params) ->
	do_request(PoolName,?ACCUMULO_TEST_TABLE_CLASS_LOAD,Params,true).

%%================================================
%% new service since 1.6.0
%%================================================

%% Param : [ TableNames::set(TableName::string()) ]
%% return : {ok,Result::#diskUsage{}}
get_disk_usage(PoolName, Params) ->
	do_request(PoolName, ?ACCUMULO_GET_DISK_USAGE, Params, true).

%% Param : [ TableName::string(), Row::string(), Update::#conditionalUpdates]
%% return : {ok, Result::int32()}
update_row_conditionally(PoolName, Params) ->
	do_request(PoolName, ?ACCUMULO_UPDATE_ROW_CONDITIONALLY, Params, true).

%% Param : [TableName::string(), Options::#conditionalWriterOptions{}]
%% return : {ok, ConditionalWriter::string()}
create_conditional_writer(PoolName, Params) ->
	do_request(PoolName, ?ACCUMULO_CREATE_CONDITIONAL_WRITER, Params, true).

%% Param : [ ConditionalWriter::string(), Update::#conditionalUpdates]
%% return : {ok, Result::dict(Row::string, Update::int32())}
update_rows_conditionally(PoolName, Params) ->
	do_request(PoolName, ?ACCUMULO_UPDATE_ROWS_CONDITIONALLY, Params, false).

%% Param : [ConditionalWriter::string()]
%% return : {ok,ok}
close_conditional_writer(PoolName, Params) ->
	do_request(PoolName, ?ACCUMULO_CLOSE_CONDITIONAL_WRITER, Params, false).

%%================================================
%% utility methods
%%================================================

%% DefaultPool::atom()
default_pool() ->
	Pool=ets:first(?ACCUMULO_CLIENT_POOLS_ETS),
	Pool#accumulo_pool.pool.
		
%%================================================
%% Internal methods
%%================================================
do_request(PoolName,Function,Params,NeedLogin)->
	try
		poolboy:transaction(PoolName,
			fun(Worker) ->
				try
					gen_server:call(Worker,{Function,Params,NeedLogin})
				catch
					_:Reason ->
						{error,Reason}
				end
			end
		)
	catch
		_:Reason ->
			{error,Reason}
	end.

	
