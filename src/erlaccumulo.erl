-module(erlaccumulo).

-include("erlaccumulo.hrl").

%-export([start/0]).

-export([
	list_tables/0,
	table_exists/1, table_id_map/0,
	create_table/3, delete_table/1, rename_table/2,
	clone_table/5, export_table/2, import_table/2, import_directory/4,
	flush_table/4, get_active_compactions/1, compact_table/6, cancel_compaction/1,
	get_locality_groups/1, set_locality_groups/2,
	delete_rows/3, get_max_row/6,
	get_table_properties/1, set_table_property/3, remove_table_property/2,
	has_table_permission/3, grant_table_permission/3, revoke_table_permission/3,
	merge_tablets/3, offline_table/2, online_table/2,
	list_constraints/1,add_constraint/2, remove_constraint/2,
	list_splits/2, add_splits/2, split_range_by_tablets/3,
	list_iterators/1, get_iterator_setting/3, check_iterator_conflicts/3, attach_iterator/3, remove_iterator/3
]).

-export([
	get_active_scans/1,
	create_scanner/2,create_batch_scanner/2,
	has_next/1, next_entry/1, next_k/2,
	close_scanner/1
]).

-export([
	create_writer/2,close_writer/1,
	update/2,flush/1,update_and_flush/2,
	get_row_range/1,get_following/2
]).

-export([
	list_local_users/0,
	create_local_user/2, drop_local_user/1, change_user_password/2,
	get_user_authorizations/1, 
	authenticate_user/2,
	change_user_authorizations/2
]).

-export([
		clear_locator_cache/1,
		get_site_configuration/0,
		get_system_configuration/0,
		get_tablet_servers/0,
		
		set_property/2,
		remove_property/1,
		
		has_system_permission/2, grant_system_permission/2, revoke_system_permission/2,
		
		ping_tablet_server/1,
		test_class_load/2,
		test_table_class_load/3
]).

-export([
	get_disk_usage/1,
	update_row_conditionally/3,
	create_conditional_writer/2,
	update_rows_conditionally/2,
	close_conditional_writer/1
]).

%start()-> ok.
	%application:start(thrift),
	%application:start(erlaccumulo).

%%================================================
%% table management
%%================================================

%% Params : []
%% return : {ok,Tables::set(Table::string{})}	
list_tables() ->
	% case do_request(PoolName,?ACCUMULO_LIST_TABLES,[],true) of
	% 	{ok,Response} ->
	% 		Tables=lists:subtract(sets:to_list(Response),?SYSTEM_TABLES),
	% 		{ok,Tables};
	% 	Reply ->
	% 		Reply
	% end.
	case do_request(?ACCUMULO_LIST_TABLES,[],true) of
		{ok, Response} ->
			%{ok,sets:subtract(Response,sets:from_list(?SYSTEM_TABLES))};
			{ok,Response};
		{_, Reply} ->
			{error, Reply}
	end.

%% Params : [TableName::string()]
%% retern : {ok, TableExists::boolean()}
table_exists(TableName) ->
	do_request(?ACCUMULO_TABLE_EXISTS,[TableName],true).

%% Params : []
%% return {ok,dict(binary(),binary())}
table_id_map() ->
	do_request(?ACCUMULO_TABLE_ID_MAP,[],true).
		
%% Params=[TableName::string(),VersionIterator::boolean(),Type::int32]
%% return {ok,ok}
create_table(TableName,VersionIterator,Type) ->
	do_request(?ACCUMULO_CREATE_TABLE,[TableName,VersionIterator,Type],true).

%% Params=[TableName::string()]
%% return {ok,ok}
delete_table(TableName) ->
	do_request(?ACCUMULO_DELETE_TABLE,[TableName],true).

%% Params=[OldTableName::string(),NewTableName::string()]
%% return {ok,ok}
rename_table(OldTableName,NewTableName) ->
	do_request(?ACCUMULO_RENAME_TABLE,[OldTableName,NewTableName],true).

%% Params =[TableName::string(), NewTableName::string(), Flush::boolean(), PropertiesToSet::dict(string(),string()), PropertiesToExclude::set(string())]
%% return {ok,ok}
clone_table(TableName,NewTableName,Flush,PropertiesToSet,PropertiesToExclude) ->
	do_request(?ACCUMULO_CLONE_TABLE, [TableName,NewTableName,Flush,PropertiesToSet,PropertiesToExclude], true).
	
%% Params = [TableName::string(),ExportDirectory::string()]
%% return {ok,ok}
export_table(TableName,ExportDirectory) ->
	do_request(?ACCUMULO_EXPORT_TABLE, [TableName,ExportDirectory], true).

%% Params = [TableName::string(),ImportDirectory::string()]
%% return {ok,ok}
import_table(TableName,ImportDirectory) ->
	do_request(?ACCUMULO_IMPORT_TABLE, [TableName,ImportDirectory], true).

%% Params = [TableName::string(),ImportDirectory::string(),FailureDirectory::string(),SetTime::boolean()]
%% return {ok,ok}
import_directory(TableName,ImportDirectory,FailureDirectory,SetTime) ->
	do_request(?ACCUMULO_IMPORT_DIRECTORY, [TableName,ImportDirectory,FailureDirectory,SetTime], true).

%% Params = [TableName::string(), StartRow::string()|undefined, EndRow::string()|undefined, Wait::boolean()]
%% return {ok,ok}
flush_table(TableName, StartRow, EndRow, Wait) ->
	do_request(?ACCUMULO_FLUSH_TABLE, [TableName, StartRow, EndRow, Wait], true).

%% Params=[TabletServerAddress::string()]
%% return {ok,ActiveCompactions::list(ActiveCompaction::#activeCompaction{})}
get_active_compactions(TabletServerAddress) ->
	do_request(?ACCUMULO_GET_ACTIVE_COMPACTIONS, [TabletServerAddress], true).
	
%% Params=[TableName::string(), StartRow::string()|undefined, EndRow::string()|undefined, Iterators::list(Iterator#iteratorSetting{}), Flush::boolean(), Wait::boolean()]
%% return {ok,ok}
compact_table(TableName, StartRow, EndRow, Iterators, Flush, Wait) ->
	do_request(?ACCUMULO_COMPACT_TABLE, [TableName, StartRow, EndRow, Iterators, Flush, Wait], true).
%% Params =[Tablename::string()]
%% return {ok,ok}
cancel_compaction(Tablename) ->
	do_request(?ACCUMULO_CANCEL_COMPACTION, [Tablename], true).
	
%% Params : [TableName::string()]
%% return : {ok, LocalityGroups::dict(string(),set(string()))}
get_locality_groups(Tablename)->
	do_request(?ACCUMULO_GET_LOCALITY_GROUPS,[Tablename],true).

%% Params : [TableName::string(),LocalityGroups::dict(string(),set(string()))]
%% return : {ok,ok}
set_locality_groups(Tablename,LocalityGroups)->
	do_request(?ACCUMULO_SET_LOCALITY_GROUPS,[Tablename,LocalityGroups],true).

%% Params = [TableName::string(), StartRow::string()|undefined, EndRow::string()|undefined]
%% return {ok,ok}
delete_rows(Tablename,StartRow,EndRow)->
	do_request(?ACCUMULO_DELETE_ROWS,[Tablename,StartRow,EndRow],true).

%% Params = [TableName, Auths, StartRow, StartInclusive, EndRow, EndInclusive]
%% return {ok,string()}
get_max_row(TableName, Auths, StartRow, StartInclusive, EndRow, EndInclusive)->
	do_request(?ACCUMULO_GET_MAX_ROW,[TableName, Auths, StartRow, StartInclusive, EndRow, EndInclusive],true).

%% Params=[TableName::string()]
%% return : {ok, Properties::dict(Property::string(),Value::string())}
get_table_properties(TableName) ->
	do_request(?ACCUMULO_GET_TABLE_PROPERTIES,[TableName],true).

%% Params : [TableName::string(), Property::string(), Value::string()]
%% return : {ok,ok}
set_table_property(TableName,Property, Value) ->
	do_request(?ACCUMULO_SET_TABLE_PROPERTY,[TableName,Property, Value],true).
	
%% Params : [TableName::string(), Property::string()]
%% return : {ok,ok}
remove_table_property(TableName, Property) ->
	do_request(?ACCUMULO_REMOVE_TABLE_PROPERTY,[TableName, Property],true).

%% Params : [User::string(),TableName::string(), Permission::int32()]
%% return : {ok,Reply::boolean()}
has_table_permission(User,TableName, Permission) ->
	do_request(?ACCUMULO_HAS_TABLE_PERMISSION,[User,TableName, Permission],true).

%% Params : [User::string(),TableName::string(), Permission::int32()]
%% return : {ok,ok}
grant_table_permission(User,TableName, Permission) ->
	do_request(?ACCUMULO_GRANT_TABLE_PERMISSION,[User,TableName, Permission],true).

%% Params : [User::string(),TableName::string(), Permission::int32()]
%% return : {ok,ok}
revoke_table_permission(User,TableName, Permission) ->
	do_request(?ACCUMULO_REVOKE_TABLE_PERMISSION,[User,TableName, Permission],true).

%% Params : [TableName::string(), StartRow::string(), EndRow::string()]
%% return : {ok,ok}
merge_tablets(TableName,StartRow,EndRow) ->
	do_request(?ACCUMULO_MERGE_TABLETS,[TableName,StartRow,EndRow],true).

%% add wait parameter since 1.6
%% Params : [TableName::string(),Wait::boolean()]
%% return : {ok,ok}
offline_table(TableName,Wait) ->
	do_request(?ACCUMULO_OFFLINE_TABLE,[TableName,Wait],true).

%% add wait parameter since 1.6
%% Params : [TableName::string(),Wait::booean()]
%% return : {ok,ok}
online_table(TableName,Wait) ->
	do_request(?ACCUMULO_ONLINE_TABLE,[TableName,Wait],true).

%% Params : [TableName::string()]
%% return : {ok,Constraints::dict(ConstraintClassName::string(),Constraint::int32())}
list_constraints(TableName) ->
	do_request(?ACCUMULO_LIST_CONSTRAINTS,[TableName],true).

%% Params : [TableName::string(),ConstraintClassName::string()]
%% return : {ok,Constraint::int32()}
add_constraint(TableName,ConstraintClassName) ->
	do_request(?ACCUMULO_ADD_CONSTRAINT,[TableName,ConstraintClassName],true).

%% Params : [TableName::string(),Constraint::int32()]
%% return : {ok,ok}
remove_constraint(TableName,Constraint) ->
	do_request(?ACCUMULO_REMOVE_CONSTRAINT,[TableName,Constraint],true).

%% Params : [TableName::string(),MaxSplits::int32]
%% return : {ok,Splits::list(Split::string())}
list_splits(TableName,MaxSplits) ->
	do_request(?ACCUMULO_LIST_SPLITS,[TableName,MaxSplits],true).

%% Params : [ TableName::string(), Splits::set(Split::string()) ]
%% return : {ok,ok}
add_splits(TableName,Splits) ->
	do_request(?ACCUMULO_ADD_SPLITS,[TableName,Splits],true).

%% Params : [ TableName::string(), Range::#range{},MaxSplits::int32() ]
%% return : {ok, SplitRange::set(Range::#range{})}
split_range_by_tablets(TableName,Range,MaxSplits) ->
	do_request(?ACCUMULO_SPLIT_RANGE_BY_TABLETS,[TableName,Range,MaxSplits],true).

%% Params :: [ TableName::string() ]
%% return : {ok,Iterators::dict(IteratorName::string(), Scopes::set(Scope::int32()))}
list_iterators(TableName) ->
	do_request(?ACCUMULO_LIST_ITERATORS,[TableName],true).

%% Params : [ TableName::string(), IteratorName::string(), Scope::int32()]
%% return : {ok, IteratorSetting::#iteratorSetting{}}
get_iterator_setting(TableName,IteratorName, Scopes) ->
	do_request(?ACCUMULO_GET_ITERATOR_SETTING,[TableName,IteratorName, Scopes],true).

%% Params : [ TableName::string(), Setting::#iteratorSetting{}, Scopes::set(Scope::int32()) ]
%% return : {ok, ok}
check_iterator_conflicts(TableName,Setting, Scopes) ->
	do_request(?ACCUMULO_CHECK_ITERATOR_CONFLICTS,[TableName,Setting, Scopes],true).

%% Params : [ TableName::string(), Setting::#iteratorSetting{}, Scopes::set(Scope::int32()) ]
%% return : {ok, ok}
attach_iterator(TableName,Setting, Scopes) ->
	do_request(?ACCUMULO_ATTACH_ITERATOR,[TableName,Setting, Scopes],true).

%% Params : [ TableName::string(), IteratorName::string(), Scopes::set(Scope::int32()) ]
%% return : {ok,ok}
remove_iterator(TableName,IteratorName, Scopes) ->
	do_request(?ACCUMULO_REMOVE_ITERATOR,[TableName,IteratorName, Scopes],true).


%%================================================
%% Scanner
%%================================================

%% Params = [TabletServerAddress::string()]
%% {ok, ActiveScans::list(ActiveScan::#activeScan{})}
get_active_scans(TabletServerAddress) ->
	do_request(?ACCUMULO_GET_ACTIVE_SCANS,[TabletServerAddress],true).

%% Params = [TableName::string(),ScanOptions::#scanOptions{}]
%% {ok,Scanner::string()}
create_scanner(TableName,ScanOptions) ->
	do_request(?ACCUMULO_CREATE_SCANNER,[TableName,ScanOptions],true).

%% Params = [TableName::string(),BatchScanOptions::#batchScanOptions{}]
%% {ok,BatchScanner::string()}
create_batch_scanner(TableName,BatchScanOptions) ->
	do_request(?ACCUMULO_CREATE_BATCH_SCANNER,[TableName,BatchScanOptions],true).

%% Params = [ Scanner::string()]
%% {ok,HasNext::boolean()}
has_next(Scanner)->
	do_request(?ACCUMULO_HAS_NEXT,[Scanner],false).

%% Params = [ Scanner::string()]
%% {ok,NextEntry::#keyValueAndPeek{}}
next_entry(Scanner)->
	do_request(?ACCUMULO_NEXT_ENTRY,[Scanner],false).
	
%% Params = [ Scanner::string(),K::int32()]
%% {ok,NextK::#scanResult{}}
next_k(Scanner,K)->
	do_request(?ACCUMULO_NEXT_K,[Scanner,K],false).

%% Params = [ Scanner::string()]
%% {ok,ok}
close_scanner(Scanner)->
	do_request(?ACCUMULO_CLOSE_SCANNER,[Scanner],false).
	
%%================================================
%% Writer
%%================================================
%% Params=[
%% 	  TableName::string(), 
%%	  Options::#writerOptions{}
%%	]
%% return : {ok,Writer::string()}
create_writer(TableName, Options) ->
	do_request(?ACCUMULO_CREATE_WRITER,[TableName, Options],true).

%% Params = [Writer::string()]
%% return : {ok,ok}
close_writer(Writer)->
	do_request(?ACCUMULO_CLOSE_WRITER,[Writer],false).

%% Params = [Writer::string(),
%%           	Cells::dict(
%%					RowID::string(),
%%					list(Cell#columnUpdate{})
%%				)
%%          ]
%% return : {ok,ok}
update(Writer, Cells)->
	do_request(?ACCUMULO_UPDATE,[Writer, Cells],false).
	

%% Params = [Writer::string()]
%% return : {ok, ok}
flush(Writer)->
	do_request(?ACCUMULO_FLUSH,[Writer],false).

%% Params = [TableName::string, Cells]
%% return : { ok, ok }
update_and_flush(TableName, Cells) ->
	do_request(?ACCUMULO_UPDATE_AND_FLUSH,[TableName, Cells],true).

%% Params = [Row::string()]
%% return : Range#range{}
get_row_range(Row)->
	do_request(?ACCUMULO_GET_ROW_RANGE,[Row],false).

%% Params = [ Key::#key{},Part::int32]
%% return : {ok, FollowingKey#key{}}
get_following(Key, Part)->
	do_request(?ACCUMULO_GET_FOLLOWING,[Key, Part],false).


%%================================================
%% Authentication & Account Mangement
%%================================================

%% Params :
%% return : {ok, Users::set(User::string())}
list_local_users() ->
	% case do_request(?ACCUMULO_LIST_LOCAL_USERS,[],true) of
	% 	{ok,Response} ->
	% 		Users=lists:subtract(sets:to_list(Response),?SYSTEM_ACCOUNTS),
	% 		{ok,Users};
	% 	Reply ->
	% 		Reply
	% end.
	do_request(?ACCUMULO_LIST_LOCAL_USERS,[],true).

%% Params : [User::string(),Password::string()]
%% return : {ok, ok}
create_local_user(User,Password) ->
	do_request(?ACCUMULO_CREATE_LOCAL_USER,[User,Password],true).

%% Params : [User::string()]
%% return : {ok, ok}
drop_local_user(User) ->
	do_request(?ACCUMULO_DROP_LOCAL_USER,[User],true).

%% Params : [User::string(),Password::string()]
%% return : {ok, ok}
change_user_password(User,Password) ->
	do_request(?ACCUMULO_CHANGE_LOCAL_USER_PASSWORD,[User,Password],true).

%% Params : [User::string()]
%% return : {ok, Authorizations::List(Authorization::string())}
get_user_authorizations(User) ->
	do_request(?ACCUMULO_GET_USER_AUTHORIZATIONS,[User],true).

%% Params : [User::string(),Properties::dict(Key::string(),Value::string())]
%% return : {ok, Authenticate::boolean()}
authenticate_user(User, Properties) ->
	do_request(?ACCUMULO_AUTHENTICATE_USER,[User, Properties],true).

%% Params : [User::string(),Authorizations::set(Authorization::string())]
%% return : {ok, ok}
change_user_authorizations(User, Authorizations) ->
	do_request(?ACCUMULO_CHANGE_USER_AUTHORIZATIONS,[User, Authorizations],true).

%%================================================
%% System management & configurations
%%================================================

%% Params : [ TableName::string() ]
%% return : {ok,ok}
clear_locator_cache(TableName) ->
	do_request(?ACCUMULO_CLEAR_LOCATOR_CACHE,[TableName],true).

%% Params : [ ]
%% return : {ok,SiteConfigurations::dict(SiteConfiguration::string(), Value::string())}
get_site_configuration() ->
	do_request(?ACCUMULO_GET_SITE_CONFIGURATION,[],true).

%% Params : [ ]
%% return : {ok,SystemConfigurations::dict(SystemConfiguration::string(), Value::string())}
get_system_configuration() ->
	do_request(?ACCUMULO_GET_SYSTEM_CONFIGURATION,[],true).

%% Params : []
%% return : {ok, TabletServers::list(TabletServer::string())}
get_tablet_servers() ->
	do_request(?ACCUMULO_GET_TABLET_SERVERS,[],true).

%% Params : [ Property::string(), Value::string()]
%% return : {ok,ok}
set_property(Property, Value) ->
	do_request(?ACCUMULO_SET_PROPERTY,[Property, Value],true).

%% Params : [ Property::string() ]
%% return : {ok,ok}
remove_property(Property) ->
	do_request(?ACCUMULO_REMOVE_PROPERTY,[Property],true).

%% Params : [ User::string(), Permission::int32() ]
%% return : {ok, HasPermission::boolean()}
has_system_permission(User,Permission) ->
	do_request(?ACCUMULO_HAS_SYSTEM_PERMISSION,[User,Permission],true).

%% Params : [ User::string(), Permission::int32() ]
%% return : {ok, ok}
grant_system_permission(User,Permission) ->
	do_request(?ACCUMULO_GRANT_SYSTEM_PERMISSION,[User,Permission],true).

%% Params : [ User::string(), Permission::int32() ]
%% return : {ok, ok}
revoke_system_permission(User,Permission) ->
	do_request(?ACCUMULO_REVOKE_SYSTEM_PERMISSION,[User,Permission],true).

%% Params : [ TabletServerAddress::string() ]
%% return : {ok, ok}
ping_tablet_server(TabletServerAddress) ->
	do_request(?ACCUMULO_PING_TABLET_SERVER,[TabletServerAddress],true).

%% Param : [ ClassName::string(), AsTypeName::string() ]
%% return : {ok, Result::boolean()}
test_class_load(ClassName, AsTypeName) ->
	do_request(?ACCUMULO_TEST_CLASS_LOAD,[ClassName, AsTypeName],true).

%% Param : [ TableName::string(), ClassName::string(), AsTypeName::string() ]
%% return : {ok, Result::boolean()}
test_table_class_load(TableName, ClassName, AsTypeName) ->
	do_request(?ACCUMULO_TEST_TABLE_CLASS_LOAD,[TableName, ClassName, AsTypeName],true).

%%================================================
%% new service since 1.6.0
%%================================================

%% Param : [ TableNames::set(TableName::string()) ]
%% return : {ok,Result::#diskUsage{}}
get_disk_usage(TableNames) ->
	do_request(?ACCUMULO_GET_DISK_USAGE, [TableNames], true).

%% Param : [ TableName::string(), Row::string(), Update::#conditionalUpdates]
%% return : {ok, Result::int32()}
update_row_conditionally(TableName, Row, Update) ->
	do_request(?ACCUMULO_UPDATE_ROW_CONDITIONALLY, [TableName, Row, Update], true).

%% Param : [TableName::string(), Options::#conditionalWriterOptions{}]
%% return : {ok, ConditionalWriter::string()}
create_conditional_writer(TableName, Options) ->
	do_request(?ACCUMULO_CREATE_CONDITIONAL_WRITER, [TableName, Options], true).

%% Param : [ ConditionalWriter::string(), Update::#conditionalUpdates]
%% return : {ok, Result::dict(Row::string, Update::int32())}
update_rows_conditionally(ConditionalWriter, Update) ->
	do_request(?ACCUMULO_UPDATE_ROWS_CONDITIONALLY, [ConditionalWriter, Update], false).

%% Param : [ConditionalWriter::string()]
%% return : {ok,ok}
close_conditional_writer(ConditionalWriter) ->
	do_request(?ACCUMULO_CLOSE_CONDITIONAL_WRITER, [ConditionalWriter], false).

%%================================================
%% utility methods
%%================================================
	
%%================================================
%% Internal methods
%%================================================
	
do_request(Function,Params,NeedLogin)->
	[Pool]=ets:lookup(?ACCUMULO_CLIENT_POOLS_ETS,random:uniform(ets:info(?ACCUMULO_CLIENT_POOLS_ETS,size))),

	try
		poolboy:transaction(Pool#accumulo_pool.pool,
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
	
