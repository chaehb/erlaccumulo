%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%		not implement 'testClassLoad'
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(accumulo_table).

-include("accumulo_proxy/proxy_types.hrl").
-include("erlaccumulo.hrl").

-define(WORKER, accumulo_table_worker).

-export([
	list_tables/0, table_exists/1,table_id_map/0, 
	create_table/3, delete_table/1, rename_table/2,
	clone_table/5, flush_table/4, export_table/2, import_table/2, import_directory/4,
	compact_table/6, cancel_compaction/1,
	get_locality_groups/1, set_locality_groups/2,
	delete_rows/3, get_max_row/6,
	get_table_properties/1, set_table_property/3, remove_table_property/2,
	merge_tablets/3,
	offline_table/2, online_table/2,
	list_constraints/1, add_constraint/2, remove_constraint/2,
	list_splits/2, add_splits/2, split_range_by_tablets/3,
	list_iterators/1, get_iterator_setting/3, check_iterator_conflicts/3, attach_iterator/3, remove_iterator/3,
	clear_locator_cache/1, 
	get_disk_usage/1,
	test_table_class_load/3
]).

%% return : {ok,set(binary())}	
list_tables() ->
	gen_server:call(?WORKER,{?ACCUMULO_LIST_TABLES}).
	
%% Params : TableName::binary()
%% return : {ok, true | false}	
table_exists(TableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_TABLE_EXISTS,[TableName]}).

%% return {ok,dict(binary(),binary())}
table_id_map() ->
	gen_server:call(?WORKER,{?ACCUMULO_TABLE_ID_MAP}).
	
%% Params : TableName::binary(),Versioning::boolean(),TimeType::int32()
create_table(TableName, Versioning, TimeType) ->
	gen_server:call(?WORKER,{?ACCUMULO_CREATE_TABLE,[TableName,Versioning,TimeType]}).

%% Params : TableName::binary()
delete_table(TableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_DELETE_TABLE,[TableName]}).

%% Params : TableName::binary(), NewTableName::binary()
rename_table(TableName, NewTableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_RENAME_TABLE,[TableName, NewTableName]}).

%% Params : 
%%		TableName::binary(), NewTableName::binary(),
%%		Flush::boolean(),PropertiesToSet::dict(binary(),binary()),PropertiesToExclude::set(binary())
clone_table(TableName,NewTableName,Flush,PropertiesToSet,PropertiesToExclude) ->
	gen_server:call(?WORKER,{?ACCUMULO_CLONE_TABLE,[TableName,NewTableName,Flush,PropertiesToSet,PropertiesToExclude]}).

%% Params : TableName::binary(), StartRow::binary()|undefined, EndRow::binary()|undefined, Wait::boolean()
flush_table(TableName, StartRow, EndRow, Wait) ->
	gen_server:call(?WORKER,{?ACCUMULO_FLUSH_TABLE,[TableName, StartRow, EndRow, Wait]}).

%% Params : TableName::binary(),ExportDirectory::binary()
export_table(TableName,ExportDirectory) ->
	gen_server:call(?WORKER,{?ACCUMULO_EXPORT_TABLE,[TableName, ExportDirectory]}).

%% Params : TableName::binary(),ImportDirectory::binary()
import_table(TableName,ImportDirectory) ->
	gen_server:call(?WORKER,{?ACCUMULO_IMPORT_TABLE,[TableName,ImportDirectory]}).

%% Params : TableName::binary(),ImportDirectory::binary(),FailureDirectory::binary(),SetTime::boolean()
import_directory(TableName,ImportDirectory,FailureDirectory,SetTime) ->
	gen_server:call(?WORKER,{?ACCUMULO_IMPORT_DIRECTORY,[TableName,ImportDirectory,FailureDirectory,SetTime]}).

%% Params : 
%%		TableName::binary(), 
%%		StartRow::binary()|undefined, EndRow::binary()|undefined, 
%%		Iterators::[#iteratorSetting{}], 
%%		Flush::boolean(), Wait::boolean()
compact_table(TableName, StartRow, EndRow, Iterators, Flush, Wait) ->
	gen_server:call(?WORKER,{?ACCUMULO_COMPACT_TABLE,[TableName, StartRow, EndRow, Iterators, Flush, Wait]}).

%% Params : TableName::binary()
cancel_compaction(TableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_CANCEL_COMPACTION,[TableName]}).

%% Params : TableName::binary()
%% return : {ok, dict(binary(),set(binary()))}
get_locality_groups(TableName)->
	gen_server:call(?WORKER,{?ACCUMULO_GET_LOCALITY_GROUPS,[TableName]}).

%% Params : TableName::binary(),LocalityGroups::dict(binary(),set(binary()))
set_locality_groups(TableName,LocalityGroups)->
	gen_server:call(?WORKER,{?ACCUMULO_SET_LOCALITY_GROUPS,[TableName,LocalityGroups]}).

%% Params : TableName::binary(), StartRow::binary()|undefined, EndRow::binary()|undefined
delete_rows(TableName,StartRow,EndRow)->
	gen_server:call(?WORKER,{?ACCUMULO_DELETE_ROWS,[TableName,StartRow,EndRow]}).

%% Params : 
%%		TableName::binary(), 
%%		Auths::set(binary()), 
%%		StartRow::binary(), StartInclusive::boolean(), 
%%		EndRow::binary(), EndInclusive::boolean()
%% return : {ok, RowId::binary()}
get_max_row(TableName, Auths, StartRow, StartInclusive, EndRow, EndInclusive)->
	gen_server:call(?WORKER,{?ACCUMULO_GET_MAX_ROW,[TableName, Auths, StartRow, StartInclusive, EndRow, EndInclusive]}).

%% Params : TableName::binary()
%% return : {ok, dict(binary(), binary())}
get_table_properties(TableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_TABLE_PROPERTIES,[TableName]}).

%% Params : TableName::binary(), Property::binary(), Value::binary()
set_table_property(TableName,Property, Value) ->
	gen_server:call(?WORKER,{?ACCUMULO_SET_TABLE_PROPERTY,[TableName,Property, Value]}).

%% Params : TableName::binary(), Property::binary()
remove_table_property(TableName, Property) ->
	gen_server:call(?WORKER,{?ACCUMULO_REMOVE_TABLE_PROPERTY,[TableName,Property]}).

%% Params : TableName::binary(), StartRow::binary(), EndRow::binary()
merge_tablets(TableName,StartRow,EndRow) ->
	gen_server:call(?WORKER,{?ACCUMULO_MERGE_TABLETS,[TableName,StartRow,EndRow]}).

%% Params : TableName::binary(), Wait::boolean()
offline_table(TableName,Wait) ->
	gen_server:call(?WORKER,{?ACCUMULO_OFFLINE_TABLE,[TableName,Wait]}).

%% Params : TableName::binary(), Wait::boolean()
online_table(TableName,Wait) ->
	gen_server:call(?WORKER,{?ACCUMULO_ONLINE_TABLE,[TableName,Wait]}).

%% Params : TableName::binary()
%% return : {ok,dict(binary(),int32())}
list_constraints(TableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_LIST_CONSTRAINTS,[TableName]}).

%% Params : TableName::binary(),ConstraintClassName::binary()
%% return : {ok, int32()}
add_constraint(TableName,ConstraintClassName) ->
	gen_server:call(?WORKER,{?ACCUMULO_ADD_CONSTRAINT,[TableName,ConstraintClassName]}).

%% Params : TableName::binary(),Constraint::int32()
remove_constraint(TableName,Constraint) ->
	gen_server:call(?WORKER,{?ACCUMULO_REMOVE_CONSTRAINT,[TableName,Constraint]}).

%% Params : TableName::binary(),MaxSplits::int32()
%% return : {ok, [binary()}
list_splits(TableName,MaxSplits) ->
	gen_server:call(?WORKER,{?ACCUMULO_LIST_SPLITS,[TableName,MaxSplits]}).

%% Params : [ TableName::binary(), Splits::set(binary()) ]
add_splits(TableName,Splits) ->
	gen_server:call(?WORKER,{?ACCUMULO_ADD_SPLITS,[TableName,Splits]}).

%% Params : TableName::binary(), Range::#range{}, MaxSplits::int32()
%% return : {ok, set(#range{})}
split_range_by_tablets(TableName,Range,MaxSplits) ->
	gen_server:call(?WORKER,{?ACCUMULO_SPLIT_RANGE_BY_TABLETS,[TableName,Range,MaxSplits]}).

%% Params :: TableName::binary()
%% return : {ok, dict(IteratorName::binary(), Scopes::set(Scope::int32()))}
list_iterators(TableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_LIST_ITERATORS,[TableName]}).

%% Params : TableName::binary(), IteratorName::binary(), Scope::int32()
%% return : {ok, #iteratorSetting{}}
get_iterator_setting(TableName,IteratorName, Scopes) ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_ITERATOR_SETTING,[TableName,IteratorName, Scopes]}).

%% Params : TableName::binary(), IteratorSetting::#iteratorSetting{}, Scopes::set(Scope::int32()) 
check_iterator_conflicts(TableName,IteratorSetting, Scopes) ->
	gen_server:call(?WORKER,{?ACCUMULO_CHECK_ITERATOR_CONFLICTS,[TableName,IteratorSetting, Scopes]}).

%% Params : TableName::binary(), IteratorSetting::#iteratorSetting{}, Scopes::set(Scope::int32()) 
attach_iterator(TableName,IteratorSetting, Scopes) ->
	gen_server:call(?WORKER,{?ACCUMULO_ATTACH_ITERATOR,[TableName,IteratorSetting, Scopes]}).

%% Params : TableName::binary(), IteratorName::binary(), Scopes::set(Scope::int32()) 
remove_iterator(TableName,IteratorName, Scopes) ->
	gen_server:call(?WORKER,{?ACCUMULO_REMOVE_ITERATOR,[TableName,IteratorName, Scopes]}).

%% Params : TableName::binary()
clear_locator_cache(TableName) ->
	gen_server:call(?WORKER,{?ACCUMULO_CLEAR_LOCATOR_CACHE,[TableName]}).

%% Params : [TableName::binary()]
%% return : {ok, #diskUsage{}}
get_disk_usage(TableNames) ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_DISK_USAGE,[TableNames]}).

%% Params : TableName::binary(), ClassName::binary(), AsTypeName::binary()
%% return : {ok, true | false }
test_table_class_load(TableName, ClassName, AsTypeName) ->
	gen_server:call(?WORKER,{?ACCUMULO_TEST_TABLE_CLASS_LOAD,[TableName, ClassName, AsTypeName]}).
