-include("accumulo_proxy/proxy_types.hrl").

-define(THRIFT_MODULE,accumuloProxy_thrift).

-define(SYSTEM_TABLES,[<<"!METADATA">>,<<"trace">>]).
-define(SYSTEM_ACCOUNTS,[<<"root">>]).

%% errors
-define(NOT_CONNECTED,{error,not_connected}).
-define(LOGIN_FAILED,{error, login_failed}).

-define(ACCUMULO_TIME_TYPE_LOGICAL,0).
-define(ACCUMULO_TIME_TYPE_MILLIS,1).

%-define(UNICODE_LAST, <<"\357\277\277">>).

-define(ACCUMULO_CLIENT_POOLS_ETS,accumulo_client_pools).

%% AccumuloProxy services

%%--------- Table  ------------
-define(ACCUMULO_LIST_TABLES,'listTables').
-define(ACCUMULO_TABLE_EXISTS,'tableExists').
-define(ACCUMULO_TABLE_ID_MAP,'tableIdMap').

-define(ACCUMULO_CREATE_TABLE,'createTable').
-define(ACCUMULO_DELETE_TABLE,'deleteTable').
-define(ACCUMULO_RENAME_TABLE,'renameTable').

-define(ACCUMULO_CLONE_TABLE,'cloneTable').
-define(ACCUMULO_EXPORT_TABLE,'exportTable').
-define(ACCUMULO_IMPORT_TABLE,'importTable').
-define(ACCUMULO_IMPORT_DIRECTORY,'importDirectory').

-define(ACCUMULO_FLUSH_TABLE,'flushTable').

-define(ACCUMULO_COMPACT_TABLE,'compactTable').
-define(ACCUMULO_CANCEL_COMPACTION,'cancelCompaction').
-define(ACCUMULO_GET_ACTIVE_COMPACTIONS,'getActiveCompactions').

-define(ACCUMULO_GET_LOCALITY_GROUPS,'getLocalityGroups').
-define(ACCUMULO_SET_LOCALITY_GROUPS,'setLocalityGroups').

-define(ACCUMULO_DELETE_ROWS,'deleteRows').
-define(ACCUMULO_GET_MAX_ROW,'getMaxRow').

-define(ACCUMULO_GET_TABLE_PROPERTIES,'getTableProperties').
-define(ACCUMULO_SET_TABLE_PROPERTY,'setTableProperty').
-define(ACCUMULO_REMOVE_TABLE_PROPERTY,'removeTableProperty').

-define(ACCUMULO_HAS_TABLE_PERMISSION,'hasTablePermission').
-define(ACCUMULO_GRANT_TABLE_PERMISSION,'grantTablePermission').
-define(ACCUMULO_REVOKE_TABLE_PERMISSION,'revokeTablePermission').

-define(ACCUMULO_MERGE_TABLETS,'mergeTablets').
-define(ACCUMULO_OFFLINE_TABLE,'offlineTable').
-define(ACCUMULO_ONLINE_TABLE,'onlineTable').


-define(ACCUMULO_LIST_CONSTRAINTS,'listConstraints').
-define(ACCUMULO_ADD_CONSTRAINT,'addConstraint').
-define(ACCUMULO_REMOVE_CONSTRAINT,'removeConstraint').

-define(ACCUMULO_LIST_SPLITS,'listSplits').
-define(ACCUMULO_ADD_SPLITS,'addSplits').
-define(ACCUMULO_SPLIT_RANGE_BY_TABLETS,'splitRangeByTablets').

-define(ACCUMULO_LIST_ITERATORS,'listIterators').
-define(ACCUMULO_GET_ITERATOR_SETTING,'getIteratorSetting').
-define(ACCUMULO_CHECK_ITERATOR_CONFLICTS,'checkIteratorConflicts').
-define(ACCUMULO_ATTACH_ITERATOR,'attachIterator').
-define(ACCUMULO_REMOVE_ITERATOR,'removeIterator').

%%--------- Scanner ------------
-define(ACCUMULO_GET_ACTIVE_SCANS,'getActiveScans').
-define(ACCUMULO_CREATE_BATCH_SCANNER,'createBatchScanner').
-define(ACCUMULO_CREATE_SCANNER,'createScanner').
%% don't need Login
-define(ACCUMULO_HAS_NEXT,'hasNext').
%% don't need Login
-define(ACCUMULO_NEXT_ENTRY,'nextEntry').
%% don't need Login
-define(ACCUMULO_NEXT_K,'nextK').
%% don't need Login
-define(ACCUMULO_CLOSE_SCANNER,'closeScanner').

%%--------- Writer ------------
-define(ACCUMULO_UPDATE_AND_FLUSH,'updateAndFlush').
-define(ACCUMULO_CREATE_WRITER,'createWriter').
%% don't need Login
-define(ACCUMULO_UPDATE,'update').
%% don't need Login
-define(ACCUMULO_FLUSH,'flush').
%% don't need Login
-define(ACCUMULO_CLOSE_WRITER,'closeWriter').

%% don't need Login
-define(ACCUMULO_GET_ROW_RANGE,'getRowRange').
%% don't need Login
-define(ACCUMULO_GET_FOLLOWING,'getFollowing').

%%--------- Authentication & Account ------------
-define(ACCUMULO_LOGIN,'login').

-define(ACCUMULO_LIST_LOCAL_USERS,'listLocalUsers').
-define(ACCUMULO_CREATE_LOCAL_USER,'createLocalUser').
-define(ACCUMULO_DROP_LOCAL_USER,'dropLocalUser').
-define(ACCUMULO_CHANGE_LOCAL_USER_PASSWORD,'changeLocalUserPassword').

-define(ACCUMULO_GET_USER_AUTHORIZATIONS,'getUserAuthorizations').
-define(ACCUMULO_AUTHENTICATE_USER,'authenticateUser').

-define(ACCUMULO_CHANGE_USER_AUTHORIZATIONS,'changeUserAuthorizations').


%%--------- System & Configurations------------
-define(ACCUMULO_CLEAR_LOCATOR_CACHE,'clearLocatorCache').

-define(ACCUMULO_GET_SITE_CONFIGURATION,'getSiteConfiguration').
-define(ACCUMULO_GET_SYSTEM_CONFIGURATION,'getSystemConfiguration').

-define(ACCUMULO_GET_TABLET_SERVERS,'getTabletServers').

-define(ACCUMULO_REMOVE_PROPERTY,'removeProperty').
-define(ACCUMULO_SET_PROPERTY,'setProperty').

-define(ACCUMULO_GRANT_SYSTEM_PERMISSION,'grantSystemPermission').
-define(ACCUMULO_HAS_SYSTEM_PERMISSION,'hasSystemPermission').
-define(ACCUMULO_REVOKE_SYSTEM_PERMISSION,'revokeSystemPermission').

-define(ACCUMULO_TEST_TABLE_CLASS_LOAD,'testTableClassLoad').
-define(ACCUMULO_PING_TABLET_SERVER,'pingTabletServer').
-define(ACCUMULO_TEST_CLASS_LOAD,'testClassLoad').

%% --- 1.6.0 ---
-define(ACCUMULO_GET_DISK_USAGE, 'getDiskUsage').

-define(ACCUMULO_UPDATE_ROW_CONDITIONALLY,'updateRowConditionally').

-define(ACCUMULO_CREATE_CONDITIONAL_WRITER,'createConditionalWriter').
-define(ACCUMULO_UPDATE_ROWS_CONDITIONALLY,'updateRowsConditionally').
-define(ACCUMULO_CLOSE_CONDITIONAL_WRITER,'closeConditionalWriter').
