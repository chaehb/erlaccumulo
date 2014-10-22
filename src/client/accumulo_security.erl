%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%		
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(accumulo_security).

-include("accumulo_proxy/proxy_types.hrl").
-include("erlaccumulo.hrl").

-define(WORKER, accumulo_security_worker).

-export([
	authenticate_user/2, change_local_user_password/2, change_user_authorizations/2,
	create_local_user/2, drop_local_user/1, get_user_authorizations/1,
	grant_system_permission/2, grant_table_permission/3,
	has_system_permission/2, has_table_permission/3,
	revoke_system_permission/2, revoke_table_permission/3,
	list_local_users/0
]).

%% Params : User::binary(),Properties::dict(Key::binary(),Value::binary())
%% return : {ok, Authenticate::boolean()}
authenticate_user(User, Properties) ->
	gen_server:call(?WORKER,{?ACCUMULO_AUTHENTICATE_USER,[User, Properties]}).

%% Params : User::binary(),Password::binary()
change_local_user_password(User,Password) ->
	gen_server:call(?WORKER,{?ACCUMULO_CHANGE_LOCAL_USER_PASSWORD,[User,Password]}).

%% Params : User::binary(),Authorizations::set(Authorization::binary())
change_user_authorizations(User, Authorizations) ->
	gen_server:call(?WORKER,{?ACCUMULO_CHANGE_USER_AUTHORIZATIONS,[User, Authorizations]}).

%% Params : User::binary(),Password::binary()
create_local_user(User,Password) ->
	gen_server:call(?WORKER,{?ACCUMULO_CREATE_LOCAL_USER,[User,Password]}).

%% Params : User::binary()
drop_local_user(User) ->
	gen_server:call(?WORKER,{?ACCUMULO_DROP_LOCAL_USER,[User]}).

%% Params : User::binary()
%% return : {ok, Authorizations::List(Authorization::binary())}
get_user_authorizations(User) ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_USER_AUTHORIZATIONS,[User]}).

%% Params : User::binary(), Permission::int32()
grant_system_permission(User,Permission) ->
	gen_server:call(?WORKER,{?ACCUMULO_GRANT_SYSTEM_PERMISSION,[User,Permission]}).

%% Params : User::binary(),TableName::binary(), Permission::int32()
grant_table_permission(User,TableName, Permission) ->
	gen_server:call(?WORKER,{?ACCUMULO_GRANT_TABLE_PERMISSION,[User,TableName,Permission]}).

%% Params : User::binary(), Permission::int32()
%% return : {ok, boolean()}
has_system_permission(User,Permission) ->
	gen_server:call(?WORKER,{?ACCUMULO_HAS_SYSTEM_PERMISSION,[User,Permission]}).

%% Params : User::binary(),TableName::binary(), Permission::int32()
%% return : {ok, boolean()}
has_table_permission(User,TableName, Permission) ->
	gen_server:call(?WORKER,{?ACCUMULO_HAS_TABLE_PERMISSION,[User,TableName,Permission]}).

%% Params : User::binary(), Permission::int32()
revoke_system_permission(User,Permission) ->
	gen_server:call(?WORKER,{?ACCUMULO_REVOKE_SYSTEM_PERMISSION,[User,Permission]}).

%% Params : User::binary(),TableName::binary(), Permission::int32()
revoke_table_permission(User,TableName, Permission) ->
	gen_server:call(?WORKER,{?ACCUMULO_REVOKE_TABLE_PERMISSION,[User,TableName,Permission]}).

%% return : {ok, Users::set(User::binary())}
list_local_users() ->
	gen_server:call(?WORKER,{?ACCUMULO_LIST_LOCAL_USERS}).
