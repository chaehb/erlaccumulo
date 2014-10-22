%%%-------------------------------------------------------------------
%%% @author chaehb <chaehb@gmail.com>
%%% @copyright (C) 2014, <Data Science Factory Ltd>
%%% @doc
%%%	
%%% @end
%%% Created : 14. Sep 2014 00:41
%%%-------------------------------------------------------------------
-module(accumulo_instance).

-include("accumulo_proxy/proxy_types.hrl").
-include("erlaccumulo.hrl").

-define(WORKER, accumulo_instance_worker).

-export([
	get_active_compactions/1, get_active_scans/1,
	get_site_configuration/0, get_system_configuration/0, get_tablet_servers/0,
	ping_tablet_server/1,
	set_property/2, remove_property/1,
	test_class_load/2
]).

%% Params : TabletServerAddress::binary()
%% return : {ok, [#activeCompaction{}]}
get_active_compactions(TabletServerAddress) ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_ACTIVE_COMPACTIONS,[TabletServerAddress]}).

%% Params : TabletServerAddress::binary()
%% return : {ok, ActiveScans::[#activeScan{}]}
get_active_scans(TabletServerAddress) ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_ACTIVE_SCANS,[TabletServerAddress]}).

%% return : {ok, dict(SiteConfiguration::binary(), Value::binary())}
get_site_configuration() ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_SITE_CONFIGURATION}).

%% return : {ok, dict(SystemConfiguration::binary(), Value::binary())}
get_system_configuration() ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_SYSTEM_CONFIGURATION}).

%% return : {ok, [TabletServer::binary()]}
get_tablet_servers() ->
	gen_server:call(?WORKER,{?ACCUMULO_GET_TABLET_SERVERS}).

%% Params : TabletServerAddress::binary()
%% return : {ok, pong}
ping_tablet_server(TabletServerAddress) ->
	gen_server:call(?WORKER,{?ACCUMULO_PING_TABLET_SERVER,[TabletServerAddress]}).
	
%% Params : [ Property::binary(), Value::binary()]
set_property(Property, Value) ->
	gen_server:call(?WORKER,{?ACCUMULO_SET_PROPERTY,[Property, Value]}).

%% Params : [ Property::binary()]
remove_property(Property) ->
	gen_server:call(?WORKER,{?ACCUMULO_REMOVE_PROPERTY,[Property]}).

%% Params : [ ClassName::binary(), AsTypeName::binary() ]
%% return : {ok, true | false }
test_class_load(ClassName, AsTypeName) ->
	gen_server:call(?WORKER,{?ACCUMULO_TEST_CLASS_LOAD,[ClassName, AsTypeName]}).
