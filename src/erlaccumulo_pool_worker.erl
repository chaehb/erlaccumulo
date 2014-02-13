-module(erlaccumulo_pool_worker).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-include("erlaccumulo.hrl").
-include("accumulo_proxy/accumuloProxy_thrift.hrl").

-define(SERVER, ?MODULE).

-record(state,{
	conn,
	login
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).
-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_link(Args) ->
	gen_server:start_link(?MODULE,Args,[]).
	
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
	Host = proplists:get_value(host,Args),
	Port = proplists:get_value(port,Args),
	User = proplists:get_value(user,Args),
	Passwd = proplists:get_value(password, Args),
	Dict = dict:new(),
	Cred = dict:store("password",Passwd,Dict),
	
	case connect(Host,Port) of
		{{ok,_},State} ->
			try thrift_client:call(State#state.conn,?ACCUMULO_LOGIN,[User,Cred]) of
				{Conn,{ok,Login}} ->
					NewState=#state{conn=Conn,login=Login},
					{ok,NewState}
			catch
				_throw ->
					NewState = #state{},
					{ok,NewState}
			end;		
		{_,State} ->
			{ok,State}
	end.

handle_call({?ACCUMULO_LOGIN,[User,Cred]},_From, State) ->
	try thrift_client:call(State#state.conn,?ACCUMULO_LOGIN,[User,Cred]) of
		{Conn,{ok,Login}} ->
			State=#state{conn=Conn,login=Login},
			{reply,{ok,login},State}
	catch
		_throw ->
			State = #state{},
			{reply,?LOGIN_FAILED,State}
	end;		

handle_call({Function,Params,true}, _From, State) ->
	try thrift_client:call(State#state.conn,Function,[State#state.login]++Params) of
		{Conn,{ok,Response}} ->
			{reply,{ok,Response},State#state{conn=Conn}};
		{Conn,{_,{Response,_,_}}} ->
			{reply,{error,Response},State#state{conn=Conn}}
	catch
		_:{_,{_,Reason}} ->
			{reply,{exception,Reason},State}
	end;

handle_call({Function,Params,false}, _From, State) ->
	try thrift_client:call(State#state.conn,Function,Params) of
		{Conn,{ok,Response}} ->
			{reply,{ok,Response},State#state{conn=Conn}};
		{Conn,{_,{Response,_,_}}} ->
			{reply,{error,Response},State#state{conn=Conn}}
	catch
		_:{_,{_,Reason}} ->
			{reply,{exception,Reason},State}
	end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
	%% some terminate codes here
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
connect(Host,Port) ->
	try thrift_client_util:new(Host,Port,?THRIFT_MODULE,[{framed,true}]) of
		{ok, Conn0} ->
			State= #state{conn=Conn0},
			{{ok,connected},State}
	catch
		_throw ->
			State= #state{},
			{ ?NOT_CONNECTED, State }
	end.
