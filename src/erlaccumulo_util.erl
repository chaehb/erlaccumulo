-module(erlaccumulo_util).

-include("erlaccumulo.hrl").

-export([
	create_rows/3
]).
	
create_rows(RowId,Cells,Dict) ->
	dict:store(RowId,Cells,Dict).
