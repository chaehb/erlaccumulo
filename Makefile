REBAR = ./rebar

all: deps
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean
	rm -rf ebin
dist-clean: clean
	rm -rf deps
	