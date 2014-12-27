PROJECT       = secdb
C_SRC_OUTPUT  = priv/$(PROJECT)_format.so
C_SRC_EXCLUDE = $(CURDIR)/c_src/secdb-reader.cpp \
                $(CURDIR)/c_src/secdb_api.cpp

CC            = g++
CXXFLAGS      = -finline-functions -Wall -std=c++11 $(if $(release),-O3,-g -O0) 

include erlang.mk

ERLC_OPTS    += -DDEBUG

app:: bin/secdb-reader

C_BIN_OUTPUT  = bin bin/secdb

bin:
	mkdir -p $@

tar:
	@rm -f $(PROJECT).tbz
	tar jcf $(PROJECT).tbz --transform 's|^|$(PROJECT)/|' \
		--exclude-from=.gitignore \
		--exclude="autom4te.cache" \
		--exclude="core*" --exclude="*.*o" --exclude="*~" \
		--exclude=".deps" --exclude=".libs" \
		*

#------------------------------------------------------------------------------
# secdb-reader
#------------------------------------------------------------------------------
bin/secdb-reader: $(CURDIR)/c_src/secdb-reader.o $(CURDIR)/c_src/secdb_api.o bin
	$(link_verbose) $(CC) $(filter %.o,$^) $(filter-out -shared,$(LDFLAGS)) $(LDLIBS) -o $@

$(CURDIR)/c_src/secdb-reader.o: $(CURDIR)/c_src/secdb-reader.cpp \
                                  $(CURDIR)/c_src/secdb_api.cpp \
                                  $(CURDIR)/c_src/secdb_api.hpp \
                                  $(CURDIR)/c_src/secdb_api.h
	$(filter-out -fPIC,$(COMPILE_CPP)) $< -o $@

$(CURDIR)/c_src/secdb_api.o: $(CURDIR)/c_src/secdb_api.cpp $(CURDIR)/c_src/secdb_api.hpp
	$(filter-out -fPIC,$(COMPILE_CPP)) $< -o $@

$(CURDIR)/c_src/secdb-reader.cpp: $(CURDIR)/c_src/secdb_api.hpp

$(CURDIR)/c_src/secdb_api.cpp:    $(CURDIR)/c_src/secdb_api.hpp
$(CURDIR)/c_src/secdb_api.hpp:    $(CURDIR)/c_src/secdb_api.h

#all:
#	ERL_LIBS=apps:deps erl -make

#app:
#	@rebar compile

#clean:
#	@rebar clean
#	@rm -f erl_crash.dump

#test:
#	@rebar eunit

#.PHONY: test

PLT_NAME=.secdb_dialyzer.plt

$(PLT_NAME):
	@ERL_LIBS=deps dialyzer --build_plt --output_plt $@ \
		--apps kernel stdlib sasl crypto || true

#dialyze: $(PLT_NAME)
#	@dialyzer ebin  --plt $(PLT_NAME) --no_native \
#		-Werror_handling -Wunderspecs
