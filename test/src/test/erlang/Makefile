
## GNU Make syntax, sorry.

ERLC ?= erlc
sources := $(wildcard src/*.erl)
objects := $(patsubst src/%,ebin/%,$(patsubst %.erl,%.beam,$(sources)))

all:
	@echo "Please use the './Build.sh' shell script"
	@echo "Usage: './Build.sh eqc' or ''./Build.sh proper'"
	exit 1

ebin: $(objects)

clean:
	rm -f $(objects)

######

ebin/%.beam : src/%.erl $(wildcard include/*.hrl)
	$(ERLC) -o ebin $(EFLAGS) $(QC_EFLAGS) $<
