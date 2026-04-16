CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -Werror -O2
CFLAGS  += -D_FILE_OFFSET_BITS=64
LDFLAGS += -lm

PREFIX  ?= /usr/local
DESTDIR ?=

PROG = compbench

.PHONY: all clean install

all: $(PROG)

$(PROG): compbench.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

install: $(PROG)
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(PROG) $(DESTDIR)$(PREFIX)/bin/$(PROG)

clean:
	rm -f $(PROG)
