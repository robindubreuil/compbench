CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -Werror -O2
CFLAGS  += -D_FILE_OFFSET_BITS=64
LDFLAGS += -lm

PREFIX  ?= /usr/local
DESTDIR ?=

PROG = compbench
MAN  = $(PROG).1

.PHONY: all clean install uninstall

all: $(PROG)

$(PROG): compbench.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

install: $(PROG) $(MAN)
	install -d $(DESTDIR)$(PREFIX)/bin
	install -d $(DESTDIR)$(PREFIX)/share/man/man1
	install -m 755 $(PROG) $(DESTDIR)$(PREFIX)/bin/$(PROG)
	install -m 644 $(MAN) $(DESTDIR)$(PREFIX)/share/man/man1/$(MAN)

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(PROG)
	rm -f $(DESTDIR)$(PREFIX)/share/man/man1/$(MAN)

clean:
	rm -f $(PROG)
