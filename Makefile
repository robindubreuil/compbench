CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -Werror -O2 -fstack-protector-strong
# DEFS is separate from CFLAGS so a command-line or environment CFLAGS
# override (make CFLAGS=..., dpkg-buildflags) cannot drop LFS support.
DEFS     = -D_FILE_OFFSET_BITS=64
LDLIBS  += -lm

PREFIX  ?= /usr/local
DESTDIR ?=

PROG = compbench
MAN  = $(PROG).1

.PHONY: all clean install uninstall test

all: $(PROG)

test: $(PROG)
	sudo bash test.sh

$(PROG): compbench.c
	$(CC) $(CFLAGS) $(DEFS) -o $@ $< $(LDFLAGS) $(LDLIBS)

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
