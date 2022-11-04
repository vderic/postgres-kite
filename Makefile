# contrib/kite_fdw/Makefile

MODULE_big = kite_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	deparse.o \
	option.o \
	kite_fdw.o \
	shippable.o \
	decimal.o \
	json.o \
	decode.o \
	op.o \
	numeric.o \
	dec.o \
	exx_int.o \
	agg.o 

all: ext $(OBJS)

ext:
	make -C ext

PGFILEDESC = "postgres_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir) -Iext/include -DKITE_CONNECT
SHLIB_LINK_INTERNAL = $(libpq) -Lext/lib -lkitesdk -levent -lxrg -lsproto -lhop -larrow -larrow_bundled_dependencies -llz4 -lstdc++ -std=c++17

EXTENSION = kite_fdw
DATA = kite_fdw--1.0.sql kite_fdw--1.0--1.1.sql

REGRESS = kite_fdw

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/kite_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: ext
