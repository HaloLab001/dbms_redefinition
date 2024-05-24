# contrib/dbms_redefition/Makefile

MODULE_big	= dbms_redefinition
OBJS = \
	$(WIN32RES) \
	pgut-spi.o \
	libred.o \
	dbms_redefinition.o

EXTENSION = dbms_redefinition
DATA = dbms_redefinition--1.0.sql
PGFILEDESC = "dbms_redefinition - functions for DO ONLINE modify Halo/PostgreSQL table (avoiding long locks)"

REGRESS = check check_btree check_heap

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dbms_redefinition
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif