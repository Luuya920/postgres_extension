# Makefile
EXTENSION = my_extension
MODULE_big = my_extension
OBJS = my_extension.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)