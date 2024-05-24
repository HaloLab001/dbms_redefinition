#ifndef LIBRED_H
#define LIBRED_H

#include "pgut-be.h"

#define LIBRARY_VERSION "1.0"

/*
 * Parsed CREATE INDEX statement. You can rebuild sql using
 * sprintf(buf, "%s %s ON %s USING %s (%s)%s",
 *		create, index, table type, columns, options)
 */
typedef struct IndexDef
{
	char *create;	/* CREATE INDEX or CREATE UNIQUE INDEX */
	char *index;	/* index name including schema */
	char *table;	/* table name including schema */
	char *type;		/* btree, hash, gist or gin */
	char *columns;	/* column definition */
	char *options;	/* options after columns, before TABLESPACE (e.g. COLLATE) */
	char *tablespace; /* tablespace if specified */
	char *where;	/* WHERE content if specified */
} IndexDef;

extern Datum PGUT_EXPORT libred_version(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_trigger(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_apply(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_get_order_by(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_indexdef(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_swap(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_drop(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_disable_autovacuum(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_reset_autovacuum(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_index_swap(PG_FUNCTION_ARGS);
extern Datum PGUT_EXPORT libred_get_table_and_inheritors(PG_FUNCTION_ARGS);


extern char *skip_const_bysql(const char *original_sql, char *sql, const char *arg1, const char *arg2);
extern char *skip_const(Oid index, char *sql, const char *arg1, const char *arg2);
extern char *skip_ident_bysql(const char *original_sql, char *sql);
extern char *skip_ident(Oid index, char *sql);
extern char *parse_error_bysql(const char *original_sql);
extern char *parse_error(Oid index);
extern char *skip_until_const_bysql(const char *original_sql, char *sql, const char *what);
extern char *skip_until_const(Oid index, char *sql, const char *what);
extern char *skip_until_bysql(const char *original_sql, char *sql, char end);
extern char *skip_until(Oid index, char *sql, char end);

#endif		/* LIBRED_H */
