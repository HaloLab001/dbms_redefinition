/*
 * dbms_redefinition: libred.c
 *
 * 版权所有 (c) 2019-2024, 易景科技保留所有权利。
 * Copyright (c) 2019-2024, Halo Tech Co.,Ltd. All rights reserved.
 * 
 * 易景科技是Halo Database、Halo Database Management System、羲和数据
 * 库、羲和数据库管理系统（后面简称 Halo ）软件的发明人同时也为知识产权权
 * 利人。Halo 软件的知识产权，以及与本软件相关的所有信息内容（包括但不限
 * 于文字、图片、音频、视频、图表、界面设计、版面框架、有关数据或电子文档等）
 * 均受中华人民共和国法律法规和相应的国际条约保护，易景科技享有上述知识产
 * 权，但相关权利人依照法律规定应享有的权利除外。未免疑义，本条所指的“知识
 * 产权”是指任何及所有基于 Halo 软件产生的：（a）版权、商标、商号、域名、与
 * 商标和商号相关的商誉、设计和专利；与创新、技术诀窍、商业秘密、保密技术、非
 * 技术信息相关的权利；（b）人身权、掩模作品权、署名权和发表权；以及（c）在
 * 本协议生效之前已存在或此后出现在世界任何地方的其他工业产权、专有权、与“知
 * 识产权”相关的权利，以及上述权利的所有续期和延长，无论此类权利是否已在相
 * 关法域内的相关机构注册。
 *
 * This software and related documentation are provided under a license
 * agreement containing restrictions on use and disclosure and are 
 * protected by intellectual property laws. Except as expressly permitted
 * in your license agreement or allowed by law, you may not use, copy, 
 * reproduce, translate, broadcast, modify, license, transmit, distribute,
 * exhibit, perform, publish, or display any part, in any form, or by any
 * means. Reverse engineering, disassembly, or decompilation of this 
 * software, unless required by law for interoperability, is prohibited.
 * 
 * This software is developed for general use in a variety of
 * information management applications. It is not developed or intended
 * for use in any inherently dangerous applications, including applications
 * that may create a risk of personal injury. If you use this software or
 * in dangerous applications, then you shall be responsible to take all
 * appropriate fail-safe, backup, redundancy, and other measures to ensure
 * its safe use. Halo Corporation and its affiliates disclaim any 
 * liability for any damages caused by use of this software in dangerous
 * applications.
 *
 * Portions Copyright (c) 2008-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2011, Itagaki Takahiro
 * Portions Copyright (c) 2012-2020, The Reorg Development Team
 */
#include "postgres.h"

#include <unistd.h>

#include "access/genam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"

/*
 * heap_open/heap_close was moved to table_open/table_close in 12.0
 */
#include "access/table.h"


/*
 * utils/rel.h no longer includes pg_am.h as of 9.6, so need to include
 * it explicitly.
 */
#include "catalog/pg_am.h"

/*
 * catalog/pg_foo_fn.h headers was merged back into pg_foo.h headers
 */
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "libred.h"
#include "pgut-spi.h"
#include "pgut-be.h"

#include "access/htup_details.h"
#include "utils/ruleutils.h"



PG_FUNCTION_INFO_V1(libred_version);
PG_FUNCTION_INFO_V1(libred_trigger);
PG_FUNCTION_INFO_V1(libred_apply);
PG_FUNCTION_INFO_V1(libred_get_order_by);
PG_FUNCTION_INFO_V1(libred_indexdef);
PG_FUNCTION_INFO_V1(libred_swap);
PG_FUNCTION_INFO_V1(libred_drop);
PG_FUNCTION_INFO_V1(libred_disable_autovacuum);
PG_FUNCTION_INFO_V1(libred_reset_autovacuum);
PG_FUNCTION_INFO_V1(libred_index_swap);
PG_FUNCTION_INFO_V1(libred_get_table_and_inheritors);

static void	libred_init(void);
static SPIPlanPtr libred_prepare(const char *src, int nargs, Oid *argtypes);
static const char *get_quoted_relname(Oid oid);
static const char *get_quoted_nspname(Oid oid);
static void swap_heap_or_index_files(Oid r1, Oid r2);

#define copy_tuple(tuple, desc) \
	PointerGetDatum(SPI_returntuple((tuple), (desc)))

#define IsToken(c) \
	(IS_HIGHBIT_SET((c)) || isalnum((unsigned char) (c)) || (c) == '_')

/* convert text pointer to C string */
#define TEXT_TO_CSTRING(textp) \
    DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

/* check access authority */
static void
must_be_superuser(const char *func)
{
	if (!superuser())
		elog(ERROR, "must be superuser to use %s function", func);
}


/* The API of RenameRelationInternal() was changed in 9.2.
 * Use the RENAME_REL macro for compatibility across versions.
 */
#define RENAME_REL(relid, newrelname) RenameRelationInternal(relid, newrelname, true, false);

/*
 * is_index flag was added in 12.0, prefer separate macro for relation and index
 */
#define RENAME_INDEX(relid, newrelname) RenameRelationInternal(relid, newrelname, true, true);


Datum
libred_version(PG_FUNCTION_ARGS)
{
	return CStringGetTextDatum("dbms_redefinition lib version" LIBRARY_VERSION);
}

/**
 * @fn      Datum libred_trigger(PG_FUNCTION_ARGS)
 * @brief   Insert a operation log into log-table.
 *
 * libred_trigger(column1, ..., columnN)
 *
 * @param	column1		A column of the table in primary key/unique index.
 * ...
 * @param	columnN		A column of the table in primary key/unique index.
 */
Datum
libred_trigger(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = (TriggerData *) fcinfo->context;
	TupleDesc		desc;
	HeapTuple		tuple;
	Datum			values[2];
	bool			nulls[2] = { 0, 0 };
	Oid				argtypes[2];
	Oid				relid;
	StringInfo		sql;

	/* authority check */
	must_be_superuser("libred_trigger");

	/* make sure it's called as a trigger at all */
	if (!CALLED_AS_TRIGGER(fcinfo) ||
		!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event) ||
		trigdata->tg_trigger->tgnargs < 1)
		elog(ERROR, "libred_trigger: invalid trigger call");

	relid = RelationGetRelid(trigdata->tg_relation);

	/* retrieve parameters */
	desc = RelationGetDescr(trigdata->tg_relation);
	argtypes[0] = argtypes[1] = trigdata->tg_relation->rd_rel->reltype;

	/* connect to SPI manager */
	libred_init();

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		/* INSERT: (NULL, newtup) */
		tuple = trigdata->tg_trigtuple;
		nulls[0] = true;
		values[1] = copy_tuple(tuple, desc);
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		/* DELETE: (oldtup, NULL) */
		tuple = trigdata->tg_trigtuple;
		values[0] = copy_tuple(tuple, desc);
		nulls[1] = true;
	}
	else
	{
		/* UPDATE: (oldtup, newtup) */
		tuple = trigdata->tg_newtuple;
		values[0] = copy_tuple(trigdata->tg_trigtuple, desc);
		values[1] = copy_tuple(tuple, desc);
	}

	/* prepare INSERT query */
	sql = makeStringInfo();
	appendStringInfo(sql, "INSERT INTO dbms_redefinition.__log_%u__(pk, row) "
		"VALUES(CASE WHEN $1 IS NULL THEN NULL ELSE (ROW(", relid);
	appendStringInfo(sql, "$1.%s", quote_identifier(trigdata->tg_trigger->tgargs[0]));
	for (int i = 1; i < trigdata->tg_trigger->tgnargs; ++i)
		appendStringInfo(sql, ", $1.%s", quote_identifier(trigdata->tg_trigger->tgargs[i]));
	appendStringInfo(sql, ")::dbms_redefinition.__pk_%u__) END, $2)", relid);

	/* execute the INSERT query */
	execute_with_args(SPI_OK_INSERT, sql->data, 2, argtypes, values, nulls);

	SPI_finish();

	PG_RETURN_POINTER(tuple);
}

/**
 * @fn      Datum libred_apply(PG_FUNCTION_ARGS)
 * @brief   Apply operations in log table into temp table.
 *
 * libred_apply(sql_peek, sql_insert, sql_delete, sql_update, sql_pop,  count)
 *
 * @param	sql_peek	SQL to pop tuple from log table.
 * @param	sql_insert	SQL to insert into temp table.
 * @param	sql_delete	SQL to delete from temp table.
 * @param	sql_update	SQL to update temp table.
 * @param	sql_pop	SQL to bulk-delete tuples from log table.
 * @param	count		Max number of operations, or no count iff <=0.
 * @retval				Number of performed operations.
 */
Datum
libred_apply(PG_FUNCTION_ARGS)
{
#define DEFAULT_PEEK_COUNT	1000

	const char *sql_peek = PG_GETARG_CSTRING(0);
	const char *sql_insert = PG_GETARG_CSTRING(1);
	const char *sql_delete = PG_GETARG_CSTRING(2);
	const char *sql_update = PG_GETARG_CSTRING(3);
	/* sql_pop, the fourth arg, will be used in the loop below */
	int32		count = PG_GETARG_INT32(5);

	SPIPlanPtr		plan_peek = NULL;
	SPIPlanPtr		plan_insert = NULL;
	SPIPlanPtr		plan_delete = NULL;
	SPIPlanPtr		plan_update = NULL;
	uint32			n, i;
	Oid				argtypes_peek[1] = { INT4OID };
	Datum			values_peek[1];
	const char			nulls_peek[1] = { 0 };
	StringInfoData		sql_pop;

	initStringInfo(&sql_pop);

	/* authority check */
	must_be_superuser("libred_apply");

	/* connect to SPI manager */
	libred_init();

	/* peek tuple in log */
	plan_peek = libred_prepare(sql_peek, 1, argtypes_peek);

	for (n = 0;;)
	{
		int				ntuples;
		SPITupleTable  *tuptable;
		TupleDesc		desc;
		Oid				argtypes[3];	/* id, pk, row */
		Datum			values[3];		/* id, pk, row */
		bool			nulls[3];		/* id, pk, row */

		/* peek tuple in log */
		if (count <= 0)
			values_peek[0] = Int32GetDatum(DEFAULT_PEEK_COUNT);
		else
			values_peek[0] = Int32GetDatum(Min(count - n, DEFAULT_PEEK_COUNT));

		execute_plan(SPI_OK_SELECT, plan_peek, values_peek, nulls_peek);
		if (SPI_processed <= 0)
			break;

		/* copy tuptable because we will call other sqls. */
		ntuples = SPI_processed;
		tuptable = SPI_tuptable;
		desc = tuptable->tupdesc;
		argtypes[0] = SPI_gettypeid(desc, 1);	/* id */
		argtypes[1] = SPI_gettypeid(desc, 2);	/* pk */
		argtypes[2] = SPI_gettypeid(desc, 3);	/* row */

		resetStringInfo(&sql_pop);
		appendStringInfoString(&sql_pop, PG_GETARG_CSTRING(4));

		for (i = 0; i < ntuples; i++, n++)
		{
			HeapTuple	tuple;
			char *pkid;

			tuple = tuptable->vals[i];
			values[0] = SPI_getbinval(tuple, desc, 1, &nulls[0]);
			values[1] = SPI_getbinval(tuple, desc, 2, &nulls[1]);
			values[2] = SPI_getbinval(tuple, desc, 3, &nulls[2]);

			pkid = SPI_getvalue(tuple, desc, 1);
			Assert(pkid != NULL);

			if (nulls[1])
			{
				/* INSERT */
				if (plan_insert == NULL)
					plan_insert = libred_prepare(sql_insert, 1, &argtypes[2]);
				execute_plan(SPI_OK_INSERT, plan_insert, &values[2], (nulls[2] ? "n" : " "));
			}
			else if (nulls[2])
			{
				/* DELETE */
				if (plan_delete == NULL)
					plan_delete = libred_prepare(sql_delete, 1, &argtypes[1]);
				execute_plan(SPI_OK_DELETE, plan_delete, &values[1], (nulls[1] ? "n" : " "));
			}
			else
			{
				/* UPDATE */
				if (plan_update == NULL)
					plan_update = libred_prepare(sql_update, 2, &argtypes[1]);
				execute_plan(SPI_OK_UPDATE, plan_update, &values[1], (nulls[1] ? "n" : " "));
			}

			/* Add the primary key ID of each row from the log
			 * table we have processed so far to this
			 * DELETE ... IN (...) query string, so we
			 * can delete all the rows we have processed at-once.
			 */
			if (i == 0)
				appendStringInfoString(&sql_pop, pkid);
			else
				appendStringInfo(&sql_pop, ",%s", pkid);
			pfree(pkid);
		}
		/* i must be > 0 (and hence we must have some rows to delete)
		 * since SPI_processed > 0
		 */
		Assert(i > 0);
		appendStringInfoString(&sql_pop, ");");

		/* Bulk delete of processed rows from the log table */
		execute(SPI_OK_DELETE, sql_pop.data);

		SPI_freetuptable(tuptable);
	}

	SPI_finish();

	PG_RETURN_INT32(n);
}

static char *
get_relation_name(Oid relid)
{
	Oid		nsp = get_rel_namespace(relid);
	char   *nspname;
	char   *strver;
	int ver;

	if (!OidIsValid(nsp))
		elog(ERROR, "table name not found for OID %u", relid);

	/* Get the version of the running server */
	strver = GetConfigOptionByName("server_version_num", NULL, 
									false	    /* missing_ok */);

	ver = atoi(strver);
	pfree(strver);

	if (ver < 140000)
		elog(ERROR, "dbms_redefinition requires Halo 14,15,16 & PostgreSQL 14,15,16.");

	/* Always qualify the name */
	if (OidIsValid(nsp))
		nspname = get_namespace_name(nsp);
	else
		nspname = NULL;

	return quote_qualified_identifier(nspname, get_rel_name(relid));
}

char *
parse_error(Oid index)
{
	elog(ERROR, "unexpected index definition: %s", pg_get_indexdef_string(index));
	return NULL;
}

char *
skip_const(Oid index, char *sql, const char *arg1, const char *arg2)
{
	size_t	len;

	if ((arg1 && strncmp(sql, arg1, (len = strlen(arg1))) == 0) ||
		(arg2 && strncmp(sql, arg2, (len = strlen(arg2))) == 0))
	{
		sql[len] = '\0';
		return sql + len + 1;
	}

	/* error */
	return parse_error(index);
}

char *
skip_until_const(Oid index, char *sql, const char *what)
{
	char *pos;

	if ((pos = strstr(sql, what)))
	{
		size_t	len;

		len = strlen(what);
		pos[-1] = '\0';
		return pos + len + 1;
	}

	/* error */
	return parse_error(index);
}

char *
skip_ident(Oid index, char *sql)
{
	while (*sql && isspace((unsigned char) *sql))
		sql++;

	if (*sql == '"')
	{
		sql++;
		for (;;)
		{
			char *end = strchr(sql, '"');
			if (end == NULL)
				return parse_error(index);
			else if (end[1] != '"')
			{
				end[1] = '\0';
				return end + 2;
			}
			else	/* escaped quote ("") */
				sql = end + 2;
		}
	}
	else
	{
		while (*sql && IsToken(*sql))
			sql++;
		*sql = '\0';
		return sql + 1;
	}

	/* error */
	return parse_error(index);
}

/*
 * Skip until 'end' character found. The 'end' character is replaced with \0.
 * Returns the next character of the 'end', or NULL if 'end' is not found.
 */
char *
skip_until(Oid index, char *sql, char end)
{
	char	instr = 0;
	int		nopen = 0;

	for (; *sql && (nopen > 0 || instr != 0 || *sql != end); sql++)
	{
		if (instr)
		{
			if (sql[0] == instr)
			{
				if (sql[1] == instr)
					sql++;
				else
					instr = 0;
			}
			else if (sql[0] == '\\')
				sql++;	/* next char is always string */
		}
		else
		{
			switch (sql[0])
			{
				case '(':
					nopen++;
					break;
				case ')':
					nopen--;
					break;
				case '\'':
				case '"':
					instr = sql[0];
					break;
			}
		}
	}

	if (nopen == 0 && instr == 0)
	{
		if (*sql)
		{
			*sql = '\0';
			return sql + 1;
		}
		else
			return NULL;
	}

	/* error */
	return parse_error(index);
}

static void
parse_indexdef(IndexDef *stmt, Oid index, Oid table)
{
	char *sql = pg_get_indexdef_string(index);
	const char *idxname = get_quoted_relname(index);
	const char *tblname = get_relation_name(table);
	const char *limit = strchr(sql, '\0');

	/* CREATE [UNIQUE] INDEX */
	stmt->create = sql;
	sql = skip_const(index, sql, "CREATE INDEX", "CREATE UNIQUE INDEX");
	/* index */
	stmt->index = sql;
	sql = skip_const(index, sql, idxname, NULL);
	/* ON */
	sql = skip_const(index, sql, "ON", NULL);
	/* table */
	stmt->table = sql;
	sql = skip_const(index, sql, tblname, NULL);
	/* USING */
	sql = skip_const(index, sql, "USING", NULL);
	/* type */
	stmt->type = sql;
	sql = skip_ident(index, sql);
	/* (columns) */
	if ((sql = strchr(sql, '(')) == NULL)
		parse_error(index);
	sql++;
	stmt->columns = sql;
	if ((sql = skip_until(index, sql, ')')) == NULL)
		parse_error(index);

	/* options */
	stmt->options = sql;
	stmt->tablespace = NULL;
	stmt->where = NULL;

	/* Is there a tablespace? Note that apparently there is never, but
	 * if there was one it would appear here. */
	if (sql < limit && strstr(sql, "TABLESPACE"))
	{
		sql = skip_until_const(index, sql, "TABLESPACE");
		stmt->tablespace = sql;
		sql = skip_ident(index, sql);
	}

	/* Note: assuming WHERE is the only clause allowed after TABLESPACE */
	if (sql < limit && strstr(sql, "WHERE"))
	{
		sql = skip_until_const(index, sql, "WHERE");
		stmt->where = sql;
	}

	elog(DEBUG2, "indexdef.create  = %s", stmt->create);
	elog(DEBUG2, "indexdef.index   = %s", stmt->index);
	elog(DEBUG2, "indexdef.table   = %s", stmt->table);
	elog(DEBUG2, "indexdef.type    = %s", stmt->type);
	elog(DEBUG2, "indexdef.columns = %s", stmt->columns);
	elog(DEBUG2, "indexdef.options = %s", stmt->options);
	elog(DEBUG2, "indexdef.tspace  = %s", stmt->tablespace);
	elog(DEBUG2, "indexdef.where   = %s", stmt->where);
}

/*
 * Parse the trailing ... [ COLLATE X ] [ DESC ] [ NULLS { FIRST | LAST } ] from an index
 * definition column.
 * Returned values point to token. \0's are inserted to separate parsed parts.
 */
static void
parse_indexdef_col(char *token, char **desc, char **nulls, char **collate)
{
	char *pos;

	/* easier to walk backwards than to parse quotes and escapes... */
	if (NULL != (pos = strstr(token, " NULLS FIRST")))
	{
		*nulls = pos + 1;
		*pos = '\0';
	}
	else if (NULL != (pos = strstr(token, " NULLS LAST")))
	{
		*nulls = pos + 1;
		*pos = '\0';
	}
	if (NULL != (pos = strstr(token, " DESC")))
	{
		*desc = pos + 1;
		*pos = '\0';
	}
	if (NULL != (pos = strstr(token, " COLLATE ")))
	{
		*collate = pos + 1;
		*pos = '\0';
	}
}

/**
 * @fn      Datum libred_get_order_by(PG_FUNCTION_ARGS)
 * @brief   Get key definition of the index.
 *
 * libred_get_order_by(index, table)
 *
 * @param	index	Oid of target index.
 * @param	table	Oid of table of the index.
 * @retval			Create index DDL for temp table.
 */
Datum
libred_get_order_by(PG_FUNCTION_ARGS)
{
	Oid				index = PG_GETARG_OID(0);
	Oid				table = PG_GETARG_OID(1);
	IndexDef		stmt;
	char		   *token;
	char		   *next;
	StringInfoData	str;
	Relation		indexRel = NULL;
	int				nattr;

	parse_indexdef(&stmt, index, table);

	/*
	 * FIXME: this is very unreliable implementation but I don't want to
	 * re-implement customized versions of pg_get_indexdef_string...
	 */

	initStringInfo(&str);
	for (nattr = 0, next = stmt.columns; next; nattr++)
	{
		char *opcname;
		char *coldesc = NULL;
		char *colnulls = NULL;
		char *colcollate = NULL;

		token = next;
		while (isspace((unsigned char) *token))
			token++;
		next = skip_until(index, next, ',');
		parse_indexdef_col(token, &coldesc, &colnulls, &colcollate);
		opcname = skip_until(index, token, ' ');
		appendStringInfoString(&str, token);
		if (colcollate)
			appendStringInfo(&str, " %s", colcollate);
		if (coldesc)
			appendStringInfo(&str, " %s", coldesc);
		if (opcname)
		{
			/* lookup default operator name from operator class */

			Oid				opclass;
			Oid				oprid;
			int16			strategy = BTLessStrategyNumber;
			Oid				opcintype;
			Oid				opfamily;
			HeapTuple		tp;
			Form_pg_opclass	opclassTup;

			opclass = OpclassnameGetOpcid(BTREE_AM_OID, opcname);

			/* Retrieve operator information. */
			tp = SearchSysCache(CLAOID, ObjectIdGetDatum(opclass), 0, 0, 0);
			if (!HeapTupleIsValid(tp))
				elog(ERROR, "cache lookup failed for opclass %u", opclass);
			opclassTup = (Form_pg_opclass) GETSTRUCT(tp);
			opfamily = opclassTup->opcfamily;
			opcintype = opclassTup->opcintype;
			ReleaseSysCache(tp);

			if (!OidIsValid(opcintype))
			{
				if (indexRel == NULL)
					indexRel = index_open(index, NoLock);

				opcintype = TupleDescAttr(RelationGetDescr(indexRel), nattr)->atttypid;
			}

			oprid = get_opfamily_member(opfamily, opcintype, opcintype, strategy);
			if (!OidIsValid(oprid))
				elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
					 strategy, opcintype, opcintype, opfamily);

			opcname[-1] = '\0';
			appendStringInfo(&str, " USING %s", get_opname(oprid));
		}
		if (colnulls)
			appendStringInfo(&str, " %s", colnulls);
		if (next)
			appendStringInfoString(&str, ", ");
	}

	if (indexRel != NULL)
		index_close(indexRel, NoLock);

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

/**
 * @fn      Datum libred_indexdef(PG_FUNCTION_ARGS)
 * @brief   Reproduce DDL that create index at the temp table.
 *
 * libred_indexdef(index, table)
 *
 * @param	index		Oid of target index.
 * @param	table		Oid of table of the index.
 * @param	tablespace	Namespace for the index. If NULL keep the original.
 * @param   boolean		Whether to use CONCURRENTLY when creating the index.
 * @retval			Create index DDL for temp table.
 */
Datum
libred_indexdef(PG_FUNCTION_ARGS)
{
	Oid				index;
	Oid				table;
	Name			tablespace = NULL;
	IndexDef		stmt;
	StringInfoData	str;
	bool			concurrent_index = PG_GETARG_BOOL(3);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	index = PG_GETARG_OID(0);
	table = PG_GETARG_OID(1);

	if (!PG_ARGISNULL(2))
		tablespace = PG_GETARG_NAME(2);

	parse_indexdef(&stmt, index, table);

	initStringInfo(&str);
	if (concurrent_index)
		appendStringInfo(&str, "%s CONCURRENTLY __index_%u__ ON %s USING %s (%s)%s",
			stmt.create, index, stmt.table, stmt.type, stmt.columns, stmt.options);
	else
		appendStringInfo(&str, "%s __index_%u__ ON dbms_redefinition.__table_%u__ USING %s (%s)%s",
			stmt.create, index, table, stmt.type, stmt.columns, stmt.options);

	/* specify the new tablespace or the original one if any */
	if (tablespace || stmt.tablespace)
		appendStringInfo(&str, " TABLESPACE %s",
			(tablespace ? quote_identifier(NameStr(*tablespace)) : stmt.tablespace));

	if (stmt.where)
		appendStringInfo(&str, " WHERE %s", stmt.where);

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

static Oid
getoid(HeapTuple tuple, TupleDesc desc, int column)
{
	bool	isnull;
	Datum	datum = SPI_getbinval(tuple, desc, column, &isnull);
	return isnull ? InvalidOid : DatumGetObjectId(datum);
}

/**
 * @fn      Datum libred_swap(PG_FUNCTION_ARGS)
 * @brief   Swapping relfilenode of tables and relation ids of toast tables
 *          and toast indexes.
 *
 * libred_swap(oid, relname)
 *
 * TODO: remove useless CommandCounterIncrement().
 *
 * @param	oid		Oid of table of target.
 * @retval			None.
 */
Datum
libred_swap(PG_FUNCTION_ARGS)
{
	Oid				oid = PG_GETARG_OID(0);
	const char	   *relname = get_quoted_relname(oid);
	const char	   *nspname = get_quoted_nspname(oid);
	Oid 			argtypes[1] = { OIDOID };
	bool	 		nulls[1] = { 0 };
	Datum	 		values[1];
	SPITupleTable  *tuptable;
	TupleDesc		desc;
	HeapTuple		tuple;
	uint32			records;
	uint32			i;

	Oid				reltoastrelid1;
	Oid				reltoastidxid1;
	Oid				oid2;
	Oid				reltoastrelid2;
	Oid				reltoastidxid2;
	Oid				owner1;
	Oid				owner2;

	/* authority check */
	must_be_superuser("libred_swap");

	/* connect to SPI manager */
	libred_init();

	/* swap relfilenode and dependencies for tables. */
	values[0] = ObjectIdGetDatum(oid);
	execute_with_args(SPI_OK_SELECT,
		"SELECT X.reltoastrelid, TX.indexrelid, X.relowner,"
		"       Y.oid, Y.reltoastrelid, TY.indexrelid, Y.relowner"
		"  FROM pg_catalog.pg_class X LEFT JOIN pg_catalog.pg_index TX"
		"         ON X.reltoastrelid = TX.indrelid AND TX.indisvalid,"
		"       pg_catalog.pg_class Y LEFT JOIN pg_catalog.pg_index TY"
		"         ON Y.reltoastrelid = TY.indrelid AND TY.indisvalid"
		" WHERE X.oid = $1"
		"   AND Y.oid = ('dbms_redefinition.__table_' || X.oid || '__')::regclass",
		1, argtypes, values, nulls);

	tuptable = SPI_tuptable;
	desc = tuptable->tupdesc;
	records = SPI_processed;

	if (records == 0)
		elog(ERROR, "libred_swap : no swap target");

	tuple = tuptable->vals[0];

	reltoastrelid1 = getoid(tuple, desc, 1);
	reltoastidxid1 = getoid(tuple, desc, 2);
	owner1 = getoid(tuple, desc, 3);
	oid2 = getoid(tuple, desc, 4);
	reltoastrelid2 = getoid(tuple, desc, 5);
	reltoastidxid2 = getoid(tuple, desc, 6);
	owner2 = getoid(tuple, desc, 7);

	/* change owner of new relation to original owner */
	if (owner1 != owner2)
	{
		ATExecChangeOwner(oid2, owner1, true, AccessExclusiveLock);
		CommandCounterIncrement();
	}

	/*
	 * Sanity check if both relations are locked in access exclusive mode
	 * before swapping these files.
	 */
	{
		LOCKTAG	tag;

		SET_LOCKTAG_RELATION(tag, MyDatabaseId, oid);
		if (!LockHeldByMe(&tag, AccessExclusiveLock))
			elog(ERROR, "must hold access exclusive lock on table \"%s\"", relname);

		SET_LOCKTAG_RELATION(tag, MyDatabaseId, oid2);
		if (!LockHeldByMe(&tag, AccessExclusiveLock))
			elog(ERROR, "must hold access exclusive lock on table \"__table_%u__\"", oid);
	}

	/* swap tables. */
	swap_heap_or_index_files(oid, oid2);
	CommandCounterIncrement();

	/* swap indexes. */
	values[0] = ObjectIdGetDatum(oid);
	execute_with_args(SPI_OK_SELECT,
		"SELECT X.oid, Y.oid"
		"  FROM pg_catalog.pg_index I,"
		"       pg_catalog.pg_class X,"
		"       pg_catalog.pg_class Y"
		" WHERE I.indrelid = $1"
		"   AND I.indexrelid = X.oid"
		"   AND I.indisvalid"
		"   AND Y.oid = ('dbms_redefinition.__index_' || X.oid || '__')::regclass",
		1, argtypes, values, nulls);

	tuptable = SPI_tuptable;
	desc = tuptable->tupdesc;
	records = SPI_processed;

	for (i = 0; i < records; i++)
	{
		Oid		idx1, idx2;

		tuple = tuptable->vals[i];
		idx1 = getoid(tuple, desc, 1);
		idx2 = getoid(tuple, desc, 2);
		swap_heap_or_index_files(idx1, idx2);

		CommandCounterIncrement();
	}

	/* swap names for toast tables and toast indexes */
	if (reltoastrelid1 == InvalidOid && reltoastrelid2 == InvalidOid)
	{
		if (reltoastidxid1 != InvalidOid ||
			reltoastidxid2 != InvalidOid)
			elog(ERROR, "libred_swap : unexpected toast relations (T1=%u, I1=%u, T2=%u, I2=%u",
				reltoastrelid1, reltoastidxid1, reltoastrelid2, reltoastidxid2);
		/* do nothing */
	}
	else if (reltoastrelid1 == InvalidOid)
	{
		char	name[NAMEDATALEN];

		if (reltoastidxid1 != InvalidOid ||
			reltoastidxid2 == InvalidOid)
			elog(ERROR, "libred_swap : unexpected toast relations (T1=%u, I1=%u, T2=%u, I2=%u",
				reltoastrelid1, reltoastidxid1, reltoastrelid2, reltoastidxid2);

		/* rename Y to X */
		snprintf(name, NAMEDATALEN, "pg_toast_%u", oid);
		RENAME_REL(reltoastrelid2, name);
		snprintf(name, NAMEDATALEN, "pg_toast_%u_index", oid);
		RENAME_INDEX(reltoastidxid2, name);
		CommandCounterIncrement();
	}
	else if (reltoastrelid2 == InvalidOid)
	{
		char	name[NAMEDATALEN];

		if (reltoastidxid1 == InvalidOid ||
			reltoastidxid2 != InvalidOid)
			elog(ERROR, "libred_swap : unexpected toast relations (T1=%u, I1=%u, T2=%u, I2=%u",
				reltoastrelid1, reltoastidxid1, reltoastrelid2, reltoastidxid2);

		/* rename X to Y */
		snprintf(name, NAMEDATALEN, "pg_toast_%u", oid2);
		RENAME_REL(reltoastrelid1, name);
		snprintf(name, NAMEDATALEN, "pg_toast_%u_index", oid2);
		RENAME_INDEX(reltoastidxid1, name);
		CommandCounterIncrement();
	}
	else if (reltoastrelid1 != InvalidOid)
	{
		char	name[NAMEDATALEN];
		int		pid = getpid();

		/* rename X to TEMP */
		snprintf(name, NAMEDATALEN, "pg_toast_pid%d", pid);
		RENAME_REL(reltoastrelid1, name);
		snprintf(name, NAMEDATALEN, "pg_toast_pid%d_index", pid);
		RENAME_INDEX(reltoastidxid1, name);
		CommandCounterIncrement();

		/* rename Y to X */
		snprintf(name, NAMEDATALEN, "pg_toast_%u", oid);
		RENAME_REL(reltoastrelid2, name);
		snprintf(name, NAMEDATALEN, "pg_toast_%u_index", oid);
		RENAME_INDEX(reltoastidxid2, name);
		CommandCounterIncrement();

		/* rename TEMP to Y */
		snprintf(name, NAMEDATALEN, "pg_toast_%u", oid2);
		RENAME_REL(reltoastrelid1, name);
		snprintf(name, NAMEDATALEN, "pg_toast_%u_index", oid2);
		RENAME_INDEX(reltoastidxid1, name);
		CommandCounterIncrement();
	}

	/* drop dbms_redefinition trigger */
	execute_with_format(
		SPI_OK_UTILITY,
		"DROP TRIGGER IF EXISTS __libred_trigger_%u__ ON %s.%s CASCADE",
		oid, nspname, relname);

	SPI_finish();

	PG_RETURN_VOID();
}

/**
 * @fn      Datum libred_drop(PG_FUNCTION_ARGS)
 * @brief   Delete temporarily objects.
 *
 * libred_drop(oid, relname)
 *
 * @param	oid		Oid of target table.
 * @retval			None.
 */
Datum
libred_drop(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);
	int			numobj = PG_GETARG_INT32(1);
	const char *relname = get_quoted_relname(oid);
	const char *nspname = get_quoted_nspname(oid);

	if (!(relname && nspname))
	{
		elog(ERROR, "table name not found for OID %u", oid);
		PG_RETURN_VOID();
	}

	/* authority check */
	must_be_superuser("libred_drop");

	/* connect to SPI manager */
	libred_init();

	/*
	 * To prevent concurrent lockers of the repack target table from causing
	 * deadlocks, take an exclusive lock on it. Consider that the following
	 * commands take exclusive lock on tables log_xxx and the target table
	 * itself when deleting the libred_trigger on it, while concurrent
	 * updaters require row exclusive lock on the target table and in
	 * addition, on the log_xxx table, because of the trigger.
	 *
	 * Consider how a deadlock could occur - if the DROP TABLE __log_%u__
	 * gets a lock on __log_%u__ table before a concurrent updater could get it
	 * but after the updater has obtained a lock on the target table, the
	 * subsequent DROP TRIGGER ... ON target-table would report a deadlock as
	 * it finds itself waiting for a lock on target-table held by the updater,
	 * which in turn, is waiting for lock on __log_%u__ table.
	 *
	 * Fixes deadlock mentioned in the Github issue #55.
	 *
	 * Skip the lock if we are not going to do anything.
	 * Otherwise, if repack gets accidentally run twice for the same table
	 * at the same time, the second repack, in order to perform
	 * a pointless cleanup, has to wait until the first one completes.
	 * This adds an ACCESS EXCLUSIVE lock request into the queue
	 * making the table effectively inaccessible for any other backend.
	 */
	if (numobj > 0)
	{
		execute_with_format(
			SPI_OK_UTILITY,
			"LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE",
			nspname, relname);
	}

	/* drop log table: must be done before dropping the pk type,
	 * since the log table is dependent on the pk type. (That's
	 * why we check numobj > 1 here.)
	 */
	if (numobj > 1)
	{
		execute_with_format(
			SPI_OK_UTILITY,
			"DROP TABLE IF EXISTS dbms_redefinition.__log_%u__ CASCADE",
			oid);
		--numobj;
	}

	/* drop type for pk type */
	if (numobj > 0)
	{
		execute_with_format(
			SPI_OK_UTILITY,
			"DROP TYPE IF EXISTS dbms_redefinition.__pk_%u__",
			oid);
		--numobj;
	}

	/*
	 * drop dbms_redefinition trigger: We have already dropped the trigger in normal
	 * cases, but it can be left on error.
	 */
	if (numobj > 0)
	{
		execute_with_format(
			SPI_OK_UTILITY,
			"DROP TRIGGER IF EXISTS __libred_trigger_%u__ ON %s.%s CASCADE",
			oid, nspname, relname);
		--numobj;
	}

	/* drop temp table */
	if (numobj > 0)
	{
		execute_with_format(
			SPI_OK_UTILITY,
			"DROP TABLE IF EXISTS dbms_redefinition.__table_%u__ CASCADE",
			oid);
		--numobj;
	}

	SPI_finish();

	PG_RETURN_VOID();
}

Datum
libred_disable_autovacuum(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	/* connect to SPI manager */
	libred_init();

	execute_with_format(
		SPI_OK_UTILITY,
		"ALTER TABLE %s SET (autovacuum_enabled = off)",
		get_relation_name(oid));

	SPI_finish();

	PG_RETURN_VOID();
}

Datum
libred_reset_autovacuum(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	/* connect to SPI manager */
	libred_init();

	execute_with_format(
		SPI_OK_UTILITY,
		"ALTER TABLE %s RESET (autovacuum_enabled)",
		get_relation_name(oid));

	SPI_finish();

	PG_RETURN_VOID();
}

/* init SPI */
static void
libred_init(void)
{
	int		ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "dbms_redefinition: SPI_connect returned %d", ret);
}

/* prepare plan */
static SPIPlanPtr
libred_prepare(const char *src, int nargs, Oid *argtypes)
{
	SPIPlanPtr	plan = SPI_prepare(src, nargs, argtypes);
	if (plan == NULL)
		elog(ERROR, "dbms_redefinition: libred_prepare failed (code=%d, query=%s)", SPI_result, src);
	return plan;
}

static const char *
get_quoted_relname(Oid oid)
{
	const char *relname = get_rel_name(oid);
	return (relname ? quote_identifier(relname) : NULL);
}

static const char *
get_quoted_nspname(Oid oid)
{
	const char *nspname = get_namespace_name(get_rel_namespace(oid));
	return (nspname ? quote_identifier(nspname) : NULL);
}

/*
 * This is a copy of swap_relation_files in cluster.c, but it also swaps
 * relfrozenxid.
 */
static void
swap_heap_or_index_files(Oid r1, Oid r2)
{
	Relation	relRelation;
	HeapTuple	reltup1,
				reltup2;
	Form_pg_class relform1,
				relform2;
	Oid			swaptemp;
	CatalogIndexState indstate;

	/* We need writable copies of both pg_class tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
	if (!HeapTupleIsValid(reltup1))
		elog(ERROR, "cache lookup failed for relation %u", r1);
	relform1 = (Form_pg_class) GETSTRUCT(reltup1);

	reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
	if (!HeapTupleIsValid(reltup2))
		elog(ERROR, "cache lookup failed for relation %u", r2);
	relform2 = (Form_pg_class) GETSTRUCT(reltup2);

	Assert(relform1->relkind == relform2->relkind);

	/*
	 * Actually swap the fields in the two tuples
	 */
	swaptemp = relform1->relfilenode;
	relform1->relfilenode = relform2->relfilenode;
	relform2->relfilenode = swaptemp;

	swaptemp = relform1->reltablespace;
	relform1->reltablespace = relform2->reltablespace;
	relform2->reltablespace = swaptemp;

	swaptemp = relform1->reltoastrelid;
	relform1->reltoastrelid = relform2->reltoastrelid;
	relform2->reltoastrelid = swaptemp;

	/*
	 * Swap relfrozenxid and relminmxid, as they must be consistent with the data
	 */
	if (relform1->relkind != RELKIND_INDEX)
	{
		TransactionId frozenxid;
		MultiXactId	minmxid;
	
		frozenxid = relform1->relfrozenxid;
		relform1->relfrozenxid = relform2->relfrozenxid;
		relform2->relfrozenxid = frozenxid;

		minmxid = relform1->relminmxid;
		relform1->relminmxid = relform2->relminmxid;
		relform2->relminmxid = minmxid;
	}

	/* swap size statistics too, since new rel has freshly-updated stats */
	{
		int32		swap_pages;
		float4		swap_tuples;
		int32		swap_allvisible;

		swap_pages = relform1->relpages;
		relform1->relpages = relform2->relpages;
		relform2->relpages = swap_pages;

		swap_tuples = relform1->reltuples;
		relform1->reltuples = relform2->reltuples;
		relform2->reltuples = swap_tuples;

		swap_allvisible = relform1->relallvisible;
		relform1->relallvisible = relform2->relallvisible;
		relform2->relallvisible = swap_allvisible;
	}

	indstate = CatalogOpenIndexes(relRelation);

	CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1, indstate);
	CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2, indstate);

	CatalogCloseIndexes(indstate);

	/*
	 * If we have toast tables associated with the relations being swapped,
	 * change their dependency links to re-associate them with their new
	 * owning relations.  Otherwise the wrong one will get dropped ...
	 *
	 * NOTE: it is possible that only one table has a toast table; this can
	 * happen in CLUSTER if there were dropped columns in the old table, and
	 * in ALTER TABLE when adding or changing type of columns.
	 *
	 * NOTE: at present, a TOAST table's only dependency is the one on its
	 * owning table.  If more are ever created, we'd need to use something
	 * more selective than deleteDependencyRecordsFor() to get rid of only the
	 * link we want.
	 */
	if (relform1->reltoastrelid || relform2->reltoastrelid)
	{
		ObjectAddress baseobject,
					toastobject;
		long		count;

		/* Delete old dependencies */
		if (relform1->reltoastrelid)
		{
			count = deleteDependencyRecordsFor(RelationRelationId,
											   relform1->reltoastrelid,
											   false);
			if (count != 1)
				elog(ERROR, "expected one dependency record for TOAST table, found %ld",
					 count);
		}
		if (relform2->reltoastrelid)
		{
			count = deleteDependencyRecordsFor(RelationRelationId,
											   relform2->reltoastrelid,
											   false);
			if (count != 1)
				elog(ERROR, "expected one dependency record for TOAST table, found %ld",
					 count);
		}

		/* Register new dependencies */
		baseobject.classId = RelationRelationId;
		baseobject.objectSubId = 0;
		toastobject.classId = RelationRelationId;
		toastobject.objectSubId = 0;

		if (relform1->reltoastrelid)
		{
			baseobject.objectId = r1;
			toastobject.objectId = relform1->reltoastrelid;
			recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
		}

		if (relform2->reltoastrelid)
		{
			baseobject.objectId = r2;
			toastobject.objectId = relform2->reltoastrelid;
			recordDependencyOn(&toastobject, &baseobject, DEPENDENCY_INTERNAL);
		}
	}

	/*
	 * Blow away the old relcache entries now.	We need this kluge because
	 * relcache.c keeps a link to the smgr relation for the physical file, and
	 * that will be out of date as soon as we do CommandCounterIncrement.
	 * Whichever of the rels is the second to be cleared during cache
	 * invalidation will have a dangling reference to an already-deleted smgr
	 * relation.  Rather than trying to avoid this by ordering operations just
	 * so, it's easiest to not have the relcache entries there at all.
	 * (Fortunately, since one of the entries is local in our transaction,
	 * it's sufficient to clear out our own relcache this way; the problem
	 * cannot arise for other backends when they see our update on the
	 * non-local relation.)
	 */
	RelationForgetRelation(r1);
	RelationForgetRelation(r2);

	/* Clean up. */
	heap_freetuple(reltup1);
	heap_freetuple(reltup2);

	table_close(relRelation, RowExclusiveLock);
}

/**
 * @fn      Datum libred_index_swap(PG_FUNCTION_ARGS)
 * @brief   Swap out an original index on a table with the newly-created one.
 *
 * libred_index_swap(index)
 *
 * @param	index	Oid of the *original* index.
 * @retval	void
 */
Datum
libred_index_swap(PG_FUNCTION_ARGS)
{
	Oid                orig_idx_oid = PG_GETARG_OID(0);
	Oid                repacked_idx_oid;
	StringInfoData     str;
	SPITupleTable      *tuptable;
	TupleDesc          desc;
	HeapTuple          tuple;

	/* authority check */
	must_be_superuser("libred_index_swap");

	/* connect to SPI manager */
	libred_init();

	initStringInfo(&str);

	/* Find the OID of our new index. */
	appendStringInfo(&str, "SELECT oid FROM pg_class "
					 "WHERE relname = '__index_%u__' AND relkind = 'i'",
					 orig_idx_oid);
	execute(SPI_OK_SELECT, str.data);
	if (SPI_processed != 1)
		elog(ERROR, "Could not find index '__index_%u__', found " UINT64_FORMAT " matches",
			 orig_idx_oid, (uint64) SPI_processed);

	tuptable = SPI_tuptable;
	desc = tuptable->tupdesc;
	tuple = tuptable->vals[0];
	repacked_idx_oid = getoid(tuple, desc, 1);
	swap_heap_or_index_files(orig_idx_oid, repacked_idx_oid);
	SPI_finish();
	PG_RETURN_VOID();
}

/**
 * @fn      Datum get_table_and_inheritors(PG_FUNCTION_ARGS)
 * @brief   Return array containing Oids of parent table and its children.
 *          Note that this function does not release relation locks.
 *
 * get_table_and_inheritors(table)
 *
 * @param	table	parent table.
 * @retval	regclass[]
 */
Datum
libred_get_table_and_inheritors(PG_FUNCTION_ARGS)
{
	Oid			parent = PG_GETARG_OID(0);
	List	   *relations;
	Datum	   *relations_array;
	int			relations_array_size;
	ArrayType  *result;
	ListCell   *lc;
	int			i;

	LockRelationOid(parent, AccessShareLock);

	/* Check that parent table exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(parent)))
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(OIDOID));

	/* Also check that children exist */
	relations = find_all_inheritors(parent, AccessShareLock, NULL);

	relations_array_size = list_length(relations);
	if (relations_array_size == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(OIDOID));

	relations_array = palloc(relations_array_size * sizeof(Datum));

	i = 0;
	foreach (lc, relations)
		relations_array[i++] = ObjectIdGetDatum(lfirst_oid(lc));

	result = construct_array(relations_array,
							 relations_array_size,
							 OIDOID, sizeof(Oid),
							 true, 'i');

	pfree(relations_array);

	PG_RETURN_ARRAYTYPE_P(result);
}
