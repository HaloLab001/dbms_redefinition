/*
 * dbms_redefinition: dbms_redefinition--xxxx.sql
 *
 * 版权所有 (c) 2019-2023, 易景科技保留所有权利。
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

CREATE SCHEMA dbms_redefinition;

CREATE FUNCTION dbms_redefinition.version() RETURNS text AS
'MODULE_PATHNAME', 'dbms_redefinition_version'
LANGUAGE C IMMUTABLE STRICT;

-- Always specify search_path to 'pg_catalog' so that we
-- always can get schema-qualified relation name
CREATE FUNCTION dbms_redefinition.oid2text(oid) RETURNS text AS
$$
	SELECT textin(regclassout($1));
$$
LANGUAGE sql STABLE STRICT SET search_path to 'pg_catalog';

CREATE FUNCTION dbms_redefinition.get_index_columns(oid, text) RETURNS text AS
$$
  SELECT coalesce(string_agg(quote_ident(attname), $2), '')
    FROM pg_attribute,
         (SELECT indrelid,
                 indkey,
                 generate_series(0, indnatts-1) AS i
            FROM pg_index
           WHERE indexrelid = $1
         ) AS keys
   WHERE attrelid = indrelid
     AND attnum = indkey[i];
$$
LANGUAGE sql STABLE STRICT;

CREATE FUNCTION dbms_redefinition.get_order_by(oid, oid) RETURNS text AS
'MODULE_PATHNAME', 'migrate_get_order_by'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION dbms_redefinition.get_create_index_type(oid, name) RETURNS text AS
$$
  SELECT 'CREATE TYPE ' || $2 || ' AS (' ||
         coalesce(string_agg(quote_ident(attname) || ' ' ||
           pg_catalog.format_type(atttypid, atttypmod), ', '), '') || ')'
    FROM pg_attribute,
         (SELECT indrelid,
                 indkey,
                 generate_series(0, indnatts-1) AS i
            FROM pg_index
           WHERE indexrelid = $1
         ) AS keys
   WHERE attrelid = indrelid
     AND attnum = indkey[i];
$$
LANGUAGE sql STABLE STRICT;

CREATE FUNCTION dbms_redefinition.get_create_trigger(relid oid, pkid oid)
  RETURNS text AS
$$
  SELECT 'CREATE TRIGGER migrate_trigger' ||
         ' AFTER INSERT OR DELETE OR UPDATE ON ' || migrate.oid2text($1) ||
         ' FOR EACH ROW EXECUTE PROCEDURE migrate.migrate_trigger(' ||
         '''INSERT INTO migrate.log_' || $1 || '(pk, row) VALUES(' ||
         ' CASE WHEN $1 IS NULL THEN NULL ELSE (ROW($1.' ||
         migrate.get_index_columns($2, ', $1.') || ')::migrate.pk_' ||
         $1 || ') END, $2)'')';
$$
LANGUAGE sql STABLE STRICT;

CREATE FUNCTION dbms_redefinition.get_enable_trigger(relid oid)
  RETURNS text AS
$$
  SELECT 'ALTER TABLE ' || migrate.oid2text($1) ||
    ' ENABLE ALWAYS TRIGGER migrate_trigger';
$$
LANGUAGE sql STABLE STRICT;

CREATE FUNCTION dbms_redefinition.get_assign(oid, text) RETURNS text AS
$$
  SELECT '(' || coalesce(string_agg(quote_ident(attname), ', '), '') ||
         ') = (' || $2 || '.' ||
         coalesce(string_agg(quote_ident(attname), ', ' || $2 || '.'), '') || ')'
    FROM (SELECT attname FROM pg_attribute
           WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped
           ORDER BY attnum) tmp;
$$
LANGUAGE sql STABLE STRICT;

CREATE FUNCTION dbms_redefinition.get_compare_pkey(oid, text)
  RETURNS text AS
$$
  SELECT '(' || coalesce(string_agg(quote_ident(attname), ', '), '') ||
         ') = (' || $2 || '.' ||
         coalesce(string_agg(quote_ident(attname), ', ' || $2 || '.'), '') || ')'
    FROM pg_attribute,
         (SELECT indrelid,
                 indkey,
                 generate_series(0, indnatts-1) AS i
            FROM pg_index
           WHERE indexrelid = $1
         ) AS keys
   WHERE attrelid = indrelid
     AND attnum = indkey[i];
$$
LANGUAGE sql STABLE STRICT;

-- Get a column list for SELECT all columns including dropped ones.
-- We use NULLs of integer types for dropped columns (types are not important).
CREATE FUNCTION dbms_redefinition.get_columns_for_create_as(oid)
  RETURNS text AS
$$
SELECT coalesce(string_agg(c, ','), '') FROM (SELECT
	CASE WHEN attisdropped
		THEN 'NULL::integer AS ' || quote_ident(attname)
		ELSE quote_ident(attname)
	END AS c
FROM pg_attribute
WHERE attrelid = $1 AND attnum > 0 ORDER BY attnum
) AS COL
$$
LANGUAGE sql STABLE STRICT;

-- Get a column list for SELECT all columns excluding dropped ones.
CREATE FUNCTION dbms_redefinition.get_columns_for_insert(oid)
  RETURNS text AS
$$
SELECT coalesce(string_agg(c, ','), '') FROM (SELECT
  CASE WHEN attisdropped
    THEN NULL -- note string_agg ignore NULLs
    ELSE quote_ident(attname)
  END AS c
FROM pg_attribute
WHERE attrelid = $1 AND attnum > 0 ORDER BY attnum
) AS COL
$$
LANGUAGE sql STABLE STRICT;

-- Get a SQL text to DROP dropped columns for the table,
-- or NULL if it has no dropped columns.
CREATE FUNCTION dbms_redefinition.get_drop_columns(oid, text)
  RETURNS text AS
$$
SELECT
	'ALTER TABLE ' || $2 || ' ' || array_to_string(dropped_columns, ', ')
FROM (
	SELECT
		array_agg('DROP COLUMN ' || quote_ident(attname)) AS dropped_columns
	FROM (
		SELECT * FROM pg_attribute
		WHERE attrelid = $1 AND attnum > 0 AND attisdropped
		ORDER BY attnum
	) T
) T
WHERE
	array_upper(dropped_columns, 1) > 0
$$
LANGUAGE sql STABLE STRICT;

-- Get a comma-separated storage paramter for the table including
-- paramters for the corresponding TOAST table.
-- Note that since oid setting is always not NULL, this function
-- never returns NULL
CREATE FUNCTION dbms_redefinition.get_storage_param(oid)
  RETURNS TEXT AS
$$
SELECT string_agg(param, ', ')
FROM (
    -- table storage parameter
    SELECT unnest(reloptions) as param
    FROM pg_class
    WHERE oid = $1
    UNION ALL
    -- TOAST table storage parameter
    SELECT ('toast.' || unnest(reloptions)) as param
    FROM (
        SELECT reltoastrelid from pg_class where oid = $1
         ) as t,
        pg_class as c
    WHERE c.oid = t.reltoastrelid
    UNION ALL
    -- table oid
    SELECT 'oids = ' ||
        CASE WHEN false
            THEN 'true'
            ELSE 'false'
        END
    FROM pg_class
    WHERE oid = $1

    ) as t
$$
LANGUAGE sql STABLE STRICT;

-- GET a SQL text to set column storage option for the table.
CREATE FUNCTION dbms_redefinition.get_alter_col_storage(oid)
  RETURNS text AS
$$
 SELECT 'ALTER TABLE migrate.table_' || $1 || array_to_string(column_storage, ',')
 FROM (
       SELECT
         array_agg(' ALTER ' || quote_ident(attname) ||
          CASE attstorage
               WHEN 'p' THEN ' SET STORAGE PLAIN'
               WHEN 'm' THEN ' SET STORAGE MAIN'
               WHEN 'e' THEN ' SET STORAGE EXTERNAL'
               WHEN 'x' THEN ' SET STORAGE EXTENDED'
          END) AS column_storage
       FROM (
            SELECT *
            FROM pg_attribute a
                 JOIN pg_type t on t.oid = atttypid
                 JOIN pg_class r on r.oid = a.attrelid
                 JOIN pg_namespace s on s.oid = r.relnamespace
            WHERE typstorage <> attstorage
                 AND attrelid = $1
                 AND attnum > 0
                 AND NOT attisdropped
           ORDER BY attnum
	   ) T
      ) T
WHERE array_upper(column_storage , 1) > 0
$$
LANGUAGE sql STABLE STRICT;

-- GET a SQL text to create a table with defaults and not null settings.
-- new_table_name can include a prefixed schema separated by a period.
CREATE FUNCTION dbms_redefinition.get_create_table_statement(target_schema varchar, target_table_name varchar, new_table_name varchar, tablespace varchar)
  RETURNS text AS
$$
DECLARE
    table_ddl   text;
    target_oid    oid;
    column_record record;
BEGIN
    FOR column_record IN
        SELECT
            row_number() OVER () as row_number,
            b.oid as table_oid,
            quote_ident(b.nspname) as schema_name,
            quote_ident(b.relname) as table_name,
            quote_ident(a.attname) as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
            CASE WHEN
                (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                 FROM pg_catalog.pg_attrdef d
                 WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) IS NOT NULL THEN
                'DEFAULT ' ||  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
                              FROM pg_catalog.pg_attrdef d
                              WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef)
            ELSE
                ''
            END as column_default_value,
            CASE WHEN a.attnotnull = true THEN
                'NOT NULL'
            ELSE
                'NULL'
            END as column_not_null,
            a.attnum as attnum,
            e.max_attnum as max_attnum
        FROM
            pg_catalog.pg_attribute a
            INNER JOIN
             (SELECT c.oid,
                n.nspname,
                c.relname
              FROM pg_catalog.pg_class c
                   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relname ~ ('^(' || target_table_name || ')$')
                AND n.nspname = target_schema
                AND pg_catalog.pg_table_is_visible(c.oid)
              ORDER BY 2, 3) b
            ON a.attrelid = b.oid
            INNER JOIN
             (SELECT
                  a.attrelid,
                  max(a.attnum) as max_attnum
              FROM pg_catalog.pg_attribute a
              WHERE a.attnum > 0
                AND NOT a.attisdropped
              GROUP BY a.attrelid) e
            ON a.attrelid=e.attrelid
        WHERE a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF column_record.row_number = 1 THEN
            target_oid := column_record.table_oid;
            table_ddl := 'CREATE TABLE ' || new_table_name || ' (';
        ELSE
            table_ddl := table_ddl || ',';
        END IF;

        IF column_record.attnum <= column_record.max_attnum THEN
            table_ddl := table_ddl || chr(10) ||
                     '    ' || column_record.column_name || ' ' || column_record.column_type || ' ' || column_record.column_default_value || ' ' || column_record.column_not_null;
        END IF;
    END LOOP;

    table_ddl := table_ddl || ') ' || ' WITH (' || migrate.get_storage_param(target_oid) || ') TABLESPACE ' || tablespace || ';';
    RETURN table_ddl;
END;
$$
LANGUAGE 'plpgsql' COST 100.0 SECURITY INVOKER;

-- includes not only PRIMARY KEYS but also UNIQUE NOT NULL keys
CREATE VIEW dbms_redefinition.primary_keys AS
  SELECT indrelid, min(indexrelid) AS indexrelid
    FROM (SELECT indrelid, indexrelid FROM pg_index
   WHERE indisunique
     AND indisvalid
     AND indpred IS NULL
     AND 0 <> ALL(indkey)
     AND NOT EXISTS(
           SELECT 1 FROM pg_attribute
            WHERE attrelid = indrelid
              AND attnum = ANY(indkey)
              AND NOT attnotnull)
   ORDER BY indrelid, indisprimary DESC, indnatts, indkey) tmp
   GROUP BY indrelid;

CREATE VIEW dbms_redefinition.tables AS
  SELECT migrate.oid2text(R.oid) AS relname,
         R.oid AS relid,
         R.reltoastrelid AS reltoastrelid,
         CASE WHEN R.reltoastrelid = 0 THEN 0 ELSE (
            SELECT indexrelid FROM pg_index
            WHERE indrelid = R.reltoastrelid
            AND indisvalid) END AS reltoastidxid,
         N.nspname AS schemaname,
         PK.indexrelid AS pkid,
         CK.indexrelid AS ckid,
         migrate.get_create_index_type(PK.indexrelid, 'migrate.pk_' || R.oid) AS create_pktype,
         'CREATE TABLE migrate.log_' || R.oid || ' (id bigserial PRIMARY KEY, pk migrate.pk_' || R.oid || ', row ' || migrate.oid2text(R.oid) || ')' AS create_log,
         migrate.get_create_trigger(R.oid, PK.indexrelid) AS create_trigger,
         migrate.get_enable_trigger(R.oid) as enable_trigger,
         'CREATE TABLE migrate.table_' || R.oid || ' WITH (' || migrate.get_storage_param(R.oid) || ') TABLESPACE '  AS create_table_1,
         coalesce(quote_ident(S.spcname), 'pg_default') as tablespace_orig,
         ' AS SELECT ' || migrate.get_columns_for_create_as(R.oid) || ' FROM ONLY ' || migrate.oid2text(R.oid) AS create_table_2,
         'INSERT INTO migrate.table_' || R.oid || ' SELECT ' || migrate.get_columns_for_insert(R.oid) || ' FROM ONLY ' || migrate.oid2text(R.oid) AS copy_data,
         migrate.get_alter_col_storage(R.oid) AS alter_col_storage,
         migrate.get_drop_columns(R.oid, 'migrate.table_' || R.oid) AS drop_columns,
         'DELETE FROM migrate.log_' || R.oid AS delete_log,
         'LOCK TABLE ' || migrate.oid2text(R.oid) || ' IN ACCESS EXCLUSIVE MODE' AS lock_table,
         migrate.get_order_by(CK.indexrelid, R.oid) AS ckey,
         'SELECT * FROM migrate.log_' || R.oid || ' ORDER BY id LIMIT $1' AS sql_peek,
         'INSERT INTO migrate.table_' || R.oid || ' VALUES ($1.*)' AS sql_insert,
         'DELETE FROM migrate.table_' || R.oid || ' WHERE ' || migrate.get_compare_pkey(PK.indexrelid, '$1') AS sql_delete,
         'UPDATE migrate.table_' || R.oid || ' SET ' || migrate.get_assign(R.oid, '$2') || ' WHERE ' || migrate.get_compare_pkey(PK.indexrelid, '$1') AS sql_update,
         'DELETE FROM migrate.log_' || R.oid || ' WHERE id IN (' AS sql_pop
    FROM pg_class R
         LEFT JOIN pg_class T ON R.reltoastrelid = T.oid
         LEFT JOIN migrate.primary_keys PK
                ON R.oid = PK.indrelid
         LEFT JOIN (SELECT CKI.* FROM pg_index CKI, pg_class CKT
                     WHERE CKI.indisvalid
                       AND CKI.indexrelid = CKT.oid
                       AND CKI.indisclustered
                       AND CKT.relam = 403) CK
                ON R.oid = CK.indrelid
         LEFT JOIN pg_namespace N ON N.oid = R.relnamespace
         LEFT JOIN pg_tablespace S ON S.oid = R.reltablespace
   WHERE R.relkind = 'r'
     AND R.relpersistence = 'p'
     AND N.nspname NOT IN ('pg_catalog', 'information_schema')
     AND N.nspname NOT LIKE E'pg\\_temp\\_%';

CREATE FUNCTION dbms_redefinition.migrate_indexdef(oid, oid, name, bool, text) RETURNS text AS
'MODULE_PATHNAME', 'migrate_indexdef'
LANGUAGE C STABLE;

CREATE FUNCTION dbms_redefinition.migrate_trigger() RETURNS trigger AS
'MODULE_PATHNAME', 'migrate_trigger'
LANGUAGE C VOLATILE STRICT SECURITY DEFINER;

CREATE FUNCTION dbms_redefinition.conflicted_triggers(oid) RETURNS SETOF name AS
$$
SELECT tgname FROM pg_trigger
 WHERE tgrelid = $1 AND tgname = 'migrate_trigger'
 ORDER BY tgname;
$$
LANGUAGE sql STABLE STRICT;

CREATE FUNCTION dbms_redefinition.disable_autovacuum(regclass) RETURNS void AS
'MODULE_PATHNAME', 'migrate_disable_autovacuum'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION dbms_redefinition.reset_autovacuum(regclass) RETURNS void AS
'MODULE_PATHNAME', 'migrate_reset_autovacuum'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION dbms_redefinition.migrate_apply(
  sql_peek      cstring,
  sql_insert    cstring,
  sql_delete    cstring,
  sql_update    cstring,
  sql_pop       cstring,
  count         integer)
RETURNS integer AS
'MODULE_PATHNAME', 'migrate_apply'
LANGUAGE C VOLATILE;

CREATE FUNCTION dbms_redefinition.migrate_swap(oid) RETURNS void AS
'MODULE_PATHNAME', 'migrate_swap'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION dbms_redefinition.migrate_drop(oid, int) RETURNS void AS
'MODULE_PATHNAME', 'migrate_drop'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION dbms_redefinition.migrate_index_swap(oid) RETURNS void AS
'MODULE_PATHNAME', 'migrate_index_swap'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION dbms_redefinition.get_table_and_inheritors(regclass) RETURNS regclass[] AS
'MODULE_PATHNAME', 'migrate_get_table_and_inheritors'
LANGUAGE C STABLE STRICT;
