# dbms_redefinition -- DO ONLINE modify Halo/PostgreSQL table (avoiding long locks)

- Project Page: https://github.com/HaloLab001/dbms_redefinition


## About

dbms_redefinition is a Halo/PostgreSQL extension which lets you make schema
changes to tables and indexes. Unlike `ALTER TABLE`, it works online, without
holding a long lived exclusive lock on the processed tables during the
migration. It builds a copy of the target table and swaps them.


The work based on the excellent pg_migrate (https://github.com/phillbaker/pg_migrate) &
pg_repack project (https://reorg.github.io/pg_repack).

## Supported Postgres Versions

Halo 14, 15, 16; PostgreSQL 14,15,16

## Installation

TODO

## Examples

TODO

## Known Limitations

TODO

