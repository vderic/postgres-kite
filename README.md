Kite Foreign Data Wrapper for PostgreSQL
=========================================

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
Kite.

Installation
------------

To compile the Kite foreign data wrapper,

1. To build on POSIX-compliant systems you need to ensure the
   `pg_config` executable is in your path when you run `make`. This
   executable is typically in your PostgreSQL installation's `bin`
   directory. For example:

    ```
    $ export PATH=/usr/local/pgsql/bin/:$PATH
    ```

2. Compile the code using make.

    ```
    $ make USE_PGXS=1
    ```

3.  Finally install the foreign data wrapper.

    ```
    $ make USE_PGXS=1 install
    ```

Usage
-----

The following parameters can be set on a Kite foreign server object:

  * `host`: List of addresses or hostname:port of the Kite servers. Defaults to
    `127.0.0.1:7878,127.0.0.2:7878`
  * `dbname`: Name of the Postgres database to query. This is a mandatory
    option.
  * `fragcnt`: The number of fragment per query
  * `fetch_size`: This option specifies the number of rows kite_fdw should
    get in each fetch operation. It can be specified for a foreign table or
    a foreign server. The option specified on a table overrides an option
    specified for the server. The default is `100`.

The following parameters can be set on a Kite foreign table object:

  * `schema_name`: Name of the Postgres schema.
  * `table_name`: Name of the Postgres table, default is the same as
    foreign table.
  * `fetch_size`: Same as `fetch_size` parameter for foreign server.

The following parameters need to supplied while creating user mapping.

  * `username`: Username to use when connecting to Postgres.
  * `password`: Password to authenticate to the Postgres server with.

Examples
--------

```sql
-- load extension first time after install
CREATE EXTENSION kite_fdw;

-- create server object
CREATE SERVER kite_server
	FOREIGN DATA WRAPPER kite_fdw
	OPTIONS (host '127.0.0.1:7878', dbname 'pgsql', fragcnt '4');

-- create user mapping
CREATE USER MAPPING FOR pgsql
	SERVER kite_server
	OPTIONS (username 'foo', password 'bar');

-- create foreign table
CREATE FOREIGN TABLE warehouse
	(
		warehouse_id int,
		warehouse_name text,
		warehouse_created timestamp
	)
	SERVER kite_server
	OPTIONS (schema_name 'public', table_name 'warehouse*');

-- select from table
SELECT * FROM warehouse ORDER BY 1;

warehouse_id | warehouse_name | warehouse_created
-------------+----------------+-------------------
           1 | UPS            | 10-JUL-20 00:00:00
           2 | TV             | 10-JUL-20 00:00:00
           3 | Table          | 10-JUL-20 00:00:00

