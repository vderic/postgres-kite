/* contrib/postgres_fdw/postgres_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION kite_fdw" to load this file. \quit

CREATE FUNCTION kite_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION kite_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER kite_fdw
  HANDLER kite_fdw_handler
  VALIDATOR kite_fdw_validator;
