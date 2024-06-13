-- my_extension--1.0.sql
CREATE FUNCTION average_value(sensor_id INT, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ) RETURNS FLOAT8
AS 'MODULE_PATHNAME', 'average_value'
LANGUAGE C STRICT;

CREATE FUNCTION add_hundred(arg INT) RETURNS INT
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'add_hundred';

CREATE FUNCTION generate_driving_periods(data JSONB, partition INT) RETURNS TEXT
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'generate_driving_periods';