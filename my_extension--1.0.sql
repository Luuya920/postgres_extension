-- my_extension--1.0.sql
CREATE FUNCTION add_hundred(arg INT) RETURNS INT
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'add_hundred';

CREATE FUNCTION generate_driving_periods(data JSONB, partition INT) RETURNS TEXT
LANGUAGE c STRICT
AS 'MODULE_PATHNAME', 'generate_driving_periods';


CREATE FUNCTION avg_speed_transfn_optimized(state internal, speeds double precision[])
RETURNS internal
AS 'MODULE_PATHNAME', 'avg_speed_transfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_finalfn_optimized(state internal, speeds double precision[])
RETURNS float8
AS 'MODULE_PATHNAME', 'avg_speed_finalfn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_combinefn_optimized(state1 internal, state2 internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'avg_speed_combinefn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_serializefn(state internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'avg_speed_serializefn'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_speed_deserializefn(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'avg_speed_deserializefn'
LANGUAGE C IMMUTABLE;

CREATE AGGREGATE avg_speed_optimized(double precision[]) (
    SFUNC = avg_speed_transfn_optimized,
    STYPE = internal,
    FINALFUNC = avg_speed_finalfn_optimized,
    FINALFUNC_EXTRA,
    COMBINEFUNC = avg_speed_combinefn_optimized,
    SERIALFUNC = avg_speed_serializefn,
    DESERIALFUNC = avg_speed_deserializefn,
    PARALLEL = SAFE
);



CREATE OR REPLACE FUNCTION avg_speed_transfn(state DOUBLE PRECISION[], speeds DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION[] AS $$
DECLARE
    i INT;
BEGIN
    IF state IS NULL THEN
        state := ARRAY[0.0, 0.0];  -- state[1] is sum, state[2] is count
    END IF;

    IF speeds IS NOT NULL THEN
        FOR i IN 1..array_length(speeds, 1) LOOP
            state[1] := state[1] + speeds[i];
            state[2] := state[2] + 1;
        END LOOP;
    END IF;

    RETURN state;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

CREATE OR REPLACE FUNCTION avg_speed_finalfn(state DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
BEGIN
    IF state IS NULL OR state[2] = 0 THEN
        RETURN NULL;
    END IF;
    RETURN state[1] / state[2];
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;

CREATE OR REPLACE FUNCTION avg_speed_combine(state1 DOUBLE PRECISION[], state2 DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION[]  AS $$
BEGIN
    IF state1 IS NULL THEN
        RETURN state2;
    ELSIF state2 IS NULL THEN
        RETURN state1;
    ELSE
        state1[1] := state1[1] + state2[1];
        state1[2] := state1[2] + state2[2];
        RETURN state1;
    END IF;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE;


CREATE AGGREGATE avg_speed_array(DOUBLE PRECISION[]) (
    SFUNC = avg_speed_transfn,
    STYPE = DOUBLE PRECISION[],
    FINALFUNC = avg_speed_finalfn,
    COMBINEFUNC = avg_speed_combine,
    PARALLEL = safe,
    INITCOND = '{0.0, 0.0}'
);



CREATE FUNCTION avg_transfn(state internal, value float8) RETURNS internal
    AS 'MODULE_PATHNAME', 'avg_transfn'
    LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_combinefn(state1 internal, state2 internal) RETURNS internal
    AS 'MODULE_PATHNAME', 'avg_combinefn'
    LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_finalfn(state internal) RETURNS float8
    AS 'MODULE_PATHNAME', 'avg_finalfn'
    LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_serializefn(state internal) RETURNS bytea
    AS 'MODULE_PATHNAME', 'avg_serializefn'
    LANGUAGE C IMMUTABLE;

CREATE FUNCTION avg_deserializefn(bytea, internal) RETURNS internal
    AS 'MODULE_PATHNAME', 'avg_deserializefn'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE average(float8) (
    SFUNC = avg_transfn,
    STYPE = internal,
    COMBINEFUNC = avg_combinefn,
    FINALFUNC = avg_finalfn,
    SERIALFUNC = avg_serializefn,
    DESERIALFUNC = avg_deserializefn,
    PARALLEL = safe
);




CREATE OR REPLACE FUNCTION vertical_agg(
    truck_type text, 
    process_field text, 
    process_value numeric, 
    group_by_fields text[]
) 
RETURNS TABLE (
    grouping_values jsonb,
    avg_speed numeric
) AS $$
DECLARE
    sql_query text;
    group_by_clause text;
    select_clause text;
    jsonb_keys text;
BEGIN
    group_by_clause := array_to_string(array(
        SELECT format('jsonb_element->>%L', field) FROM unnest(group_by_fields) AS field
    ), ', ');

    jsonb_keys := array_to_string(array(
        SELECT format('''%s'', jsonb_element->>%L', field, field) FROM unnest(group_by_fields) AS field
    ), ', ');

    sql_query := format(
        'SELECT 
            jsonb_build_object(%s) AS grouping_values,
            AVG((jsonb_element->>''speed'')::numeric) AS avg_speed
         FROM (
             SELECT unnest(data) AS jsonb_element
             FROM truck_data
         ) AS elements
         WHERE jsonb_element->>''type'' = %s
           AND (jsonb_element->''process''->>%L)::numeric = %s
         GROUP BY %s
         ORDER BY %s',
        jsonb_keys, quote_literal(truck_type), process_field, process_value, group_by_clause, group_by_clause
    );
    
    RETURN QUERY EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION horizontal_agg(
    truck_type text, 
    driver_names text[], 
    process_field text,
    agg_function text
) 
RETURNS TABLE(driver_name text, process_value text, result numeric) AS $$
DECLARE
    sql_query text;
BEGIN
    sql_query := format(
        'SELECT 
            jsonb_element->>''driver_name'' AS driver_name,
            jsonb_element->''process''->>%L AS process_value,
            %s((jsonb_element->>''speed'')::numeric) AS result
         FROM (
             SELECT unnest(data) AS jsonb_element
             FROM truck_data
         ) AS elements
         WHERE jsonb_element->>''type'' = %s
           AND jsonb_element->>''driver_name'' = ANY (ARRAY[%s])
         GROUP BY jsonb_element->>''driver_name'', jsonb_element->''process''->>%L 
         ORDER BY jsonb_element->>''driver_name'', jsonb_element->''process''->>%L',
        process_field, 
        agg_function, 
        quote_literal(truck_type), 
        array_to_string(array(
            SELECT quote_literal(name) FROM unnest(driver_names) AS name
        ), ', '), 
        process_field, 
        process_field
    );
    
    RETURN QUERY EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;
