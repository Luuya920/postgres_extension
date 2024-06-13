#include "postgres.h"
#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"
#include "executor/spi.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(average_value);

Datum average_value(PG_FUNCTION_ARGS)
{
    int32 sensor_id = PG_GETARG_INT32(0);
    TimestampTz start_time = PG_GETARG_TIMESTAMPTZ(1);
    TimestampTz end_time = PG_GETARG_TIMESTAMPTZ(2);
    double result = 0.0;

    char query[256];
    snprintf(query, sizeof(query),
             "SELECT AVG(value) FROM timeseries_data WHERE source_id = %d AND timestamp BETWEEN '%s' AND '%s'",
             sensor_id, timestamptz_to_str(start_time), timestamptz_to_str(end_time));

    SPI_connect();
    SPI_execute(query, true, 0);
    if (SPI_processed > 0 && SPI_tuptable != NULL)
    {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        HeapTuple tuple = tuptable->vals[0];
        bool isnull;
        Datum avg_value = SPI_getbinval(tuple, tupdesc, 1, &isnull);

        if (!isnull)
            result = DatumGetFloat8(avg_value);
    }
    SPI_finish();

    PG_RETURN_FLOAT8(result);
}

PG_FUNCTION_INFO_V1(add_hundred);

Datum add_hundred(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);

    PG_RETURN_INT32(arg + 100);
}

PG_FUNCTION_INFO_V1(generate_driving_periods);

Datum generate_driving_periods(PG_FUNCTION_ARGS)
{
    Jsonb *jb = PG_GETARG_JSONB_P(0);
    int32 partition = PG_GETARG_INT64(1);
    JsonbIterator *it;
    JsonbValue v;
    JsonbIteratorToken r;
    StringInfo result = makeStringInfo();

    char *current_driver = NULL;
    int64 start_timestamp = 0;
    bool first_entry = true;

    it = JsonbIteratorInit(&jb->root);
    while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {

        if (r == WJB_BEGIN_ARRAY)
        {
            while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_END_ARRAY)
            {
                if (r == WJB_BEGIN_OBJECT)
                {
                    char *driver_name = NULL;
                    int64 timestamp = 0;
                    while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_END_OBJECT)
                    {
                        if (r == WJB_KEY)
                        {
                            char *key = pnstrdup(v.val.string.val, v.val.string.len);

                            r = JsonbIteratorNext(&it, &v, false);
                            if (strcmp(key, "driver_name") == 0)
                            {
                                if (v.type == jbvString)
                                {
                                    driver_name = pnstrdup(v.val.string.val, v.val.string.len);
                                }
                            }
                            else if (strcmp(key, "timestamp") == 0)
                            {
                                if (v.type == jbvNumeric)
                                {
                                    timestamp = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v.val.numeric)));
                                }
                            }
                            pfree(key);
                        }
                    }
                    if (first_entry)
                    {
                        current_driver = driver_name;
                        start_timestamp = timestamp;
                        first_entry = false;
                    }
                    else if (current_driver && driver_name && strcmp(current_driver, driver_name) != 0)
                    {
                        appendStringInfo(result, "Driver changed from %s to %s at timestamp %ld\n", current_driver, driver_name, timestamp);
                        current_driver = driver_name;
                        start_timestamp = timestamp;
                    }
                    else if (driver_name)
                    {
                        pfree(driver_name);
                    }

                    while (timestamp > start_timestamp)
                    {
                        appendStringInfo(result, "Driver %s drove at %ld\n", current_driver, start_timestamp);
                        start_timestamp += partition;
                    }
                }
            }
        }
    }
    if (current_driver)
    {
        appendStringInfo(result, "Driver %s drove at %ld\n", current_driver, start_timestamp);
    }

    PG_RETURN_TEXT_P(cstring_to_text(result->data));
}