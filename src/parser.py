from pyspark.sql.functions import regexp_extract

def parse_log_line(df, regex):
    return df.select(
        regexp_extract("value", regex, 1).alias("host"),
        regexp_extract("value", regex, 2).alias("timestamp"),
        regexp_extract("value", regex, 3).alias("method"),
        regexp_extract("value", regex, 4).alias("endpoint"),
        regexp_extract("value", regex, 5).alias("status"),
        regexp_extract("value", regex, 6).alias("content_size")
    )
