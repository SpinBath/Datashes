from pyspark.sql.functions import col, to_date, try_to_timestamp, when, expr

def clean_data(df, timestamp_format):
    df = df.withColumn("status", when(col("status").rlike("^[0-9]+$"), col("status").cast("integer")).otherwise(None))
    df = df.withColumn("content_size", when(col("content_size").rlike("^[0-9]+$"), col("content_size").cast("integer")).otherwise(None))
    df = df.withColumn("date", expr(f"to_date(try_to_timestamp(timestamp, '{timestamp_format}'))"))

    """ df = df.filter(col("date").isNotNull()) """


    return df
