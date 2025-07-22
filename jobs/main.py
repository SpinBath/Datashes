import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession
from src import config
from src.parser import parse_log_line
from src.transforms import clean_data
from src.analytics import hourly_activity, count_unique_hosts, count_errors_404, top_endpoints, daily_traffic, count_errors
from src.graphs import graph_endpoints, graph_dailyTraffic

def main():

    file_size = os.path.getsize(config.LOG_FILE_PATH) / (1024 * 1024)

    if(file_size > config.FILE_LIMIT_SIZE):
        print(f"⚠️  El archivo es demasiado grande: {file_size:.2f} MB")
        print(f"⚠️  El limite de tamaño actual es de {config.FILE_LIMIT_SIZE} MB")
        exit(1)

    spark = SparkSession.builder.appName("DataAshes").getOrCreate()

    df_raw = spark.read.text(config.LOG_FILE_PATH)
    df_parsed = parse_log_line(df_raw, config.LOG_REGEX)
    
    df_clean = clean_data(df_parsed, config.TIMESTAMP_FORMAT)

    graph_dailyTraffic(df_clean)

    spark.stop()

if __name__ == "__main__": 
    main()
