from pyspark.sql import SparkSession

def start_spark(cores,region):
    cores = max(1,cores)
    
    spark = SparkSession.builder \
        .appName(region)\
        .master(f"local[{cores}]") \
        .config("spark.sql.shuffle.partitions",cores * 2) \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    return spark