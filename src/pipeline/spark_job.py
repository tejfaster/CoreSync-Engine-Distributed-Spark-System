from pyspark.sql import SparkSession

def start_spark(cores,region):
    
    sc = SparkSession.builder \
        .appName(region)\
        .master(f"local[{cores}]") \
        .config("spark.sql.shuffle.partitions",cores * 2) \
        .getOrCreate()

    return sc