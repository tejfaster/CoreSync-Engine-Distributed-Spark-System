import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"../..")))
from src.queue.offset_manager import update_offset
from src.queue.reader import read_new_batch


def start_spark(cores,region):
    cores = max(1,cores)
    
    spark = SparkSession.builder \
        .appName(region)\
        .master(f"local[{cores}]") \
        .config("spark.sql.shuffle.partitions",cores * 2) \
        .getOrCreate()

    return spark

def main():

    region = sys.argv[1]
    load = int(sys.argv[2])
    cores = int(sys.argv[3])

    topic = region

    print(f"{region.upper()} JOB START")
    print("Load:",load)
    print("Cores:",cores)

    # Read data(offset)
    data ,offset = read_new_batch(topic)

    data = [item for sublist in data for item in sublist]

    if not data:
        print("No new data")
        return 
    
    print("Total new data:",len(data))

    # workload 
    batch = data[:load]

    processed_count = len(batch)

    print("Processing:",processed_count)

    # update offset
    update_offset(topic,offset + processed_count)

    # spark 
    spark = start_spark(cores,region)

    df = spark.createDataFrame(batch)

    print("Schema:")
    df.printSchema()

    # processing 
    df = df.withColumn("Processed_at",current_timestamp())

    # Repartition(simulate cores)
    df = df.repartition(max(1,cores * 2))

    print("Partitions:",df.rdd.getNumPartitions())

    # action
    print("Processed Count rows:",df.count())

    spark.stop()

    print(f"{region.upper()} JOB END\n")

if __name__ == "__main__":
    main()