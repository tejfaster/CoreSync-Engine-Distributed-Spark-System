import os
import sys
from pyspark.sql.functions import avg, sum as _sum, col,desc, lit, rank
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"../..")))
from src.spark.jobs import start_spark

def main():
    print("Gold Job:Aggregation layer")

    region = sys.argv[1]
    load = int(sys.argv[2])
    cores = int(sys.argv[3])
    print("region_gold",region)
    # print("load_gold",load)
    print("cores_gold",cores)

    # spark 
    spark = start_spark(cores, region)

    df = spark.read.parquet(f"data/silver/region={region}")
    df = df.withColumn("region",lit(region))
    

    if df.rdd.isEmpty():
        print("No data in silver layer")
        return
    
    # aggregation
    
    window = Window.partitionBy("symbol").orderBy(df["price"].desc())

    df = df.withColumn("rank",rank().over(window))
    result = df.groupBy("symbol").agg(
        avg("price").alias("avg_price"),
        _sum("volume").alias("total_volume")
    )
   
    # top 10 by price
    top_price = df.orderBy(desc("price")).limit(10)

    # top 10 by volume
    top_volume = df.orderBy(desc("volume")).limit(10)

    # write results
    top_price.write.mode("overwrite").parquet(f"data/gold/top_price/{region}/")
    top_volume.write.mode("overwrite").parquet(f"data/gold/top_volume/{region}/")
    result.write.mode("overwrite").parquet(f"data/gold/agg_metrics/{region}/")

    print("Aggregation completed")

    spark.stop()

if __name__ == "__main__":
    main()    
