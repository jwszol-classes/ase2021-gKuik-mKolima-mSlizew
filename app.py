from pyspark.sql import SparkSession, functions as F, types

def printResults(df):
    print(df.groupBy().avg("Velocity").collect()[0])
    print(df.agg({"Velocity": "max"}).collect()[0])


def init():
    spark = SparkSession.builder.master("local").appName("MyApp").getOrCreate()

    df_green = (
        spark.read.option("delimiter", ",")
        .csv(
            "./dataset/green_tripdata_2020-04.csv",
            header=True,
        )
        .select("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance")
    )
    df_green = df_green.withColumnRenamed("lpep_pickup_datetime", "pickup_time").withColumnRenamed(
        "lpep_dropoff_datetime", "drop_time"
    )

    df_yellow = (
        spark.read.option("delimiter", ",")
        .csv(
            "./dataset/yellow*.csv",
            header=True,
        )
        .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance")
    )
    df_yellow = df_yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_time").withColumnRenamed(
        "tpep_dropoff_datetime", "drop_time"
    )

    df = df_green.union(df_yellow)


    time_diff = (F.unix_timestamp("drop_time") - F.unix_timestamp("pickup_time")).cast(types.DoubleType()) / 3600

    df = df.withColumn("Duration", time_diff)

    velocity = (F.col("trip_distance").cast(types.DoubleType()) / F.col("Duration"))
    df = df.withColumn("Velocity", velocity)

    printResults(df)

if __name__ == "__main__":
    init()