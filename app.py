from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("MyApp").getOrCreate()
# 'lpep_pickup_datetime', 'lpep_dropoff_datetime'
df_green = (
    spark.read.option("delimiter", ",")
    .csv(
        "./dataset/green*.csv",
        header=True,
    )
    .select("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance")
)

df_yellow = (
    spark.read.option("delimiter", ",")
    .csv(
        "./dataset/yellow*.csv",
        header=True,
    )
    .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance")
)

df_green = df_green.withColumnRenamed("lpep_pickup_datetime", "pickup_time").withColumnRenamed(
    "lpep_dropoff_datetime", "drop_time"
)
df_yellow = df_yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_time").withColumnRenamed(
    "tpep_dropoff_datetime", "drop_time"
)
# print(df_green.count(), df_yellow.count())


df = df_green.union(df_yellow)
df.show(10)

# print(df.count())
