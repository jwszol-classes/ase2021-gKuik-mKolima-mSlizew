from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("MyApp").getOrCreate()
# 'lpep_pickup_datetime', 'lpep_dropoff_datetime'
df = spark.read.option("delimiter", ',').csv('./*.csv', header=True,).select('','trip_distance')
print(df.count())
df.show(5)