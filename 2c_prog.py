from pyspark.sql import SparkSession, functions as F, types
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import matplotlib.dates as mdates
import datetime

def df_filter(df, max_velocity = 300, date_from="2019-05-01", date_to = "2020-05-31"):
    f = df.filter(df.trip_distance > 0).filter(df.duration > 0)
    df = df.filter(df.velocity > 0).filter(df.velocity < max_velocity)
    df = df.filter(df.pickup_time >= date_from).filter(df.pickup_time <= date_to)
    return df

paths = ["s3://nyc-tlc/trip data/g*19-0[5-9].csv",
         "s3://nyc-tlc/trip data/g*19-[1-2][0-9].csv", 
         "s3://nyc-tlc/trip data/g*20-0[1-5].csv"]

df_green = (
    spark.read.option("delimiter", ",")
    .csv(
        paths,
        header=True,
    )
    .select("lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance")
)

paths = ["s3://nyc-tlc/trip data/y*19-0[5-9].csv",
         "s3://nyc-tlc/trip data/y*19-[1-2][0-9].csv", 
         "s3://nyc-tlc/trip data/y*20-0[1-5].csv"]

df_yellow = (
    spark.read.option("delimiter", ",")
    .csv(
        paths,
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

df_yellow = df_yellow.withColumn('color', F.lit('yellow'))
df_green = df_green.withColumn('color', F.lit('green'))
df = df_green.union(df_yellow)

time_diff = (F.unix_timestamp("drop_time") - F.unix_timestamp("pickup_time")).cast(types.DoubleType()) / 3600
df = df.withColumn("duration", time_diff)

velocity = (F.col("trip_distance").cast(types.DoubleType()) / F.col("Duration"))
df = df.withColumn("velocity", velocity)
df = df_filter(df)

avg_velocity_over_year = df.groupBy().avg("Velocity").collect()[0]
print(f"Avg velocity over the year: \n{avg_velocity_over_year}\n")

df_by_month  = (df
.groupBy([df['color'],F.year("pickup_time").alias('year'),F.month("pickup_time").alias("month")])
.agg(F.avg("velocity").alias('avg_velocity')))

n = df_by_month.count()
rows = df_by_month.take(n)

dates_yellow,dates_green = [],[]
means_yellow, means_green = [],[]

for row in rows:
    if row.color == 'yellow':
        dates_yellow.append(datetime.datetime(row.year, row.month,1))
        means_yellow.append(row.avg_velocity)
    else:
        dates_green.append(datetime.datetime(row.year, row.month,1))
        means_green.append(row.avg_velocity)


fig,axs = plt.subplots(2)
dates_yellow = date2num(dates_yellow)
dates_green = date2num(dates_green)
axs[0].bar(dates_yellow-6, means_yellow, width=12, color='y', align='center',  label = 'avg velocity per month- yellow taxi')
axs[0].bar(dates_green+6, means_green, width=12, color='g', align='center',  label = 'avg velocity per month - green taxi')
myFmt = mdates.DateFormatter('%y%y - %b')
axs[0].xaxis.set_major_formatter(myFmt)
axs[0].legend()

df_by_day = df.withColumn("basic_date", F.to_date(F.col("pickup_time")))
df_by_day = (df_by_day
    .groupBy([df_by_day['color'],df_by_day['basic_date']])
    .agg(F.avg("velocity").alias('avg_velocity')))

n = df_by_day.count()
rows = df_by_day.take(n)

yellow_data, green_data = [],[]

for row in rows:
    if row.color == 'yellow':
        yellow_data.append((row.basic_date,row.avg_velocity))
    else:
        green_data.append((row.basic_date,row.avg_velocity))

yellow_data = sorted(yellow_data, key = lambda x: x[0])
green_data = sorted(green_data, key = lambda x: x[0])
dates_yellow, means_yellow = list(zip(*yellow_data))
dates_green, means_green = list(zip(*green_data))
axs[1].plot(dates_yellow, means_yellow, 'y', label = 'avg velocity per day - yellow taxi')
axs[1].plot(dates_green, means_green, 'g', label = 'avg velocity per day - green taxi')
axs[1].legend()
plt.xticks(rotation=20)
plt.plot()