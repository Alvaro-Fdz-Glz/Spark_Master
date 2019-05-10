spark2-shell -- conf spark.ui.port=10018
import spark.implicits._
val data = spark.read.format("csv").option("header", "true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
// data.show()
// data.printSchema
// .data.count


// val data2 = spark.read.format("csv").
// option("header", "true").
// option("inferSchema", "true").
// load("/YellowCab/2017/yellow_tripdata_2017-01.csv")


import org.apache.spark.sql.types._

val customSchema = StructType(Array(
        StructField("VendorID", StringType, true),
        StructField("tpep_pickup_datetime", TimestampType, true),
        StructField("tpep_dropoff_datetime",TimestampType, true),
        StructField("passenger_count", IntegerType, true),
        StructField("trip_distance", DoubleType, true),
        StructField("RatecodeID", StringType, true),
        StructField("store_and_fwd_flag", StringType, true),
        StructField("PULocationID", IntegerType, true),
        StructField("DOLocationID", IntegerType, true),
        StructField("payment_type", IntegerType, true),
        StructField("fare_amount", DoubleType, true),
        StructField("extra", DoubleType, true),
        StructField("mta_tax", DoubleType, true),
        StructField("tip_amount", DoubleType, true),
        StructField("tolls_amount", DoubleType, true),
        StructField("improvement_surcharge", DoubleType, true),
        StructField("total_amount", DoubleType, true)
)
)


val data = spark.read.format("csv").
option("header", "true").
schema(customSchema).
load("/YellowCab/2017/yellow_tripdata_2017-01.csv")

val data3 = data.withColumn("pickup_seconds", unix_timestamp($"tpep_pickup_datetime")).
withColumn("dropoff_seconds", unix_timestamp($"tpep_dropoff_datetime")).
withColumn("duration_sec", $"dropoff_seconds"-$"pickup_seconds").
withColumn("duration_hour", $"duration_sec" /3600).
withColumn("speed", $"trip_distance" / $"duration_hour" ).
select("tpep_pickup_datetime","tpep_dropoff_datetime","duration_sec","duration_hour", "trip_distance", "speed", "RatecodeID")


// data3.map(x => x.getDouble(4))

val dist_tot = data3.map(x => x.getDouble(4)).reduce(_+_)
val time_tot = data3.map(x => x.getLong(2)).reduce(_+_)
val avg_speed = dist_tot/time_tot*3600
// mean_speed = 11.37405654347895

val data_small = data3.limit(10000)

// data3.groupBy($"RatecodeID").map(x => x.getDouble(4)).reduce(_+_)
// data3.groupBy($"RatecodeID").map(x => x.getLong(2)).reduce(_+_)

data_small.groupBy($"RatecodeID").map(x => x.getDouble(4)).reduce(_+_)
data_small.groupBy($"RatecodeID").map(x => x.getLong(2)).reduce(_+_)

data_small.groupBy($"RatecodeID").map(x => (x.getDouble(4) , x.getLong(2)).reduce(_+_)


data_small.select("RatecodeID","duration_sec", "trip_distance").
groupBy($"RatecodeID").sum().
withColumn("avg_speed", $"sum(trip_distance)" / $"sum(duration_sec)" * 3600)

val avg_speed_grouped = data3.select("RatecodeID","duration_sec", "trip_distance").
groupBy($"RatecodeID").sum().
withColumn("avg_speed", $"sum(trip_distance)" / $"sum(duration_sec)" * 3600)

avg_speed_grouped.show

avg_speed_grouped.printSchema
avg_speed_grouped.count
