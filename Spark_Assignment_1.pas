#Practica YellowCab

/// 1 Cuidado con los sql -> por que la versión de spark no es la standard///



import spark.implicits._
val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")

cabs.createOrReplaceTempView("yellow")



def time[R](block: => R): R = {
val t0 = System.currentTimeMillis()
val result = block    // call-by-name
val t1 = System.currentTimeMillis()
println("Elapsed time: " + (t1 - t0) + "ms")
result
}

val texts = spark.sql("SELECT trip_distance, RateCodeID as rate,(-unix_timestamp(tpep_pickup_datetime)+unix_timestamp(tpep_dropoff_datetime)) as time FROM yellow WHERE RateCodeID <> 99")
texts.createOrReplaceTempView("with_avg")
///Creo una columna con la velocidad en km/h
val with_avg2=spark.sql("SELECT trip_distance, time, rate,(trip_distance*1.609)/(time/3600) as speed  FROM with_avg")
///Filtro valores absurdos
with_avg2.createOrReplaceTempView("speedy")
val filtered_speed=spark.sql("SELECT * FROM speedy WHERE (speed<140 AND speed >1)")
///Para sacar conclusiones, RESULTADO DEL PRIMER EJERCICIO:
filtered_speed.createOrReplaceTempView("filtered")
val grouped=spark.sql("SELECT rate,ROUND(AVG(speed),3) AS avg FROM filtered GROUP BY rate ORDER BY avg DESC")

time(grouped.show)

val copy = grouped.rdd.
 map(x => Array(x.getString(0), x.getDouble(1))).
 map(x => x.mkString(",")).
 repartition(1).
 saveAsTextFile("query1/time6")

--------------------------------------------------------------------------------------------------------
	
/// 2 Basicamente es como el anterior, pero con otra columna para agrupar
import spark.implicits._

def time[R](block: => R): R = {
val t0 = System.currentTimeMillis()
val result = block    // call-by-name
val t1 = System.currentTimeMillis()
println("Elapsed time: " + (t1 - t0) + "ms")
result
}

val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017")
cabs.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT trip_distance, HOUR(tpep_pickup_datetime) AS hour_1,(-unix_timestamp(tpep_pickup_datetime)+unix_timestamp(tpep_dropoff_datetime)) as time FROM yellow WHERE RateCodeID <> 99")
yellow.createOrReplaceTempView("yellow")
val yellow=spark.sql("SELECT trip_distance, time, hour_1,(trip_distance*1.609)/(time/3600) as speed  FROM yellow")
yellow.createOrReplaceTempView("yellow")
val yellow=spark.sql("SELECT * FROM yellow WHERE (speed<140 AND speed >0)")
yellow.createOrReplaceTempView("yellow")
val yellow=spark.sql("SELECT hour_1,AVG(speed) AS avg FROM yellow GROUP BY hour_1 ORDER BY avg DESC")

time(yellow.show)

val copy = yellow.rdd.
 map(x => Array(x.getInt(0), x.getDouble(1))).
 map(x => x.mkString(",")).
 repartition(1).
 saveAsTextFile("query2/time1")

 
-------------------------------------------------------------------------------------------------------------------------------------------
/// 3 lo que hago es ordenar por numero de veces que aparece un PULocationID y me quedo con el 10% 

def time[R](block: => R): R = {
val t0 = System.currentTimeMillis()
val result = block    // call-by-name
val t1 = System.currentTimeMillis()
println("Elapsed time: " + (t1 - t0) + "ms")
result
}

val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val texts = spark.sql("SELECT PULocationID, COUNT(PULocationID) AS num FROM yellow GROUP BY PULocationID")
texts.createOrReplaceTempView("filtered")
val result=spark.sql("SELECT PULocationID,num, NTILE(10) OVER(ORDER BY num DESC) as NT FROM filtered SORT BY num DESC")
result.createOrReplaceTempView("filtered")
val result=spark.sql("SELECT * FROM filtered WHERE NT =1")

time(result.show)

val copy = result.rdd.
 map(x => Array(x.getString(0), x.getLong(1),x.getInt(2))).
 map(x => x.mkString(",")).
 repartition(1).
 saveAsTextFile("query3/time4")
 
 ///Revisar

--------------------------------------------------------------------------------------------------------
/// 4 Bigger tips - Me quedo con las 10 que tienen el ratio mas alto de tip_amount/total_amount

def time[R](block: => R): R = {
val t0 = System.currentTimeMillis()
val result = block    // call-by-name
val t1 = System.currentTimeMillis()
println("Elapsed time: " + (t1 - t0) + "ms")
result
}

val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT ROUND((tip_amount/total_amount),3) as ratio, tip_amount, total_amount FROM yellow")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT * FROM yellow SORT BY ratio DESC LIMIT 10")

time(yellow.show)

val copy = yellow.rdd.
 map(x => Array(x.getDouble(0), x.getString(1),x.getString(2))).
 map(x => x.mkString(",")).
 repartition(1).
 saveAsTextFile("query4/time1")

-------------------------------------------------------------------------------------------------------------
/// 5 Averaged incomes in terms of hour/month/labor days vs weekends.


def time[R](block: => R): R = {
val t0 = System.currentTimeMillis()
val result = block    // call-by-name
val t1 = System.currentTimeMillis()
println("Elapsed time: " + (t1 - t0) + "ms")
result
}
	
val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT total_amount, HOUR(tpep_pickup_datetime) AS hour_1, date_format(tpep_pickup_datetime,'EEEE') as day_1 FROM yellow")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT day_1,hour_1,ROUND(SUM(total_amount)/COUNT(total_amount),2) as mean_amount FROM yellow GROUP BY ROLLUP (hour_1,day_1)")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT * FROM yellow WHERE day_1 IS NOT NULL ORDER BY mean_amount DESC")

time(yellow.show)

val copy = yellow.rdd.
 map(x => Array(x.getString(0), x.getInt(1),x.getDouble(2))).
 map(x => x.mkString(",")).
 repartition(1).
 saveAsTextFile("query5/time1")

-----------------------------------------------------------------------------------------------------------------
/// 6 Distance of the trips in terms of factors such as final destination
 
import spark.implicits._


def time[R](block: => R): R = {
val t0 = System.currentTimeMillis()
val result = block    // call-by-name
val t1 = System.currentTimeMillis()
println("Elapsed time: " + (t1 - t0) + "ms")
result
}

val cabs=spark.read.format("csv").option("header","true").option("header","true").load("/YellowCab/2017/yellow_tripdata_2017-01.csv")
cabs.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT DOLocationID, ROUND(AVG(trip_distance),2) as mean_distance FROM yellow GROUP BY DOLocationID")
yellow.createOrReplaceTempView("yellow")
val yellow = spark.sql("SELECT * FROM yellow ORDER BY mean_distance DESC")

time(yellow.show)

val copy = yellow.rdd.
 map(x => Array(x.getString(0), x.getDouble(1))).
 map(x => x.mkString(",")).
 repartition(1).
 saveAsTextFile("query6/time1")

-----------------------------------------------------------------------------------------------------------------


 
 