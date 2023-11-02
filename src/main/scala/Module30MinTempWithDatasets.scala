package com.ldi.spark



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

import java.util.Date

object Module30MinTempWithDatasets {

  case class Temperature(stationId: String, date: Int, measure_type: String, temperature: Float)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("module 30")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationId", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    import spark.implicits._

    //create dataset
    val ds = spark.read
      .schema(temperatureSchema)
      .option("sep",",")
      .csv("data/1800.csv")
      .as[Temperature] //required import spark.implicits._

    val minTemps = ds.select("stationId", "temperature")
    val minTempsByStation = minTemps.groupBy("stationId").min("temperature")
    //minTempsByStation.show(minTempsByStation.count().toInt)



    val minTempsByStationF = minTempsByStation
      .withColumn("temperature",functions.round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f ,2))
      .select("stationId", "temperature").sort("temperature")

    val results = minTempsByStationF.collect()

    //minTempsByStationF.show()

    for (result <- results){
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F."
      println(s"station $station min temp : $formattedTemp")
    }
  }
}