package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
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

    ds.show()


  }

}
