package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
Module 25
 */
object Module25 {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    // spark session as we want to use Spark Dataset
    val spark = SparkSession
      .builder
      .appName("module 25")
      .master("local[*]")
      .getOrCreate()

    // load each line in a Dataset
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header","false")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")

    schemaPeople.show(5)
  }

}
