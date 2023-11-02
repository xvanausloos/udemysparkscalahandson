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
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      //.as[Person] //if we comment it we got a dataframe (schema is  inferred)

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    val teenAgers = spark.sql("SELECT * FROM people WHERE age > 13 AND age <= 19")
    val results = teenAgers.collect()
    results.foreach(println)

    Thread.sleep(1000)
  }

}
