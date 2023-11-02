package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.Console.println

/*
Module 27 Friends by age using Spark SQL Dataset
 */
object Module27 {

  case class Friend(id:Int, name: String, age:Int, friends:Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    // spark session as we want to use Spark Dataset
    val spark = SparkSession
      .builder
      .appName("module 27")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val friends = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      .as[Friend]

    friends.show(10)

    val friendsFields = friends.select("age","friends")
    val friendsByAge = friendsFields.groupBy("age").avg("friends")
    val friendsByAgeSorted = friendsByAge.sort("age").show

    println("*** end ***")

  }


}
