package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.Console.println

/*
Module 26
 */
object Module26 {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    // spark session as we want to use Spark Dataset
    val spark = SparkSession
      .builder
      .appName("module 26")
      .master("local[*]")
      .getOrCreate()

    // load each line in a Dataset
    import spark.implicits._
    val people = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      .as[Person] //if we comment it we got a dataframe (schema is  inferred)

    println("Here is our inferred schema: ")
    people.printSchema()

    println("Let's select the name column:")
    people.select("name").show()

    println("Filter anyone over 21:")
    people.filter("age>=21")
    people.show()

    println("Group by age:")
    people.groupBy("age").count().show()


    println("Make everyone 10 years older:")
    people.select(people("name"), people("age"), people("age")+10).show()

    spark.stop()
  }

}
