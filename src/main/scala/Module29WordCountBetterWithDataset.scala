package com.ldi.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._


object Module29WordCountBetterWithDataset {

  case class Book(value: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("Ldi").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Module 29")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    //split words iusing reg exp
    val wordsSplit = input.select(explode(split($"value", "\\W+")).alias("word")).filter($"word" =!= "")

    wordsSplit.select(lower($"word").alias("word lowercase")).show()

  }

}
