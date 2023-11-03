package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object Module33MostPopularMovies {

  // Use final with case class as good practice https://nrinaudo.github.io/scala-best-practices/tricky_behaviours/final_case_classes.html
  final case class Movie(userID: Int, movieID: Int, rating: Int, timestamp: Long)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("module 33")
      .master("local[*]")
      .getOrCreate()

    val moviesSchema = new StructType()
      .add("userID", IntegerType, true)
      .add("movieID", IntegerType, true)
      .add("rating", IntegerType, true)
      .add("timestamp", LongType, true)

    import spark.implicits._
    val movieDS = spark.read
      .option("headers","false")
      .option("sep", "\t")
      .schema("moviesSchema")
      .csv("data/ml-100k/u.data")
      .as[Movie]

    val topMovies = movieDS.groupBy("movieID").count()
    topMovies.show(false)

  }



}
