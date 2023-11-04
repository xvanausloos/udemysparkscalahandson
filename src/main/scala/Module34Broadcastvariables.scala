package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import scala.io.{Codec, Source}

object Module34Broadcastvariables {

  // Use final with case class as good practice https://nrinaudo.github.io/scala-best-practices/tricky_behaviours/final_case_classes.html
  final case class Movie(userID: Int, movieID: Int, rating: Int, timestamp: Long)



  // load a map of movie ids as integer and movie names as string
  def loadMovieNames(): Map[Int, String] = {
    // handle encoding issue
    implicit val codec: Codec = Codec("ISO-8859-1")

    // create a map of Ints and Strings and populate it with u.item
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()){
      val fields = line.split('|')
      if (fields.length>1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()
    movieNames // return Map
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("module 33")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    val moviesSchema = new StructType()
      .add("userID", IntegerType, true)
      .add("movieID", IntegerType, true)
      .add("rating", IntegerType, true)
      .add("timestamp", LongType, true)

    import spark.implicits._

    val movieDS = spark.read
      .option("headers","false")
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]

    val moviesCount = movieDS.groupBy("movieID").count()

    // create a user-defined function to look up movie names from our shared Map variable

    // we start by declaring an anonymous function in scala
    val lookupName: Int => String = (movieID:Int) => {
      nameDict.value(movieID)
    }

    // wrap it using UDF
    val lookupNameUDF = udf(lookupName)

    // add a movieTitle column using our new udf
    val moviesWithNames = moviesCount.withColumn("movieTitle", lookupNameUDF(col("movieID")))



    spark.stop()


  }



}
