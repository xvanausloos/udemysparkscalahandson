package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


/*
Udemy Spark Scala hands on
Ratings counter
Module 12 : count the occurences of second field (movie_id) from file data/ml-100k/u.data
0	50	5	881250949
0	172	5	881250949
0	133	1	881250949

 */
object Module12RatingsCounter {
  def main(args: Array[String]): Unit = {
    // set the logger
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    // create a Spark context
    val sc = new SparkContext("local[*]", "ratingCounters")

    // load all the lines
    val lines = sc.textFile("data/ml-100k/u.data")
    val ratings = lines.map(x => x.split("\t")(2)) //get third field with rating
    val results = ratings.countByValue() // one value in ratings = rating from 1 to 5 stars. Group by value and count
    val sortedResults = results.toSeq.sortBy(_._1)
    sortedResults.foreach(println)

    println("*** END OF PROGRAM ***")

  }

}
