package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Module15AvgFriendsByAge {

  // function for getting age and nb of friends
  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    //create a tuple
    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsbyAge")
    val lines = sc.textFile("data/fakefriends.csv")

    val rdd = lines.map(x => parseLine(x)) // lines.map(parseLine) is equivalent

    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.

    // totalsByAge
    val rddByage = rdd.mapValues(x => (x,1)).reduceByKey( (x,y) => (x._1+y._1,x._2 + y._2) )
    rddByage.foreach(println)


  }

}
