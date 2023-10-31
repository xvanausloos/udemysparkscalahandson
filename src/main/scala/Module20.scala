package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/*
WordCount sorting using the cluster (improved version module 18)
 */
object Module20 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    var sc = new SparkContext("local[*]","module 20")
    var input = sc.textFile("data/book.txt")
    var words = input.flatMap(x => x.split("\\W+"))
    val lowerCaseWords = words.map(x => x.toLowerCase())

    var wordsCount = lowerCaseWords.map( x => (x,1)).reduceByKey((x,y) => x + y)

    //flip (word, count) to (count, word) and sort by key
    var wordsCountSorted = wordsCount.map(x => (x._2, x._1)).sortByKey()
    wordsCountSorted.collect().foreach(println)

  }

}
