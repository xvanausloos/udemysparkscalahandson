package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Module18 {

  /*

  words count
   */

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","word count")

    val input = sc.textFile("data/book.txt")

    val words = input.flatMap(x => x.split("\\W+"))

    val wordsCounts = words.countByValue()

    wordsCounts.foreach(println)

  }

}
