package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object HelloWorld {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","hello world")
    val lines = sc.textFile("data/ml-100k/u.data")
    val nbLines = lines.count()
    println(s"hello num lines $nbLines")
    sc.stop()
  }

}
