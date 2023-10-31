package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


/*
Module 21 total amount by customer
 */
object Module21 {

  def extractCustomerPricePairs(line: String) = {
    val fields = line.split(",")
    var custid = fields(0)
    var amount = fields(2)
    (fields(0).toInt, fields(2).toFloat) //return cust_id and amount
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("ldi").setLevel(Level.ERROR)
    var sc = new SparkContext("local[*]","module 21")

    val input = sc.textFile("data/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)

    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y  )

    totalByCustomer.saveAsTextFile("module21.txt")

    // Print the results.
    println("ENDr")
  }
}
