package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.parquet.format.IntType
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{FloatType, StringType, StructType}

import java.sql.Struct

object Module31TotalSpentByCustDatasets {

  val logger = Logger.getLogger(this.getClass.getName)

  case class CustomerOrders(custId: String, productId: String, amount: Float)

  def main(args: Array[String]): Unit = {
    logger.info("Logger : Welcome to log4j")

    val myschema = new StructType()
      .add("custId",StringType)
      .add("productId", StringType)
      .add("amount", FloatType)

    // set Spark Session
    val spark = SparkSession
      .builder
      .appName("module 30")
      .master("local[*]")
      .getOrCreate()

    // read input file

    import spark.implicits._
    val ds = spark.read
      .option("headers","false")
      .schema(myschema)
      .csv("data/customer-orders.csv")
      .as[CustomerOrders]

    //input.show()

    val data = ds.select("custId","amount")
    //val datagrouped = data.groupBy("custId").sum()

    val datagrouped = data.groupBy("custId").agg(functions.round(functions.sum("amount"),2)
      .alias("total_spent"))

   val datasorted = datagrouped.sort("total_spent")

    datasorted.show()

  }


}
