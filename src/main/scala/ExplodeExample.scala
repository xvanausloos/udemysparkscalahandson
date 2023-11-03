package com.ldi.spark

import org.apache.spark
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
object ExplodeExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("testexplode")
      .master("local[*]")
      .getOrCreate()

    val arrayData = Seq(
      Row("James", List("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")),
      Row("Michael", List("Spark", "Java", null), Map("hair" -> "brown", "eye" -> null)),
      Row("Robert", List("CSharp", ""), Map("hair" -> "red", "eye" -> "")),
      Row("Washington", null, null),
      Row("Jefferson", List(), Map())
    )

    val arraySchema = new StructType()
      .add("name", StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType, StringType))

    import spark.implicits._
    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
    df.printSchema()
    val df2 = df.select($"name", explode($"knownLanguages"))
    df2.show(false)
  }

}
