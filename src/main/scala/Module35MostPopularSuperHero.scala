package com.ldi.spark



import org.apache.log4j.{Level, Logger}
import org.apache.parquet.format.IntType
import org.apache.spark.sql.functions.{col, size, split, sum, filter}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Module35MostPopularSuperHero {

  Logger.getLogger("ldi").setLevel(Level.ERROR)

  final case class SuperHeroNames(id: Int, name: String)
  final case class SuperHero(value: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("module 35")
      .getOrCreate()

    // create a schema for reading Marvel-names.txt
    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)




    //build up a hero ID -> name Dataset
    import spark.implicits._

    val names = spark.read
      .option("sep"," ")
      .schema(superHeroNamesSchema)
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id",split(col("value"),"")(0))
      .withColumn("connections", size(split(col("value")," ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    //connections.show()

    // return a row with the most popular hero
    val mostPopular = connections
      .sort($"connections".desc)
      .first()

    val mostPopularName = names
      .filter($"id" === mostPopular(0))
      .select("name")
      .first()

   println(s"${mostPopularName(0)} is the most pop super hero with ${mostPopular(1)} co-appearances")

  }


}
