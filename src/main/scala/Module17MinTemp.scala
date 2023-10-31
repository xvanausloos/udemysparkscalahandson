package com.ldi.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark._
import scala.math.min

object Module17MinTemp {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("ldi").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MinTemp")
    val lines = sc.textFile("data/1800.csv")
    val parsedLines = lines.map(parseLine)

    //filter out all but TMIN
    val minTemps = parsedLines.filter(x => (x._2 == "TMIN"))

    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(x => (x._1,x._3.toFloat))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y) )

    val results = minTempsByStation.collect()

    for (result <- results.sorted){
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f Farenheit"
      println(s"Station $station min temp : $formattedTemp")
    }


  }

}
