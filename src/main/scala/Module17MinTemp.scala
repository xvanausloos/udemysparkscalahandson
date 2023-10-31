package com.ldi.spark

object Module17MinTemp {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(1)
    val temperature = fields(2).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {




  }

}
