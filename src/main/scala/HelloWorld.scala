package com.ldi.spark

import org.apache.log4j.{Level, Logger}


object HelloWorld {
  def main(args: Array[String]): Unit = {
   Logger.getLogger("ldi").setLevel(Level.ERROR)
  }

}
