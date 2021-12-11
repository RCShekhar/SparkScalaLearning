package com.scala.spark.learning

import org.apache.log4j.{Level, Logger}

object ReduceExample {

  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc

    val lst = List(1,2,3,4,5,6,7,8,9)
    val intRDD = sc.parallelize(lst)

    val reduced = intRDD.reduce((a,b) => a+b)
    println("The total of all rdd elements is: " + reduced)
  }

}