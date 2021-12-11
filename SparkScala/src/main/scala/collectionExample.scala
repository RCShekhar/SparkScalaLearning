package com.scala.spark.learning

import org.apache.log4j.{Level, Logger}

object collectionExample {

  def main(args:Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = SparkContextFactory.sc

    val lst = List("Hadoop", "Spark", "Scala", "Hive", "Python", "SQL")

    val echoSystem = sc.parallelize(lst)
    val count = echoSystem.count()
    println("number of elements in the RDD is: " + count)

    //val collectedList = echoSystem.collect()

    //for(s <- collectedList) println(s.toString)

  }

}
