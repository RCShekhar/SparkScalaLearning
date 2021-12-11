package com.scala.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

object PersistExample {

  def main(args:Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc

    val ints = List(1,2,3,4,5)
    val intRDD = sc.parallelize(ints)

    intRDD.persist(StorageLevel.MEMORY_ONLY)

    intRDD.reduce((x,y) => (x*y)) // this will create and persist RDD
    intRDD.count() // this action will re-use the rdd which was created during reduce actions

  }

}
