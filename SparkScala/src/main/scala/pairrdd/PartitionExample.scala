package com.scala.spark.learning
package pairrdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel

object PartitionExample {
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc

    val lines = sc.textFile("src/main/scala/in/RealEstate.csv")
    val noHeaderLines = lines.filter(line => !line.contains("Bedrooms"))

    val housePricePairRDD = noHeaderLines.map(line => (line.split(",")(3), (1, line.split(",")(2).toDouble)))
    val partitionedHousePricePairRDD = housePricePairRDD.partitionBy(new HashPartitioner(8))
    partitionedHousePricePairRDD.persist(StorageLevel.MEMORY_ONLY)

    val housePriceTotal = partitionedHousePricePairRDD.reduceByKey((x, y) => (x._1+y._1, x._2+y._2))
    //for((bedroom, total) <- housePriceTotal.collect()) println(bedroom + ": " + total)
    val housePriceAvg = housePriceTotal.mapValues(countAndPrice => countAndPrice._2/countAndPrice._1)

    for ((bedroom, avgPrice) <- housePriceAvg.collect()) println(bedroom + ": " + avgPrice)


  }

}
