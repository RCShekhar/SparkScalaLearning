package com.scala.spark.learning
package pairrdd

import org.apache.log4j.{Level, Logger}

object GroupByKeyVsReduceByKey {
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc

    val words = List("One", "Two", "Two", "Three", "Three", "Three")

    val wordsPairRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountWithReduceByKey = wordsPairRDD.reduceByKey((x,y) => x+y).collect()
    println("wordCountWithReduceByKey: " + wordCountWithReduceByKey.toList)

    val wordCountWithGroupByKey = wordsPairRDD.groupByKey().mapValues(intIerable => intIerable.size).collect()
    println("WordCountWithGroupByKey: " + wordCountWithGroupByKey.toList)
  }

}
