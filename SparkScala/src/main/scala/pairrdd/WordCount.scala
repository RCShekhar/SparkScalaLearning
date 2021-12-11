package com.scala.spark.learning
package pairrdd

import org.apache.log4j.{Level, Logger}

object WordCount {

  def main(args:Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc

    val lines = sc.textFile("src/main/scala/in/word_count.text")
    val wordRDD = lines.flatMap(line => line.split(" "))

    val wordPairRDD = wordRDD.map(word => (word, 1))
    val wordCounts = wordPairRDD.reduceByKey((prev, curr) => prev+curr)
    val sortedWordCounts = wordCounts.sortByKey()

    for ((word, count) <- sortedWordCounts.collect()) println(word + ": " + count)
  }

}
