package com.scala.spark.learning
package pairrdd

import org.apache.log4j.{Level, Logger}

object JoinOperations {

  def main(args:Array[String])= {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc

    val ages = sc.parallelize(List(("Tom", 29), ("John", 22)))
    val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))

    val join = ages.join(addresses)
    join.saveAsTextFile("src/main/scala/out/joinOperations/join.txt")

    val leftOuterJoin = ages.leftOuterJoin(addresses)
    leftOuterJoin.saveAsTextFile("src/main/scala/out/joinOperations/leftOuter.txt")

    val rightOuterJoin = ages.rightOuterJoin(addresses)
    rightOuterJoin.saveAsTextFile("src/main/scala/out/joinOperations/rightOuter.txt")

    val fullOuterJoin = ages.fullOuterJoin(addresses)
    fullOuterJoin.saveAsTextFile("src/main/scala/out/joinOperations/fullOuter.txt")
  }
}
