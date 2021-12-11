package com.scala.spark.learning

object PairRDDFromTupleList {

  def main(args:Array[String]) = {
    val sc = SparkContextFactory.sc

    val tupleList = List(("Ravi", 23),("John", 29), ("Lily", 29), ("James", 8))
    val pairRDD = sc.parallelize(tupleList)

    pairRDD.coalesce(1).saveAsTextFile("src/main/scala/out/pariRDDFromTupleList.txt")
  }
}
