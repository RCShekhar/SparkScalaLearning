package com.scala.spark.learning

object PairRddFromRegularRdd {
  def main(args:Array[String]) = {
    val sc = SparkContextFactory.sc

    val regularList = List("Ravi 23", "John 29", "Lily 29", "James 8")
    val regularRDD = sc.parallelize(regularList)

    val pairRDD = regularRDD.map(s => (s.split(" ")(0), s.split(" ")(1)))
    pairRDD.coalesce(1).saveAsTextFile("src/main/scala/out/pariRDDFromRegularRDD.txt")
  }

}
