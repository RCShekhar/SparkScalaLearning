package com.scala.spark.learning

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextFactory {
  val conf = new SparkConf().setAppName("UnionProblem").setMaster("local")
  val sc = new SparkContext(conf)
}
