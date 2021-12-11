package com.scala.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object numbers {
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Numbers").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val nums = sc.parallelize(List(1,2,3,4,5,6,7,8,9))
    val s = nums.map(num => num*num)

    for (i <- nums.collect) print(i.toString + " ")
    println()
    for (i <- s.collect) print(i.toString + " ")

  }

}
