package com.scala.spark.learning
package pairrdd

import org.apache.log4j.{Level, Logger}

import scala.io.Source

object UkMakersSpace {

  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc

    val postCodeMap = sc.broadcast(loadPostCode())

    val makersSpaceRDD = sc.textFile("src/main/scala/in/uk-makerspaces-identifiable-data.csv")
    val makersSpacePostCode = makersSpaceRDD.filter(line => line.split(Utils.COMMA_DELIMITER)(0) != "Timestamp")

    val makersSpacePostCodeRefined = makersSpacePostCode.map(line => postCodeMap.value.getOrElse(getPostCode(line).get, "Unknown"))
    val sorted = makersSpacePostCodeRefined.map(value => (value, 1)).reduceByKey((v1,v2) => v1+v2).sortByKey()

    for ((region, count) <- sorted.collect()) println(region + ": " + count)
  }

  def loadPostCode():Map[String, String] = {
    Source.fromFile("src/main/scala/in/uk-postcode.csv").getLines().map(line => {
      val fields = line.split(Utils.COMMA_DELIMITER)
      fields(0) -> fields(7)
    }).toMap
  }

  def getPostCode(line:String):Option[String] = {
    val fields = line.split(Utils.COMMA_DELIMITER)
    val postcode = fields(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }

}
