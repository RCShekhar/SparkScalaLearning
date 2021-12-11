package com.scala.spark.learning
package sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RddDatasetConversion {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]")

    val sc = new SparkContext(conf)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

    val lines = sc.textFile("src/main/scala/in/2016-stack-overflow-survey-responses.csv")

    val responseRDD = lines
      .filter(line => !line.split(Utils.COMMA_DELIMITER, -1)(2).equals("country"))
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER, -1)
        RowType(splits(2), toDouble(splits(6)), splits(9), toDouble(splits(14)))
      })

    import session.implicits._
    val responseDataset = responseRDD.toDS()

    System.out.println("=== Print out schema ===")
    responseDataset.printSchema()

    System.out.println("=== Print 20 records of responses table ===")
    responseDataset.show(20)

    for (response <- responseDataset.rdd.collect()) println(response)
  }

  def toDouble(split: String): Option[Double] = {
    if (split.isEmpty) None else Some(split.toDouble)
  }
}
