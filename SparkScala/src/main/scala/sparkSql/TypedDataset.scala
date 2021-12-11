package com.scala.spark.learning
package sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TypedDataset {

  def main(args:Array[String]) ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("TypedDataset").master("local[*]").getOrCreate()
    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value=true)
      .csv("src/main/scala/in/2016-stack-overflow-survey-responses.csv")

    val responseSelectedColumns = responses.select("country", "age_midpoint", "occupation", "salary_midpoint")
    responseSelectedColumns.printSchema()

    import session.implicits._
    val typedDatset = responseSelectedColumns.as[RowType]

    typedDatset.printSchema()

    println("filter data based on country")
    typedDatset.filter(rowType => rowType.country == "Afghanistan").show()

    println("generated counts bases on occupation")
    typedDatset.groupBy(typedDatset.col("occupation")).count().show()

    println("sorting based on salray midpoint")
    typedDatset.orderBy(typedDatset.col("salary_midpoint").desc).show()

    session.stop()
  }

}
