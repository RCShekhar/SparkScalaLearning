package com.scala.spark.learning
package sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RunRealSQL {

  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("TypedDataset").master("local[*]").getOrCreate()
    val dataFrameReader = session.read

    val responses = dataFrameReader
      .option("header", "true")
      .option("inferSchema", value=true)
      .csv("src/main/scala/in/2016-stack-overflow-survey-responses.csv")

    val responseSelectedColumns = responses.select("country", "age_midpoint", "occupation", "salary_midpoint")
    responseSelectedColumns.printSchema()

    //responseSelectedColumns.select("country", "occupation").filter(condition)
    responseSelectedColumns.createTempView("responses")

    session.sql("select * from responses").show()
    session.sql("select * from responses where country = \"Afghanistan\"").show()
    session.sql("select country, count(*) from responses group by country order by country desc").show()



    session.stop()
  }

}
