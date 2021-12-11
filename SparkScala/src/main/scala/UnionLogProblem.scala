package com.scala.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UnionLogProblem {

  def isHeader(line: String): Boolean = line.startsWith("host") && line.contains("response")

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = SparkContextFactory.sc

    val julLogs = sc.textFile("src/main/scala/in/nasa_19950701.tsv")
    val augLogs = sc.textFile("src/main/scala/in/nasa_19950801.tsv")

    val unionLogs = julLogs.union(augLogs)

    val cleanUnionLogs = unionLogs.filter(line => !isHeader(line))

    //cleanUnionLogs.saveAsTextFile("src/main/scala/out/unonProblem.txt")

    val sample = cleanUnionLogs.sample(withReplacement = false, fraction = 0.2)

    sample.saveAsTextFile("src/main/scala/out/sampleLog.txt")

  }
}
