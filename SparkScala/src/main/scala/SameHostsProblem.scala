package com.scala.spark.learning

import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    val conf = new SparkConf().setAppName("SameHostsProblem")
    val sc = new SparkContext(conf)

    // Creating RDDs
    val julLogs = sc.textFile("s3://de-ds-learning/in/nasa_19950701.tsv")
    val augLogs = sc.textFile("s3://de-ds-learning/in/nasa_19950801.tsv")

    // Tranformation 1
    val julHosts = julLogs.map(line => line.split("\t")(0))
    val augHosts = augLogs.map(line => line.split("\t")(0))

    // Tranformation 2
    val commonHosts = julHosts.intersection(augHosts)
    val commonHostsWithoutHeader = commonHosts.filter(line => !line.contains("host"))

    //Action
    commonHostsWithoutHeader.saveAsTextFile("s3://de-ds-learning/out/sameHostSolution.txt")

  }
}
