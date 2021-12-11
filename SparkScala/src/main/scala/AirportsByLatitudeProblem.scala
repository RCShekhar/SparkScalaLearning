package com.scala.spark.learning

import org.apache.hadoop.hdfs.util.ReadOnlyList.Util
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are
       bigger than 40. Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    // hint: line.split(",")(6) > 40
    val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

    val conf = new SparkConf().setAppName("LatitudeProblem").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("src/main/scala/in/airports.text")
    val higherLatitude = airports.filter(line => line.split(COMMA_DELIMITER)(6).toDouble > 40)
    val airportLatitude = higherLatitude.map(line => line.split(COMMA_DELIMITER)(1) + "," + line.split(",")(6))

    airportLatitude.saveAsTextFile("src/main/scala/out/aiportLatitude.txt")
  }
}
