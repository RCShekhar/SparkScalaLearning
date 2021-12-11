package com.scala.spark.learning

import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

    val conf = new SparkConf().setAppName("AirportsInUsaProblem").setMaster("local")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("src/main/scala/in/airports.text")
    val USairports = airports.filter(line => line.split(",")(3) == "\"United States\"")
    val airportNameCity = USairports.map(line => line.split(",")(1) + "," + line.split(",")(2))

    airportNameCity.saveAsTextFile("src/main/scala/out/airports_in_us.txt")
  }
}
