package com.scala.spark.learning
package pairrdd

import org.apache.log4j.{Level, Logger}

object AirportsByCountryProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = SparkContextFactory.sc
    val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

    val lines = sc.textFile("src/main/scala/in/airports.text")
    val countryAndAirportPairRDD = lines.map(line => (line.split(COMMA_DELIMITER)(3), line.split(COMMA_DELIMITER)(1)))
    val airportsByCountry = countryAndAirportPairRDD.groupByKey().mapValues(airports => airports.toList)
    for ((country, airports) <- airportsByCountry.collect) println(country + ": " + airports)

  }
}
