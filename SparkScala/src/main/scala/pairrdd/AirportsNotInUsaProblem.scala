package com.scala.spark.learning
package pairrdd

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */

    val sc = SparkContextFactory.sc
    val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

    val airportRdd = sc.textFile("src/main/scala/in/airports.text")

    val airportPairRDD = airportRdd.map(line => (line.split(COMMA_DELIMITER)(1), line.split(COMMA_DELIMITER)(3)))
    val airportsNotInUSA = airportPairRDD.filter(keyValuePair => keyValuePair._2 != "\"United States\"")

    airportsNotInUSA.saveAsTextFile("src/main/scala/out/airports_not_in_usa_pair_rdd.txt")
  }
}
