package com.scala.spark.learning
package pairrdd

object AirportsUppercaseProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text

       Each row of the input file contains the following columns:

       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */

    val sc = SparkContextFactory.sc
    val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

    val airportRDD = sc.textFile("src/main/scala/in/airports.text")

    val airportPairRDD = airportRDD.map(line => (line.split(COMMA_DELIMITER)(1), line.split(COMMA_DELIMITER)(3)))

    val upperCasePairRDD = airportPairRDD.mapValues(countryName => countryName.toUpperCase())
    upperCasePairRDD.saveAsTextFile("src/main/scala/out/upperCasePairRDD.txt")
  }
}
