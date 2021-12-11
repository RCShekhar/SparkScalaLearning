package com.scala.spark.learning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// output will like below
// word: nof times repeated

object WordCount {

  def main(args:Array[String]) = {

    //System.setProperty("hadoop.home.dir", "D:\\LearnigScala\\winutils\\hadoop-3.0.0")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)



    //Creating RDD
    val lines = sc.textFile("src/main/scala/in/word_count.text")


    // transoformation
    def customCondition(line:String):Boolean = {
      line.toLowerCase.contains("the")
      //line.contains()
    }

    val nonEmptyLines = lines.filter(customCondition)
    //for(line <- nonEmptyLines) println(line)


    val words = nonEmptyLines.flatMap(line => line.split(" "))

    //Action
    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts) println(word + ": " + count)

  }


}
