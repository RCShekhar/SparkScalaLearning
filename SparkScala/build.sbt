name := "SparkLerningProject"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.scala.spark.learning")

mainClass := Some("com.scala.spark.learning.WordCount")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.3"

