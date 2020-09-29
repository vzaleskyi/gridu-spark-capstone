name := "SparkFinalPrj"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "2.4.3"),
  ("org.apache.spark" %% "spark-sql" % "2.4.3"),
  ("org.apache.spark" %% "spark-avro" % "2.4.4")
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test