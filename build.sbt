name := "spark_test"

version := "1.0"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-avro" % "2.4.2"
)
