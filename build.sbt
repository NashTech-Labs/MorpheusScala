name := "SparkCypherDemo"

version := "0.1"

scalaVersion := "2.12.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.opencypher" % "morpheus-spark-cypher" % "0.4.2"
fork in run := true