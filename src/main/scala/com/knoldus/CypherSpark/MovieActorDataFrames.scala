package com.knoldus.CypherSpark

import org.apache.spark.sql.{DataFrame, SparkSession}

object MovieActorDataFrames {

  val csvOptions = Map("header"->"true", "delimiter" -> ";", "inferSchema" -> "true")

  val resourcePath = "src/main/resources"

  def createNode(session: SparkSession, fileName: String): DataFrame = {
    session.read.options(csvOptions).csv(s"$resourcePath/$fileName")
  }

}
