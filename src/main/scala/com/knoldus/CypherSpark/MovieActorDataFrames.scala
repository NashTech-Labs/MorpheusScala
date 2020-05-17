package com.knoldus.CypherSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object MovieActorDataFrames {

  private val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

  val csvOptions = Map("header"->"true", "delimiter" -> ";", "inferSchema" -> "true")

  val resourcePath = "src/main/resources"

  def createNode(session: SparkSession, fileName: String): DataFrame = {
    LOGGER.info(s"Reading data from $fileName")
    session.read.options(csvOptions).csv(s"$resourcePath/$fileName")
  }

}
