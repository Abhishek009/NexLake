package com.sdf.core.dataflow.utils

import SparkJob.spark
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJob {

  val logger: Logger = LogManager.getLogger(this.getClass.getName)
  var spark:SparkSession = _

  /**
   * @param appName
   * @param master
   */
  def createSparkSession(appName: String): SparkSession = {

    logger.info(s"AppName: ${appName}")
    val sparkBuilder = if (isWindows()) {
      SparkSession.builder().appName(appName).master("local[2]")
    } else {
      SparkSession.builder().appName(appName)
    }
    val spark = sparkBuilder.enableHiveSupport().getOrCreate()
    logger.info(s"spark: ${spark}")
    spark
  }

  def isWindows(): Boolean = {
    System.getProperty("os.name").toLowerCase().contains("windows")
  }
  /**
   * @param appName
   * @param master
   * @param session
   */
  /*def isSparkSessionAvailable(appName: String, session: SparkSession): Unit = {
    val sparkSession = session match {
      case ss => ss
      case _ => createSparkSession(appName)
    }
  }
*/
  def createTempTable(spark: SparkSession, df: DataFrame,viewName:String): Unit = {
    df.createOrReplaceTempView(viewName)
  }



}
