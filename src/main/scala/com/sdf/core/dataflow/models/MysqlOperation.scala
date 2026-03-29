package com.sdf.core.dataflow.models

import com.sdf.core.dataflow.Flow.getClass
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MysqlOperation {
  val logger = LogManager.getLogger(getClass.getSimpleName)

  /*
  @param spark:SparkSession
  @param connectionUrl:String
  @param databaseName:String
  @param tableName:String
  @param userName:String
  @param password:String
   */
  def getJdbcDF(spark: SparkSession,
                connectionUrl: String,
                databaseName: String,
                tableName: String,
                userName: String, password: String, driver: String, identifier: String): DataFrame = {

    var url = ""
    if (identifier == "mysql") {
      url = s"${connectionUrl}/${databaseName}"
    }
    if (identifier == "sqlserver") {
      url = s"${connectionUrl}"
    }

    val jdbcDf = spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", s"${url}")
      .option("dbtable", databaseName + "." + tableName)
      .option("user", userName)
      .option("password", password)
      .load()
     
    jdbcDf.show()
    jdbcDf
  }

  def writeToJdbc(spark: SparkSession,
                  tempView: String, df_name: String,
                  databaseName: String, tableName: String, outputType: String,
                  identifier: String, connectionUrl: String,
                  userName: String, password: String,
                  mode: String, driver: String): Unit = {

    val df = spark.table(s"${tempView}")

    var url = ""
    if (identifier == "mysql") {
      url = s"${connectionUrl}/${databaseName}"
    }
    if (identifier == "sqlserver") {
      url = s"${connectionUrl}"
    }

    try {
      df.write
        .format(s"${outputType}")
        .option("driver", driver)
        .option("url", s"${url}")
        .option("dbtable", databaseName + "." +tableName)
        .option("user", userName)
        .option("password", password)
        .mode(mode).save()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

  }

}
