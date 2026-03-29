package com.sdf.core.dataflow.models

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOperation {

  val logger = LogManager.getLogger(getClass.getSimpleName)

  def getHiveDF(spark: SparkSession, tableName: String, schemaName: String): DataFrame ={
    val hiveDF = spark.read.table(s"${schemaName}.${tableName}")
    hiveDF
  }

def writeToHiveTable(spark: SparkSession,tempView:String,outputTableName:String,
                     outputSchemaName:String,partition:String,outputMode:String): Unit = {
  val df = spark.table(s"${tempView}")
  try {
   logger.info(s"checking if the table ${outputSchemaName}.${outputTableName} is present")
    if (spark.catalog.tableExists(outputSchemaName,outputTableName))
    {
    logger.info(s"Table ${outputSchemaName}.${outputTableName} is present")
      val column = spark.sql(s"select * from ${outputSchemaName}.${outputTableName} limit 0").schema.fieldNames
      val finalColumnList = column.mkString(",")

      df.select(column.map(name => col(name)):_*).write.mode(outputMode)
        .insertInto(s" ${outputSchemaName}.${outputTableName}")

    }else{
      logger.error(s"Output table ${outputSchemaName}.${outputTableName} is not present. " +
        s"Table needs to be present to load the data into it.")
    }
  }
  catch {
    case e: Exception => {
      e.printStackTrace()
    }
  }
}

}
