package com.sdf.core.dataflow.utils

object CommonCodeSnippet {

  val sparkSession="sparkSession"
  val sparkSessionInitialize=s"${sparkSession} = SparkSession.builder.getOrCreate()"
  val sparkConfig=s"${sparkSession}.conf.set"
  val initialImports=
    s"""
       |import os
       |from pyspark.sql import SparkSession,DataFrame
       |""".stripMargin

  val fileDf="fileDF"
  val createOrTempView="createOrReplaceTempView"
  val mainFunction="if __name__ == '__main__':"
  val indentation="\t"
  var stagingLocation="staging"
  var tempCodeFile="codeToExecute.py"
}
