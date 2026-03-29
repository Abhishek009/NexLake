package com.sdf.core.dataflow.utils

import com.sdf.core.dataflow.Flow.getClass
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import upickle.default.read

import scala.io
import java.io.{File, FileInputStream, FileNotFoundException}
import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import scala.io.Source



object CommonConfigParser {

  val logger = LogManager.getLogger(getClass.getSimpleName)

  /**
   *
   * @param configFile
   * @param connectionType
   * @param connectionSubType
   * @return Map[String,String]
   */
  def parseConfig(configFile: String, connectionType: String, connectionSubType: String): util.Map[String, String] = {

    logger.info(s"configFile ${configFile}")
    logger.info(s"connectionType ${connectionType}")
    logger.info(s"connectionSubType ${connectionSubType}")
    val jobConfig = new FileInputStream((new File(configFile)))
    logger.info(s"jobConfig ${jobConfig}")
    val yamlData = new Yaml().load(jobConfig).asInstanceOf[java.util.Map[String, Any]]
    logger.info(s"yamlData ${yamlData}")
    val jdbcConfiguration = yamlData.get(connectionType).asInstanceOf[java.util.Map[String, String]]
    logger.info(s"jdbcConfiguration ${jdbcConfiguration}")
    jdbcConfiguration.get(connectionSubType).asInstanceOf[java.util.Map[String, String]]

  }

  def getMetaConfig(configVariables:String):Map[String,String] ={

    val input = io.Source.fromFile(configVariables).mkString
    read[Map[String,String]](input)
  }

  def getSparkConfig(sparkConfigFile:String):Map[String,String] = {
    val lines = Source.fromFile(sparkConfigFile).getLines()
    val propertiesMap = lines.map {
      line =>
        val Array(key, value) = line.split("=")
        key -> value
    }.toMap
    propertiesMap
  }

  import org.yaml.snakeyaml.error.YAMLException

  def parseYamlConfig(configFile: String): Option[Map[String, Any]] = {
    try {
      val jobConfig = new FileInputStream(new File(configFile))
      val yaml = new Yaml().load(jobConfig).asInstanceOf[java.util.Map[String, Any]].asScala.toMap
      Some(yaml)
    } catch {
      case e: FileNotFoundException =>
        logger.error(s"Config file not found: $configFile", e)
        None
      case e: YAMLException =>
        logger.error(s"YAML parsing error in file: $configFile", e)
        None
      case e: Exception =>
        logger.error(s"Unexpected error while parsing YAML: $configFile", e)
        None
    }
  }

}
