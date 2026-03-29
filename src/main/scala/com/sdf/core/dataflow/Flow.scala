package com.sdf.core.dataflow

import org.apache.logging.log4j.LogManager
import scala.io.Source

object Flow {

  def main(args: Array[String]): Unit = {
    val usage: String =
      """Looks like you have pass some yaml key which is not acceptable by the SparkDataFlow
        | Acceptable tags are input,transform,output""".stripMargin

    val logger = LogManager.getLogger(getClass.getSimpleName)
    val conf = new CLIConfigParser(args)
    val argsMap: scala.collection.mutable.Map[String, String] = conf.getArgMap()
    val jobProcessFile = argsMap.getOrElse("jobFile", "")
    val jobConfigFile = argsMap.getOrElse("jobConfig", "")
    val configVariables = argsMap.getOrElse("configFile", "")
    val paramFiles = argsMap.getOrElse("paramFile","")



    logger.info(s"Job yaml file: $jobProcessFile")
    val scalaFileContents = Source.fromFile(jobProcessFile).getLines()
    scalaFileContents.foreach(line => logger.info(line))
    logger.info(s"Job Configuration file: ${jobConfigFile}")
    logger.info(s"Configuration file: ${configVariables}")
    logger.info(s"Parameter file: ${paramFiles}")

    val pipeline = JobParser.parseJobConfig(jobProcessFile, paramFiles)
    val engineName = try{
     if(pipeline.engine.nonEmpty){
       logger.info(s"Engine specified in job yml file: ${pipeline.engine}")
       pipeline.engine
     }else{
       val error = "No engine specified in job yaml file." +
         "Please specify a engine"
       logger.error(error)
       throw new IllegalArgumentException(error)
     }

    }catch {
      case e:IllegalArgumentException =>
        logger.error(s"Engine Configuration error: ${e.getMessage}")
        throw e
      case e:Exception =>
        logger.error(s"Unexpected error while determining engine ${e.getMessage}",e)
        throw new RuntimeException("Failed to determine execution engine",e)
    }


    EngineRunner.run(pipeline, configVariables, usage, engineName)


  }

}
