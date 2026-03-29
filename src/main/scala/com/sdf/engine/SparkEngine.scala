package com.sdf.engine

import com.sdf.core.dataflow.EngineRunner.logger
import com.sdf.core.dataflow.models.FlowOperation
import com.sdf.core.dataflow.models.FlowOperation.transformToOutputMapping
import com.sdf.core.dataflow.utils.SparkJob

object SparkEngine extends Engine {
  override def run(pipeline: Any, configVariables: String, usage: String): Unit = {
    // pipeline is expected to be a com.sdf.core.dataflow.configparser.Pipeline at runtime
    val pl = pipeline.asInstanceOf[com.sdf.core.dataflow.configparser.Pipeline]

    val spark = SparkJob.createSparkSession(pl.jobName)
    try {
      val jobList = pl.job

      jobList.foreach {
        case input: com.sdf.core.dataflow.configparser.Input => FlowOperation.createInput(input, spark, configVariables)
        case transform: com.sdf.core.dataflow.configparser.Transform => transformToOutputMapping = FlowOperation.createTransformation(transform, spark)
        case output: com.sdf.core.dataflow.configparser.Output => FlowOperation.createOutput(output, spark, transformToOutputMapping, configVariables)
        case _ => logger.error(usage)
      }
    } finally {
      spark.stop()
    }
  }
}
