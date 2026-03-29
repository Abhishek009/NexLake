package com.sdf.core.dataflow

import com.sdf.engine.EngineFactory
import org.apache.logging.log4j.{LogManager, Logger}

object EngineRunner {
  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def run(pipeline: Any, configVariables: String, usage: String, engineName: String): Unit = {
    EngineFactory.getEngine(engineName).run(pipeline, configVariables, usage)
  }


}
