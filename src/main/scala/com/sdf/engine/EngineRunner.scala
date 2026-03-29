/*
// scala
package com.sdf.engine

object EngineRunner {
  def run(pipeline: Any, configVariables: String, usage: String, engineName: String): Unit = {
    EngineFactory.getEngine(engineName).run(pipeline, configVariables, usage)
  }

  // keep backward compatible API
  def run(pipeline: Any, configVariables: String, usage: String): Unit =
    run(pipeline, configVariables, usage, "spark")
}
*/
