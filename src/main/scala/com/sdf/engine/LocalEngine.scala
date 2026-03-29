package com.sdf.engine


object LocalEngine extends Engine {
  override def run(pipeline: Any, configVariables: String, usage: String): Unit = {
    // Simple local fallback - could run a lightweight executor or run Spark in local mode
    println("Running pipeline on LocalEngine (fallback)")
    // Optionally delegate to SparkEngine in local mode:
    SparkEngine.run(pipeline, configVariables, usage)
  }
}
