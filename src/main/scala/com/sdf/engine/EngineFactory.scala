// scala
package com.sdf.engine

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

object EngineFactory {
  // Discover Engine implementations via ServiceLoader (allows adding engines without code changes)
  private lazy val discoveredEngines: Map[String, Engine] = {
    try {
      val loader = ServiceLoader.load(classOf[Engine])
      loader.iterator().asScala.toSeq.map(e => e.name.toLowerCase -> e).toMap
    } catch {
      case _: Throwable => Map.empty[String, Engine]
    }
  }

  // Built-in engines (fallback)
  private val builtinEngines: Map[String, Engine] = Map(
    "spark" -> SparkEngine,
    "local" -> LocalEngine,
    "duckdb" -> DuckDBEngine
  )

  // Merge discovered engines with builtins (discovered overrides builtins)
  private lazy val engines: Map[String, Engine] = builtinEngines ++ discoveredEngines

  def getEngine(name: String): Engine = engines.getOrElse(name.toLowerCase, engines.getOrElse("spark", SparkEngine))
}
