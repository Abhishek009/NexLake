// Scala
package com.sdf.engine.impl

import com.sdf.engine.Engine

class SampleEngine extends Engine {
  // public no-arg constructor required for ServiceLoader
  override def run(pipeline: Any, configVariables: String, usage: String): Unit = {
    println(s"SampleEngine running pipeline: $pipeline; configVars: $configVariables; usage: $usage")
    // implementation here
  }
}
