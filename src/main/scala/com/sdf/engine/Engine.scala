package com.sdf.engine

trait Engine {
  /** human friendly name; default to simple class name */
  def name: String = this.getClass.getSimpleName
  def run(pipeline: Any, configVariables: String, usage: String): Unit
}
