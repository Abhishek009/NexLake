package com.sdf.core.logical

case class LogicalTransform(
                      `df-name`: String,
                      t_inputs: Option[String],
                      query: String,
                      output: String)
