package com.sdf.core.logical

case class LogicalOutput(
                   `df-name`: String,
                   `type`: String,
                   identifier: String,
                   var path: Option[String],
                   var table: Option[String],
                   var schema: Option[String],
                   var output_format: Option[String],
                   var option: Option[String],
                   var mode:Option[String],
                   var partition:Option[String]) {
  path = Option(path.getOrElse(""))
  table = Option(table.getOrElse(""))
  schema = Option(schema.getOrElse(""))
  output_format = Option(output_format.getOrElse("parquet"))
  mode = Option(mode.getOrElse("overwrite"))
  option = Option(option.getOrElse(""))
}
