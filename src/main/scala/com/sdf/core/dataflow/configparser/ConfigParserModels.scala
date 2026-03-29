package com.sdf.core.dataflow.configparser

/**
 * Lightweight config parser models used by JobParser and engines.
 * Field names are chosen to match how JobParser constructs these objects.
 */
case class Input(`type`: String,
                 identifier: String,
                 dfName: String,
                 table: Option[String],
                 schema: Option[String],
                 option: Option[String],
                 path: Option[String],
                 format: Option[String])

case class Transform(dfName: String,
                     tInputs: Option[String],
                     query: String,
                     output: String)

case class Output(dfName: String,
                  `type`: String,
                  identifier: String,
                  path: Option[String],
                  table: Option[String],
                  schema: Option[String],
                  outputFormat: Option[String],
                  option: Option[String],
                  mode: Option[String],
                  partition: Option[String])

case class Pipeline(jobName: String, engine: String, job: List[Any],
                    engineConfig:Option[Map[String, Any]] = None)

