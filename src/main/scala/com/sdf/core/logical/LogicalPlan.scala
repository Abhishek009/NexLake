package com.sdf.core.logical

import com.sdf.core.dataflow.configparser.{Input, Output, Transform}

case class LogicalPlan(
                input: Option[Input],
                transform: Option[Transform],
                output: Option[Output])


