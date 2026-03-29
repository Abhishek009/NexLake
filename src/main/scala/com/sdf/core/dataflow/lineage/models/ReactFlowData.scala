package com.sdf.core.dataflow.lineage.models

case class ReactFlowData(
                          label: String,
                          details: NodeDetails,
                          tInputs: Option[List[String]] = None // Added for transform nodes to define multiple input handles
                        )
