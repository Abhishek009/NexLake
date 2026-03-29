package com.sdf.core.dataflow.lineage.models

case class ReactFlowEdge(
                          id: String,
                          source: String,
                          target: String,
                          sourceHandle: Option[String] = None,
                          targetHandle: Option[String] = None,
                          animated: Boolean = true,
                          style: Option[Map[String, String]] = None // Optional: for edge styling
                        )
