package com.sdf.core.dataflow.lineage.models

case class ReactFlowNode(
                          id: String,
                          `type`: String, // e.g., "input", "default", "output" (ReactFlow types)
                          data: ReactFlowData,
                          position: ReactFlowPosition,
                          style: ReactFlowStyle // Optional, but useful for visual cues
                        )
