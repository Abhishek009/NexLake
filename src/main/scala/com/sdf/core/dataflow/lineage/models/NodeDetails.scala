package com.sdf.core.dataflow.lineage.models

case class NodeDetails(
                        `type`: String, // e.g., "input", "transform", "output" (ETL types)
                        description: String,
                        Identifier: String,
                        config: Map[String, String] // Use Map[String, String] for simpler config display
                      )
