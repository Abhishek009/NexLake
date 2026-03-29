package com.sdf.core.dataflow.lineage

import com.sdf.core.dataflow.lineage.models.{NodeDetails, PipelineMetadata, ReactFlowConfig, ReactFlowData, ReactFlowEdge, ReactFlowNode, ReactFlowPosition, ReactFlowStyle}
import com.sdf.core.dataflow.lineage.models._
import com.sdf.core.dataflow.lineage.models.ReactFlowEncoders._
import org.yaml.snakeyaml.Yaml
import io.circe.syntax._

import java.io.{File, FileReader, FileWriter}
import scala.collection.JavaConverters._

object LineageGenerator {

  // Helper function to safely get a value from a Java Map
  private def getOpt[T](map: java.util.Map[String, Any], key: String): Option[T] =
    Option(map.get(key)).map(_.asInstanceOf[T])

  // Helper function to convert Java Map to Scala Map[String, String]
  private def javaMapToScalaStringMap(javaMap: java.util.Map[String, Any]): Map[String, String] = {
    javaMap.asScala.map {
      case (k, v) => k -> v.toString // Convert all values to String for simplicity in config
    }.toMap
  }

  /**
   * Transforms the ETL configuration (parsed from YAML) into a ReactFlow compatible JSON structure.
   * This function is tailored to the provided ETL YAML structure.
   *
   * @param etlConfig The parsed YAML object as a Java Map.
   * @return A `ReactFlowConfig` object containing nodes, edges, and pipeline metadata.
   */
  def transformEtlConfigToReactFlow(etlConfig: java.util.Map[String, Any]): ReactFlowConfig = {
    val jobName = getOpt[String](etlConfig, "jobName").getOrElse("Unnamed Pipeline")
    val engine = getOpt[String](etlConfig, "engine").getOrElse("spark") // Assuming spark as default
    val jobSteps = getOpt[java.util.ArrayList[java.util.Map[String, Any]]](etlConfig, "job")
      .map(_.asScala.toList)
      .getOrElse(List.empty)

    println(s"Generating ReactFlow config for job: $jobName with engine: $engine")
    println(s"Steps: $jobSteps ")
    val nodes = collection.mutable.ListBuffer[ReactFlowNode]()
    val edges = collection.mutable.ListBuffer[ReactFlowEdge]()
    println(s"nodes: ${nodes.toList} ")
    println(s"edges: ${edges.toList} ")
    // Maps df-name to its corresponding node ID and output handle ID.
    // Useful for connecting transforms and outputs to their sources.
    val dfNameToNodeInfo = collection.mutable.Map[String, (String, String)]() // (nodeId, outputHandleId)

    // Initial positioning for nodes - will be adjusted for better layout
    var inputYOffset = 50
    var transformYOffset = 50
    var outputYOffset = 50
    val nodeHeight = 80 // Approximate height for spacing
    val nodeSpacing = 40 // Additional spacing between nodes

    val xInput = 50
    val xTransform = xInput + 250 // X-coordinate for transform nodes
    val xOutput = xTransform + 300 // X-coordinate for output nodes

    // First pass: Create input and transform nodes and map their df-names
    jobSteps.foreach { step =>
      if (step.containsKey("input")) {
        val input = step.get("input").asInstanceOf[java.util.Map[String, Any]]
        val dfName = getOpt[String](input, "df-name").getOrElse(s"input-${nodes.size}")
        val inputType = getOpt[String](input, "type").getOrElse("unknown")
        val identifier = getOpt[String](input, "identifier").getOrElse("N/A")
        val label = s"Input: $dfName (${inputType.capitalize})"
        val nodeId = dfName // Use df-name as node ID for inputs

        nodes += ReactFlowNode(
          id = nodeId,
          `type` = "inputNode", // Custom ReactFlow node type
          data = ReactFlowData(
            label = label,
            details = NodeDetails(
              `type` = "input",
              description = s"Reads data from $inputType source.",
              Identifier = identifier,
              config = javaMapToScalaStringMap(input)
            )
          ),
          position = ReactFlowPosition(x = xInput, y = inputYOffset),
          style = ReactFlowStyle(
            backgroundColor = "#D1FAE5", // Green-ish
            border = "1px solid #34D399",
            borderRadius = "8px",
            padding = "10px",
            boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
            minWidth = "150px",
            textAlign = "center"
          )
        )
        dfNameToNodeInfo(dfName) = (nodeId, "outputHandle") // Input nodes have one source handle
        inputYOffset += (nodeHeight + nodeSpacing)

      } else if (step.containsKey("transform")) {
        val transform = step.get("transform").asInstanceOf[java.util.Map[String, Any]]
        val dfName = getOpt[String](transform, "df-name").getOrElse(s"transform-${nodes.size}")
        val outputDfName = getOpt[String](transform, "output").getOrElse(s"output-of-${dfName}")
        val label = s"Transform: $dfName"
        val nodeId = dfName // Use df-name as node ID for transforms
        val tInputsString = getOpt[String](transform, "t_inputs").getOrElse("")
        val tInputsList = tInputsString.split(',').map(_.trim).filter(_.nonEmpty).toList

        nodes += ReactFlowNode(
          id = nodeId,
          `type` = "transformNode", // Custom ReactFlow node type
          data = ReactFlowData(
            label = label,
            details = NodeDetails(
              `type` = "transform",
              description = "Performs data transformation.",
              Identifier = getOpt[String](transform, "identifier").getOrElse("N/A"),
              config = javaMapToScalaStringMap(transform)
            ),
            tInputs = Some(tInputsList) // Pass the list of inputs directly to ReactFlowData
          ),
          position = ReactFlowPosition(x = xTransform, y = transformYOffset),
          style = ReactFlowStyle(
            backgroundColor = "#DBEAFE", // Blue-ish
            border = "1px solid #60A5FA",
            borderRadius = "8px",
            padding = "10px",
            boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
            minWidth = "150px",
            textAlign = "center"
          )
        )
        // IMPORTANT: Add the transform's df-name itself to dfNameToNodeInfo
        // so it can be referenced as a source for other transforms (like t1 for t2)
        dfNameToNodeInfo(dfName) = (nodeId, "outputHandle") // Transform nodes have one source handle for their own df-name
        dfNameToNodeInfo(outputDfName) = (nodeId, "outputHandle") // Also map its output df-name
        transformYOffset += (nodeHeight + nodeSpacing)
      }
    }

    // Second pass: Create output nodes and connect edges
    // Reset yOffset for output nodes to align them
    outputYOffset = 50
    jobSteps.foreach { step =>
      if (step.containsKey("output")) {
        val output = step.get("output").asInstanceOf[java.util.Map[String, Any]]
        val dfName = getOpt[String](output, "df-name").getOrElse(s"output-${nodes.size}")
        val outputType = getOpt[String](output, "type").getOrElse("unknown")
        val label = s"Output: $dfName (${outputType.capitalize})"
        val nodeId = s"sink-${dfName}-${nodes.size}" // Ensure unique ID for multiple outputs from same df-name

        nodes += ReactFlowNode(
          id = nodeId,
          `type` = "outputNode", // Custom ReactFlow node type
          data = ReactFlowData(
            label = label,
            details = NodeDetails(
              `type` = "output",
              description = s"Writes data to $outputType destination.",
              Identifier = getOpt[String](output, "identifier").getOrElse("N/A"),
              config = javaMapToScalaStringMap(output)
            )
          ),
          position = ReactFlowPosition(x = xOutput, y = outputYOffset),
          style = ReactFlowStyle(
            backgroundColor = "#FFEDD5", // Orange-ish
            border = "1px solid #FB923C",
            borderRadius = "8px",
            padding = "10px",
            boxShadow = "0 2px 4px rgba(0,0,0,0.1)",
            minWidth = "150px",
            textAlign = "center"
          )
        )

        // Connect output to its source (transform or input)
        dfNameToNodeInfo.get(dfName).foreach { case (sourceNodeId, sourceHandleId) =>
          edges += ReactFlowEdge(
            id = s"e-${sourceNodeId}-${nodeId}",
            source = sourceNodeId,
            target = nodeId,
            sourceHandle = Some(sourceHandleId),
            targetHandle = Some("inputHandle"), // Output nodes have one target handle
            style = Some(Map("stroke" -> "#FB923C")) // Orange for output edges
          )
        }
        outputYOffset += (nodeHeight + nodeSpacing)
      }
    }

    // Third pass: Connect transform inputs
    jobSteps.foreach { step =>
      if (step.containsKey("transform")) {
        val transform = step.get("transform").asInstanceOf[java.util.Map[String, Any]]
        val transformDfName = getOpt[String](transform, "df-name").getOrElse("")
        val tInputsString = getOpt[String](transform, "t_inputs").getOrElse("")
        // Handle multiple t_inputs if comma-separated
        val tInputs = tInputsString.split(',').map(_.trim).filter(_.nonEmpty).toList

        val transformNodeId = transformDfName // Assuming transform df-name is its node ID

        tInputs.foreach { inputDfName =>
          val sourceNodeInfo = dfNameToNodeInfo.get(inputDfName)
          if (sourceNodeInfo.isDefined && transformNodeId.nonEmpty) {
            edges += ReactFlowEdge(
              id = s"e-${sourceNodeInfo.get._1}-${transformNodeId}-${inputDfName}",
              source = sourceNodeInfo.get._1,
              target = transformNodeId,
              sourceHandle = Some(sourceNodeInfo.get._2),
              targetHandle = Some(s"target-${inputDfName}"), // Target handle for transform inputs now matches the dynamic ID in App.js
              animated = true,
              style = Some(Map("stroke" -> "#60A5FA"))
            )
          }
        }
      }
    }

    ReactFlowConfig(
      nodes = nodes.toList,
      edges = edges.toList,
      pipelineMetadata = PipelineMetadata(
        name = jobName,
        description = s"Data pipeline generated from ETL YAML (engine: $engine)."
      )
    )
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: LineageGenerator <path/to/etl_pipeline.yml> <path/to/output_config.json>")
      System.exit(1)
    }

    val yamlFilePath = args(0)
    val outputJsonPath = args(1)

    try {
      println(s"Reading YAML from: $yamlFilePath")
      val yaml = new Yaml()
      val yamlFile = new File(yamlFilePath)
      if (!yamlFile.exists()) {
        throw new RuntimeException(s"YAML file not found: $yamlFilePath")
      }
      val reader = new FileReader(yamlFile)
      val etlConfig = yaml.load(reader).asInstanceOf[java.util.Map[String, Any]]
      reader.close()

      println("Transforming ETL config to ReactFlow format...")
      val reactFlowConfig = transformEtlConfigToReactFlow(etlConfig)

      println(s"Writing ReactFlow JSON to: $outputJsonPath")
      val jsonString = reactFlowConfig.asJson.spaces2 // Pretty print JSON
      val writer = new FileWriter(outputJsonPath)
      writer.write(jsonString)
      writer.close()

      println("Lineage JSON generated successfully!")

    } catch {
      case e: Exception =>
        System.err.println(s"Error generating lineage: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }
}
