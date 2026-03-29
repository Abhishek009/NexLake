import React, { useState, useEffect, useCallback } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  Handle, // Import Handle
  Position, // Import Position
} from 'reactflow';
import 'reactflow/dist/style.css';
import yaml from 'js-yaml';

// Material-UI Imports
import { Box, Paper, Typography, Button,Grid } from '@mui/material';

// Ensure these are loaded via CDN or npm install in a real project
// For Canvas environment, these are usually available or need CDN links
// <script src="https://unpkg.com/@mui/material@latest/umd/material-ui.production.min.js"></script>
// <script src="https://unpkg.com/@emotion/react@latest/umd/emotion-react.production.min.js"></script>
// <script src="https://unpkg.com/@emotion/styled@latest/umd/emotion-styled.production.min.js"></script>


// Define custom node styles for different types outside the component
const nodeStyles = {
  input: {
    backgroundColor: '#D1FAE5', // Green-ish
    color: '#065F46',
    border: '1px solid #34D399',
    borderRadius: '8px',
    padding: '10px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
  },
  transform: {
    backgroundColor: '#DBEAFE', // Blue-ish
    color: '#1E40AF',
    border: '1px solid #60A5FA',
    borderRadius: '8px',
    padding: '10px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
  },
  output: {
    backgroundColor: '#FFEDD5', // Orange-ish
    color: '#9A3412',
    border: '1px solid #FB923C',
    borderRadius: '8px',
    padding: '10px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
  },
};

const initialYaml = `
jobName: DatabricksJob
engine: databricks
job:
  - input:
      df-name: dbread
      type: databricks
      identifier: dev
      table: deliveries
      schema: ipl

  - input:
      df-name: fileread
      type: file
      identifier: local
      format: csv
      option: |
        header=true
        delimiter=,
      path: D:/SampleData/IPL/matches.csv

  - transform:
      df-name: t1
      t_inputs: dbread,fileread
      query: "--file D:/Google_Drive_Rahul/GitHub/SparkDataFlow/sql/sample.sql"
      output: out-01

  - transform:
      df-name: t2
      t_inputs: dbread,fileread
      query: "--file D:/Google_Drive_Rahul/GitHub/SparkDataFlow/sql/sample.sql"
      output: out-02

  - output:
      df-name: out-01
      type: file
      identifier: adfss
      partition: ods
      output_format: csv
      option: |
        mergeSchema=true
        doSomething=yes
      path: D:\\SampleData\\mysql_sample_data_1

  - output:
      df-name: out-02
      type: databricks
      identifier: dev
      output_format: csv
      schema: deliveries
      table: iploutput

  - output:
      df-name: out-02
      type: databricks
      identifier: dev
      output_format: csv
      schema: deliveries
      table: iploutput
`;

// Custom Input Node Component
const InputNode = ({ data }) => {
  return (
    <div style={nodeStyles.input}>
      <div>{data.label}</div>
      {/* Single source handle at the right */}
      <Handle type="source" position={Position.Right} id="outputHandle" style={{ background: '#555' }} />
    </div>
  );
};

// Custom Transform Node Component
const TransformNode = ({ data }) => {
  // data.tInputs is an array of input df-names for this transform
  // We create a target handle for each input
  return (
    <div style={nodeStyles.transform}>
      {/* Multiple target handles at the left for inputs, distributed vertically */}
      {data.tInputs && data.tInputs.map((inputDfName, index) => (
        <Handle
          key={`target-${inputDfName}`} // Unique key for React list rendering
          type="target"
          position={Position.Left}
          id={`target-${inputDfName}`} // Unique ID for each target handle
          // Distribute handles vertically
          style={{ background: '#555', top: `${(index + 1) * 100 / (data.tInputs.length + 1)}%` }}
        />
      ))}
      <div>{data.label}</div>
      {/* Single source handle at the right for output */}
      <Handle type="source" position={Position.Right} id="outputHandle" style={{ background: '#555' }} />
    </div>
  );
};

// Custom Output Node Component
const OutputNode = ({ data }) => {
  return (
    <div style={nodeStyles.output}>
      {/* Single target handle at the left */}
      <Handle type="target" position={Position.Left} id="inputHandle" style={{ background: '#555' }} />
      <div>{data.label}</div>
    </div>
  );
};

// Define nodeTypes object to map custom node names to components
const nodeTypes = {
  inputNode: InputNode,
  transformNode: TransformNode,
  outputNode: OutputNode,
};

const App = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [error, setError] = useState(null);
  const [selectedNodeData, setSelectedNodeData] = useState(null); // State to store selected node data

  // Function to parse YAML and generate nodes/edges
  const generateFlow = useCallback(() => {
    try {
      setError(null);
      const data = yaml.load(initialYaml); // Use initialYaml directly
      const jobSteps = data.job;

      let newNodes = [];
      let newEdges = [];
      let yOffset = 0;
      const nodeHeight = 80;
      const nodeWidth = 200;
      const xInput = 50;
      const xTransform = xInput + nodeWidth + 100;
      const xOutput = xTransform + nodeWidth + 100;

      // Map to store df-name to node ID and its default output handle ID
      const dfNameToNodeInfo = {}; // Stores { nodeId: '...', outputHandleId: '...' }

      let inputCount = 0;
      let transformCount = 0;
      let outputCount = 0;

      // First pass: Create nodes and map df-names
      jobSteps.forEach((step) => {
        if (step.input) {
          const dfName = step.input['df-name'];
          const id = `input-${dfName}-${inputCount++}`;
          newNodes.push({
            id: id,
            type: 'inputNode', // Use custom input node type
            data: { label: `Input: ${dfName}\n(${step.input.type})`, dfName: dfName, details: step.input }, // Pass full input details
            position: { x: xInput, y: yOffset },
            style: nodeStyles.input,
          });
          dfNameToNodeInfo[dfName] = { nodeId: id, outputHandleId: 'outputHandle' }; // Input nodes have one source handle
          yOffset += nodeHeight + 20;
        } else if (step.transform) {
          const dfName = step.transform['df-name'];
          const outputDfName = step.transform.output;
          const tInputsArray = step.transform.t_inputs.split(',').map(s => s.trim());
          const id = `transform-${dfName}-${transformCount++}`;
          newNodes.push({
            id: id,
            type: 'transformNode', // Use custom transform node type
            data: { label: `Transform: ${dfName}`, tInputs: tInputsArray, details: step.transform }, // Pass full transform details
            position: { x: xTransform, y: yOffset },
            style: nodeStyles.transform,
          });
          dfNameToNodeInfo[outputDfName] = { nodeId: id, outputHandleId: 'outputHandle' }; // Transform nodes have one source handle for their output
          yOffset += nodeHeight + 20;
        }
      });

      // Reset yOffset for output nodes to align them
      yOffset = 0;

      // Second pass: Create output nodes and connect edges
      jobSteps.forEach((step) => {
        if (step.output) {
          const dfName = step.output['df-name'];
          const outputId = `output-${dfName}-${outputCount++}`; // Unique ID for each output instance
          newNodes.push({
            id: outputId,
            type: 'outputNode', // Use custom output node type
            data: { label: `Output: ${dfName}\n(${step.output.type})`, dfName: dfName, details: step.output }, // Pass full output details
            position: { x: xOutput, y: yOffset },
            style: nodeStyles.output,
          });

          // Connect output to its source (transform or input)
          const sourceNodeInfo = dfNameToNodeInfo[dfName];
          if (sourceNodeInfo) {
            newEdges.push({
              id: `e-${sourceNodeInfo.nodeId}-${outputId}`,
              source: sourceNodeInfo.nodeId,
              target: outputId,
              sourceHandle: sourceNodeInfo.outputHandleId, // Connect from the source handle of the previous node
              targetHandle: 'inputHandle', // Connect to the target handle of the output node
              animated: true,
              style: { stroke: '#FB923C' }, // Orange for output edges
            });
          }
          yOffset += nodeHeight + 20;
        }
      });

      // Third pass: Connect transform inputs
      jobSteps.forEach((step) => {
        if (step.transform) {
          const transformDfName = step.transform['df-name'];
          // Find the transform node we just created to get its ID
          const transformNode = newNodes.find(node => node.data.label.includes(`Transform: ${transformDfName}`));
          const transformNodeId = transformNode.id;
          const tInputs = step.transform.t_inputs.split(',').map(s => s.trim());

          tInputs.forEach(inputDfName => {
            const sourceNodeInfo = dfNameToNodeInfo[inputDfName];
            if (sourceNodeInfo && transformNodeId) {
              newEdges.push({
                id: `e-${sourceNodeInfo.nodeId}-${transformNodeId}-${inputDfName}`, // Unique edge ID for each input
                source: sourceNodeInfo.nodeId,
                target: transformNodeId,
                sourceHandle: sourceNodeInfo.outputHandleId, // Connect from the source handle of the input node
                targetHandle: `target-${inputDfName}`, // Connect to the specific target handle of the transform node
                animated: true,
                style: { stroke: '#60A5FA' }, // Blue for transform edges
              });
            }
          });
        }
      });

      setNodes(newNodes);
      setEdges(newEdges);
    } catch (e) {
      setError(`Error parsing YAML: ${e.message}`);
      console.error("Error parsing YAML:", e);
    }
  }, []);

  useEffect(() => {
    generateFlow();
  }, [generateFlow]);



  // Handle node click to display details in the dedicated space
  const onNodeClick = useCallback((event, node) => {
    // Pass the whole node object, not just node.data
    setSelectedNodeData(node);
  }, []);


// ...existing code...
  const renderInfoPanelContent = (node) => {
    if (!node || !node.data || !node.data.details) {
      return (
        <Box sx={{ p: 2, textAlign: 'center', color: 'text.secondary', width: '90%' }}>
          <Typography variant="h6" component="p" sx={{ mb: 1, color: 'inherit' }}>
            Select a Node for Details
          </Typography>
          <Typography variant="body2" sx={{ color: 'inherit' }}>
            Click on any node in the diagram to view its details here.
          </Typography>
        </Box>
      );
    }

    const details = node.data.details;
    const type = node.type;

    // Prepare key-value pairs for grid display
    let infoRows = [];
    if (type === 'inputNode') {
      infoRows = [
        { label: 'Type', value: details.type },
        { label: 'Identifier', value: details.identifier },
        details.table && { label: 'Table', value: details.table },
        details.schema && { label: 'Schema', value: details.schema },
        details.format && { label: 'Format', value: details.format },
        details.path && { label: 'Path', value: details.path },
      ].filter(Boolean);
    } else if (type === 'transformNode') {
      infoRows = [
        { label: 'Input DFs', value: details.t_inputs },
        { label: 'Output DF', value: details.output },
        //details.query && { label: 'Query', value: details.query },
      ].filter(Boolean);
    } else if (type === 'outputNode') {
      infoRows = [
        { label: 'Type', value: details.type },
        { label: 'Identifier', value: details.identifier },
        { label: 'Output Format', value: details.output_format },
        details.partition && { label: 'Partition', value: details.partition },
        details.schema && { label: 'Schema', value: details.schema },
        details.table && { label: 'Table', value: details.table },
        details.path && { label: 'Path', value: details.path },
      ].filter(Boolean);
    }

    return (
      <Box sx={{ p: 2, position: 'relative', width: '90%' }}>
        <Typography variant="h6" component="h3" sx={{ fontWeight: 'bold', mb: 2, color: 'primary.dark', textAlign: 'center' }}>
          {node.data.label.split('\n')[0]}
        </Typography>
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: 'max-content 1fr',
            gap: 2,
            alignItems: 'left',
            mb: 1,
            px: 1,
          }}
        >
          {infoRows.map((row, idx) => (
            <React.Fragment key={idx}>
              <Typography
                variant="body2"
                sx={{
                  fontWeight: 600,
                  color: '#0e7490',
                  textAlign: 'left',
                  pr: 2,
                  whiteSpace: 'nowrap',
                }}
              >
                {row.label}:
              </Typography>
              <Typography
                variant="body2"
                sx={{
                  color: '#334155',
                  fontWeight: 500,
                  wordBreak: 'break-all',
                  background: '#f1f5f9',
                  borderRadius: 1,
                  px: 1,
                  py: 0.5,
                }}
              >
                {row.value}
              </Typography>
            </React.Fragment>
          ))}
        </Box>
        {/* Special handling for options/query as a code block */}
        {type === 'inputNode' && details.option && (
          <Box sx={{
            display: 'grid',
            gridTemplateColumns: 'max-content 1fr',
            gap: 2,
            alignItems: 'left',
            mb: 1,
            px: 1,
          }}>
            <Typography variant="body2"
            sx={{ fontWeight: 600,
                  color: '#0e7490',
                  textAlign: 'left',
                  pr: 3,
                  whiteSpace: 'nowrap',}}>
              Options:
            </Typography>
            <Typography
              component="pre" variant="body2"
              sx={{
                bgcolor: '#f0fdfa',
                p: 1.5,
                borderRadius: 2,
                fontSize: '0.85rem',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-all',
                border: '1px solid #bae6fd',
                mt: 0.5,
                boxShadow: 'inset 0 1px 3px rgba(6,182,212,0.08)',
                fontFamily: 'monospace',
                color: '#0e7490',

              }}
            >
              {details.option}
            </Typography>
          </Box>
        )}
        {type === 'transformNode' && details.query && (
          <Box sx={{ display: 'grid',
            gridTemplateColumns: 'max-content 1fr',
            gap: 4,
            alignItems: 'left',
            mb: 1,
            px: 1, }}>
            <Typography variant="body2" sx={{ fontWeight: 600,
                  color: '#0e7490',
                  textAlign: 'left',
                  pr: 3,
                  whiteSpace: 'nowrap', }}>
              Query:
            </Typography>
            <Typography
              component="pre"
              sx={{
                 bgcolor: '#f0fdfa',
                p: 1.5,
                borderRadius: 2,
                fontSize: '0.85rem',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-all',
                border: '1px solid #bae6fd',
                mt: 0.5,
                boxShadow: 'inset 0 1px 3px rgba(6,182,212,0.08)',
                fontFamily: 'monospace',
                color: '#0e7490',
              }}
            >
              {details.query}
            </Typography>
          </Box>
        )}
        {type === 'outputNode' && details.option && (
          <Box sx={{ display: 'grid',
            gridTemplateColumns: 'max-content 1fr',
            gap: 6,
            alignItems: 'left',
            mb: 1,
            px: 1,}}>
            <Typography variant="body2" sx={{ fontWeight: 600,
                  color: '#0e7490',
                  textAlign: 'left',
                  pr: 3,
                  whiteSpace: 'nowrap', }}>
              Options:
            </Typography>
            <Typography
              component="pre"
              sx={{
                bgcolor: '#f0fdfa',
                p: 1.5,
                borderRadius: 2,
                fontSize: '0.85rem',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-all',
                border: '1px solid #bae6fd',
                mt: 0.5,
                boxShadow: 'inset 0 1px 3px rgba(6,182,212,0.08)',
                fontFamily: 'monospace',
                color: '#0e7490',
              }}
            >
              {details.option}
            </Typography>
          </Box>
        )}
      </Box>
    );


  };
// ...existing code...



 return (
     <div className="flex flex-col h-screen bg-gray-100 font-sans p-4  ">
       <h1 className="text-3xl font-bold text-gray-800 mb-4 text-center w-full">Data Flow Diagram</h1>
       {/* Main container for ReactFlow and the info panel */}
       <div className="flex-grow flex flex-col bg-white rounded-lg shadow-md overflow-hidden" style={{ height: '90vh' }}>
         {/* ReactFlow component fills most of the height */}
         <div className="flex-grow" style={{ height: 'calc(100% - 200px)' }}> {/* Fixed height for ReactFlow */}
           <ReactFlow
             nodes={nodes}
             edges={edges}
             onNodesChange={onNodesChange}
             onEdgesChange={onEdgesChange}
             //onConnect={onConnect}
             onNodeClick={onNodeClick}
             fitView
             className="rounded-lg"
             style={{ width: '100%', height: '100%' }}
             nodeTypes={nodeTypes}
           >
             <MiniMap />
             <Controls />
             <Background variant="dots" gap={12} size={1} />
           </ReactFlow>
         </div>




           <Paper
             elevation={16}
             sx={{
               width: 'auto',
               height: '220px',
               //background: 'linear-gradient(90deg,rgb(197, 215, 218) 0%,rgb(216, 230, 219) 100%)',
               borderRadius: '8px 8px 0 0',
               borderTop: '2px solid #06b6d4',
               boxShadow: '0 -8px 10px 0 rgba(6,182,212,0.18), 0 -1.5px 0 #06b6d4',
             display: 'flex',
               flexShrink: 0,
               overflowY: 'auto',
               alignItems: 'flex-start',
              position: 'relative',
              px: 4,
              py: 3,
              gap: 4,
              zIndex: 10,
              transition: 'box-shadow 0.2s',
            }}
          >
            <Box
              sx={{
                width: '100%',
                maxWidth: 'auto',
                mx: 'auto',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'flex-start',
                justifyContent: 'flex-start',
                height: '100%',
                overflowY: 'auto',
                position: 'relative',
              }}
            >
              {/* Close button */}
               {selectedNodeData && selectedNodeData.data && selectedNodeData.data.details && (
              <Button
                onClick={() => setSelectedNodeData(null)}
                sx={{
                  position: 'absolute',
                  top: 8,
                  right: 8,
                  minWidth: 0,
                  width: 32,
                  height: 32,
                  borderRadius: '50%',
                  color: '#06b6d4',
                  bgcolor: '#e0f7fa',
                  fontWeight: 'bold',
                  fontSize: 22,
                  boxShadow: '0 1px 4px rgba(6,182,212,0.12)',
                  '&:hover': { bgcolor: '#b2f5ea', color: '#0e7490' },
                }}
              >
                ×
              </Button>
               )}
              {/* Render the info panel content based on the selected node */}
              {renderInfoPanelContent(selectedNodeData)}
            </Box>
          </Paper>

      </div>
      {error && (
        <div className="mt-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded-md">
          <p className="font-semibold">Error:</p>
          <p>{error}</p>
        </div>
      )}
    </div>
   );


};

export default App;
