window.addEventListener("message", (event) => {
  const { action, payload } = event.data;
  switch (action) {
    case "network_components":      
      console.log(3)
      getNetworkComponents(payload);
      break;
    case "all_property_keys":
      const windowId = payload?.id || null;  
      getAllNodeKeys(windowId);
      break;

    case "graph_search":
      graphSearch(payload); // your existing limit function
      break;
    
    case "limit_nodes_amount":
      let setLimitEnabled = payload;
      applyLimit(setLimitEnabled); // your existing limit function
      break;

    case "weight_edges":
      weightEdgesEnabled = payload;
      weightEdges(weightEdgesEnabled); // define logic here
      break;

    case "show_title":
      let showTitleEnabled = payload;
      applyTitleToggle(showTitleEnabled);
      break;

    case "show_label":
      let showLabelsEnabled = payload;
      showNodelabels(showLabelsEnabled);
      break;

    case "edit_infos":
      let editInfosEnabled = payload;
      toggleNodeInfos(editInfosEnabled);
      break;

    case "graph_physics":
      let state = payload;
      networkphysics(state);
      break;

    case "layout_type":
      let type = payload;
      networkLayoutType(type);
      break;

    case "layout_direction":
      let direction = payload;
      networkLayoutDirection(direction);
      break;

    case "sort_method":
      let sort = payload;
      networkLayoutSort(sort);
      break;

    case "new_graph":      
      createNewGraph(payload); // <-- payload should contain {nodes, edges}
      break;

    case "load_graph_url":
      const id = payload?.id || null;  
      const file = payload?.file || null;  
      loadGraphFromFile(id,file);
      break;

    case "graph_snapshot":
      captureGraphSnapshot();
      break;
    
    case "graph_print":
      printGraph();
      break;

    case "export_graph":
      exportGraph(payload);
      break;      

    case "reset_graph":      
      resetGraph(payload);
      break;

    case "fit_graph":
      fit_graph();
      break;

    case "label_nodes_with":
      labelNodesWith(payload);
      break;

    default:
      console.warn("Unknown action:", action);
      break;
  }
});
// Passing a message to the parent window
function messageParent(payload){
  const {id, selectedNodes, selectedEdges} = payload;

  const nodeIds = Object.keys(selectedNodes || {}); // get IDs only
  const edgeIds = selectedEdges || []; // already an array from sender

  console.log("Selected Nodes:", nodeIds);
  console.log("Selected Edges:", edgeIds);

  window.parent.postMessage(
    {
      type: id,
      payload: {
        id: parentWindowId,
        nodes: nodesData.get(nodeIds), // pass array of IDs
        edges: edgesData.get(edgeIds)  // pass array of edge IDs
      }
    },
    "*"
  );
}

// overwrite with the latest options (for runtime changes like Physics/ Layouts)
function updateGraphOption(settingName, value) {
  switch (settingName) {
    case "graph_physics":
      // store boolean directly
      window.currentOptions.physics = !!value;
      break;

    case "layout_type":
      if (value === "hierarchical") {
        window.currentOptions.layout = {
          hierarchical: {
            enabled: true,
            direction: "UD",
            sortMethod: "directed"
          }
        };
      } else {
        window.currentOptions.layout = {};
      }
      break;

    case "layout_direction":
      if (window.currentOptions.layout.hierarchical) {
        window.currentOptions.layout.hierarchical.direction = value;
      }
      break;

    case "layout_sort":
      if (window.currentOptions.layout.hierarchical) {
        window.currentOptions.layout.hierarchical.sortMethod = value;
      }
      break;
  }
}


function areDataSetsEqual(ds1, ds2) {
    const items1 = ds1.get();
    const items2 = ds2.get();

    if (items1.length !== items2.length) return false;

    // compare item by item
    return items1.every(item1 => {
        const item2 = items2.find(i => i.id === item1.id);
        return item2 && JSON.stringify(item1) === JSON.stringify(item2);
    });
}

// overwrite with the latest state
function syncModifiedNodes() {
  if (!modified_nodes) return;
  nodesData.forEach(node => {
    modified_nodes.update({
      ...node,   // keep all current properties (color, label, icon, note, etc.)
    }); 
  });
}

function getAllNodeKeys(id) {
  if (!modified_nodes) {
    console.warn("modified_nodes not ready yet");
    return;
  }
  const keySet = new Set();
  const allNodes = modified_nodes.get();

  const excludeKeys = [
    "size","x","y","vx","vy","index","edges","neighbors",
    "color","shape","borderWidth","borderWidthSelected",
    "borderWidth1","image","imagePadding","iconPath","title","batch_id","input_file","label","nodes_label","colorBehavior"
  ];


  allNodes.forEach(node => {
    Object.keys(node).forEach(key => {
      if (!excludeKeys.includes(key)) {
        keySet.add(key);
      }
    });
  });

  const keysArray = Array.from(keySet);

  // Post message with id + keys
  window.parent.postMessage(
    {
      type: "all_property_keys_response",
      payload: {
        id,          // include the window ID
        keys: keysArray
      }
    },
    "*"
  );

  // Return both, so you can use them directly too
  return { id, keys: keysArray };
}

function restoreSettings(settings) {
  console.log("restoreSettings() called with:", settings);

  if (!settings || !Array.isArray(settings)) {
    console.warn("Invalid settings array, skipping restore.");
    return;
  }

  try {
    console.log("Applying limit...");
    applyLimit({
      key: settings[0],
      sort: settings[1] || "asc",
      amount: Math.min(settings[2] || 25, 300)
    });
    console.log("Limit applied.");

    console.log("Applying weighted edges...");
    weightEdges(settings[3]);
    console.log("Weighted edges applied.");

    console.log("Applying title toggle...");
    applyTitleToggle(settings[4]);
    console.log("Title toggle applied.");

    console.log("Applying node labels...");
    showNodelabels(settings[5]);
    console.log("Node labels applied.");

    console.log("Skipping edit infos at index 6.");

    console.log("Applying physics...");
    networkphysics(settings[7]);
    console.log("Physics applied.");

    console.log("Applying layout type...");
    networkLayoutType(settings[8]);
    console.log("Layout type applied.");

    console.log("Applying label_nodes_with...");
    labelNodesWith(settings[11] || null);
    console.log("Label nodes with applied.");

    if (settings[8] === "hierarchical") {
      console.log("Applying hierarchical layout direction...");
      networkLayoutDirection(settings[9]);
      console.log("Layout direction applied.");

      console.log("Applying layout sort method...");
      networkLayoutSort(settings[10]);
      console.log("Layout sort applied.");
    }

    console.log("restoreSettings() finished.");
  } catch (err) {
    console.error("Error in restoreSettings:", err);
  }
}


function brightenColor(hex, percent) {
  // Remove "#" if present
  hex = hex.replace(/^#/, "");

  // Parse r, g, b
  let r = parseInt(hex.substring(0, 2), 16);
  let g = parseInt(hex.substring(2, 4), 16);
  let b = parseInt(hex.substring(4, 6), 16);

  // Brighten each channel more aggressively
  r = Math.max(0, Math.min(255, r + (255 - r) * (percent / 100)));
  g = Math.max(0, Math.min(255, g + (255 - g) * (percent / 100)));
  b = Math.max(0, Math.min(255, b + (255 - b) * (percent / 100)));

  // Return brightened hex
  return "#" +
    ("0" + Math.round(r).toString(16)).slice(-2) +
    ("0" + Math.round(g).toString(16)).slice(-2) +
    ("0" + Math.round(b).toString(16)).slice(-2);
}

function darkenColor(hex, percent) {
  // Remove "#" if present
  hex = hex.replace(/^#/, "");

  // Parse r,g,b
  let r = parseInt(hex.substring(0,2), 16);
  let g = parseInt(hex.substring(2,4), 16);
  let b = parseInt(hex.substring(4,6), 16);

  // Darken each channel
  r = Math.max(0, Math.min(255, r * (100 - percent) / 100));
  g = Math.max(0, Math.min(255, g * (100 - percent) / 100));
  b = Math.max(0, Math.min(255, b * (100 - percent) / 100));

  // Return darkened hex
  return "#" + 
    ("0" + Math.round(r).toString(16)).slice(-2) +
    ("0" + Math.round(g).toString(16)).slice(-2) +
    ("0" + Math.round(b).toString(16)).slice(-2);
}

function updateNodeLabel(nodeId, text) {
  if (!modified_nodes) {
    modified_nodes = new vis.DataSet(nodesData.get());
  }

  // Update both datasets
  modified_nodes.update({ id: nodeId, label: text });
  nodesData.update({ id: nodeId, label: text });

  // Get the updated node *after* applying changes
  const updatedNode = nodesData.get(nodeId);
  const modified = modified_nodes.get(nodeId);
  //alert(modified.label)
  // Push updated info to React
  showNodeInfos(updatedNode, "state");
}

function UpdateNodeShape(newShape){
    const selectedNodes = network.getSelectedNodes();
    if (selectedNodes.length > 0) {
        selectedNodes.forEach(nodeId => {
        const currentNode = nodesData.get(nodeId);
        // Map custom names to vis.js shapes
        let visShape = 'dot';
        if (newShape === 'circle') visShape = 'dot';
        else if (newShape === 'rectangle') visShape = 'box';
        else if (newShape === 'diamond') visShape = 'diamond';
        // Ensure a proper color object
        let colorObj = currentNode.color;
        if (!colorObj || typeof colorObj === 'string') {
            const baseColor = currentNode.color || '#rgb(255,255,255)';
            const borderColor = darkenColor(baseColor, 20)
            const borderColorHighlight = currentNode.bordercolorHlt || '#111';
            const borderColorHover = currentNode.bordercolorHvr || '#333';
            colorObj = {
                background: baseColor,
                border: borderColor,
                highlight: { background: baseColor, border: borderColorHighlight },
                hover: { background: baseColor, border: borderColorHover }
            };
        }
        // Ensure size is defined (defaults to 15 if missing)
        console.log("colorObj:",colorObj)
        const size = currentNode.size || 15;
            nodesData.update({
                id: nodeId,
                shape: visShape,
                color: colorObj.color,
                size: size,
                borderWidth: currentNode.borderWidth || 1,
                borderWidthSelected: currentNode.borderWidthSelected || 2
            });
        });
    }
}

function updateNodeSize(nodeId, newSize) {
  if (!modified_nodes) {
    modified_nodes = new vis.DataSet(nodesData.get());
  }  

  const node = nodesData.get(nodeId); 
  if (!node) return; // safety check

  const modified = modified_nodes.get(nodeId);
  const original = original_nodes.get(nodeId);

  // Decide label correctly
  let labelToApply;
  if (showLabelsEnabled) {
    // Use modified label if it exists, else fallback to original
    labelToApply = (modified && modified.label && modified.label !== "")
      ? modified.label 
      : (original ? original.label : "");
  } else {
    console.log("s:",modified.label,node.label)
    if (node.label !== "" ){
        labelToApply=node.label;
    }
    else{
        labelToApply = ""; // hide labels        
    }
  }

  const updateNodesDataPayload = {
    ...node,   // keep other props (color, icon, note, etc.)
    id: nodeId,
    size: newSize,
    label: labelToApply //Added as update
  };
  const updateModifiedPayload = {
    ...node,   // keep other props (color, icon, note, etc.)
    id: nodeId,
    size: newSize,
    label: modified.label // Use the modified previous label to update the modified
  };
  nodesData.update(updateNodesDataPayload);
  modified_nodes.update(updateModifiedPayload);
}

// Helper to apply color while preserving border and sizes
function applyColorBehavior(nodeId, color) {
  if (!modified_nodes) {
    modified_nodes = new vis.DataSet(nodesData.get());
  }

  const node = nodesData.get(nodeId);
  if (!node) return;

  const borderColor = darkenColor(color, 20);

  const updatePayload = {
    id: nodeId,
    color: {
      background: color,
      border: borderColor,
      hover: { background: color, border: borderColor },
      highlight: { background: color, border: borderColor }
    },
    size: node.size || 15,
    shape: node.shape || "dot",
    borderWidth: node.borderWidth || 1,
    borderWidthSelected: node.borderWidthSelected || 2
  };

  nodesData.update(updatePayload);
  modified_nodes.update(updatePayload);
}

// Function to update edge color
function updateEdgeColor(edgeId, color) {
    // Initialize working copy once
    if (!modified_edges) {
        modified_edges = new vis.DataSet(edgesData.get());
    }            
    const edge = edgesData.get(edgeId);
    if (!edge) return;

    // Always overwrite with a fresh object so vis.js redraws
    edgesData.update({
        id: edgeId,
        color: {
            color: color,        // main line
            inherit: false,      // prevent fallback to node
            highlight: color,    // on select
            hover: color         // on hover
        }
    });
    modified_edges.update({
        id: edgeId,
        color: {
            color: color,        // main line
            inherit: false,      // prevent fallback to node
            highlight: color,    // on select
            hover: color         // on hover
        }
    });
}

// Function to update node color
function updateNodeColor(nodeId, color) {
    if (!modified_nodes) modified_nodes = new vis.DataSet(nodesData.get());

    const node = nodesData.get(nodeId);
    if (!node) return;

    const behavior = node.colorBehavior || 'individual';
    const affectedNodes = [];

    // Collect nodes to update
    if (behavior === 'individual') {
        affectedNodes.push(nodeId);
    } else if (behavior === 'group') {
        const groupId = node.group;
        nodesData.get().forEach(n => {
            if (n.group === groupId) affectedNodes.push(n.id);
        });
    } else if (behavior === 'link') {
        affectedNodes.push(nodeId);
        affectedNodes.push(...network.getConnectedNodes(nodeId));
    }

    // Batch node updates
    const nodeUpdates = affectedNodes.map(nId => {
        const n = nodesData.get(nId);
        const borderColor = darkenColor(color, 20);
        return {
            id: nId,
            color: {
                background: color,
                border: borderColor,
                hover: { background: color, border: borderColor },
                highlight: { background: color, border: borderColor }
            }
        };
    });
    nodesData.update(nodeUpdates);
    modified_nodes.update(nodeUpdates);

    // Batch edge updates
    const edgeUpdates = [];
    affectedNodes.forEach(nId => {
        const connectedEdges = network.getConnectedEdges(nId);
        connectedEdges.forEach(eId => {
            edgeUpdates.push({
                id: eId,
                color: {
                    color: darkenColor(color, 20),
                    inherit: false,
                    highlight: darkenColor(color, 20),
                    hover: darkenColor(color, 20)
                }
            });
        });
    });
    edgesData.update(edgeUpdates);
    if (modified_edges) modified_edges.update(edgeUpdates);

    network.redraw(); // single redraw

    // Notify parent once
    const allSelectedNodes = {};
    affectedNodes.forEach(nId => allSelectedNodes[nId] = nodesData.get(nId));
    messageParent({ id: "entity_selection", selectedNodes: allSelectedNodes, selectedEdges: '' });
}


function updateNodeDisplay(node) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
    const el = document.getElementById(node.id);
    if (!el) return;

    // Reset transform first
    el.style.transform = 'rotate(0deg)';

    switch(node.shape) {
        case 'circle':
            el.style.borderRadius = '50%';
            break;
        case 'rectangle':
            el.style.borderRadius = '0%';
            break;
        case 'diamond':
            el.style.borderRadius = '0%';
            el.style.transform = 'rotate(45deg)';
            break;
    }

    // Use the actual background color
    const bgColor = (node.color && node.color.background) ? node.color.background : '#EEE';
    const borderColor = darkenColor(color, 20);
    el.style.backgroundColor = bgColor;
    el.style.borderColor = borderColor
}

function applyNodeIcon(nodeId, iconPath) {
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
    const currentNode = modified_nodes.get(nodeId); // use modified_nodes as source of truth
    if (!currentNode) return;

    const updatedNode = {
        ...currentNode,            // keep all properties (size, color, label, note, etc.)
        shape: 'circularImage',
        image: iconPath,
        imagePadding: 10,
        iconPath: iconPath
    };

    // Update both datasets
    modified_nodes.update(updatedNode);
    nodesData.update(updatedNode);
}

function updateNodeNote(nodeId,text) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
    const currentNode = nodesData.get(nodeId);
    if (!currentNode) return;
    // Update node note
    nodesData.update({
        id: nodeId,
        note: text
    });
    modified_nodes.update({
        id: nodeId,
        note: text
    });
}

//Context menu actions
function handleAddNode(x, y) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
  const id = Date.now();
  const pos = network.DOMtoCanvas({ x: x, y: y }); // Convert screen coords to canvas
  nodesData.add({ 
    id: id, 
    label: showLabelsEnabled ?  `Node ${id}` : "", 
    x: pos.x, y: pos.y 
  });
  modified_nodes.add({
    id: id, 
    label: `Node ${id}`, 
    x: pos.x, y: pos.y
  });
  console.log('Node added:', id);
}

function handleDeleteNode(nodeId) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
  if (!nodeId) return;
  const parsedId = isNaN(nodeId) ? nodeId : Number(nodeId); // Handle string/number mismatch
  if (nodesData.get(parsedId)) {
    nodesData.remove(parsedId);
    console.log('Node deleted:', parsedId);
  } else {
    console.warn('Node not found in DataSet:', parsedId);
  }
}

function handleLinkNodes(selectedNodes) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
  if (selectedNodes.length < 2) return;
  for (let i = 0; i < selectedNodes.length; i++) {
    for (let j = i + 1; j < selectedNodes.length; j++) {
      const existingEdges = network.getConnectedEdges(selectedNodes[i])
        .map(edgeId => edgesData.get(edgeId))
        .filter(edge => (edge.from === selectedNodes[j] || edge.to === selectedNodes[j]));
      if (existingEdges.length === 0) {
        edgesData.add({ from: selectedNodes[i], to: selectedNodes[j] });
      }
    }
  }
  console.log('Linked nodes:', selectedNodes);
}

//Handle single node
function handleUnlinkNode(nodeId) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
  if (!nodeId) return;

  // Get connected edge IDs from network
  const connectedEdges = network.getConnectedEdges(nodeId);

  // Only remove edges that exist in edgesData
  const edgesToRemove = connectedEdges.filter(id => edgesData.get(id));

  if (edgesToRemove.length > 0) {
    edgesData.remove(edgesToRemove);    // remove from DataSet
    console.log('Unlinked node:', nodeId, 'Removed edges:', edgesToRemove);
  } else {
    console.log('No matching edges found in edgesData for node:', nodeId);
  }

  // Force redraw just in case
  network.redraw();
}

//Handle multiple nodes
function handleUnlinkNodes(selectedNodes) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
  if (selectedNodes.length < 2) return;

  // Get all edges connected to the selected nodes
  const allEdges = edgesData.get();
  const edgesToRemove = [];

  allEdges.forEach(edge => {
    if (selectedNodes.includes(edge.from) && selectedNodes.includes(edge.to)) {
      edgesToRemove.push(edge.id);
    }
  });

  if (edgesToRemove.length > 0) {
    edgesData.remove(edgesToRemove);
    console.log('Unlinked multiple nodes:', selectedNodes, 'Removed edges:', edgesToRemove);
    network.redraw();
  } else {
    console.log('No matching edges found for selected nodes:', selectedNodes);
  }
}

function handleGroupNodes(selectedNodes,x,y) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
  if (selectedNodes.length < 2) return;

  // Create a unique group ID
  const groupId = "group:" + Date.now();

  // Extract nodes and edges before removing them
  const originalNodes = nodesData.get(selectedNodes);
  const connectedEdges = edgesData.get().filter(edge => 
    selectedNodes.includes(edge.from) || selectedNodes.includes(edge.to)
  );

  // Save original data in groupMap
  groupMap[groupId] = {
    nodes: originalNodes,
    edges: connectedEdges
  };

  // Remove original nodes and edges from the graph
  nodesData.remove(selectedNodes);
  edgesData.remove(connectedEdges.map(e => e.id));
  const pos = network.DOMtoCanvas({ x: x, y: y });
  // Add the group node
  nodesData.add({
    id: groupId,
    label: "Group",
    color: { background: "rgb(255,255,255)" },
    size: 40, // bigger size for visibility
    note: "", // You can customize later
    x: pos.x, 
    y: pos.y
  });
  console.log("Grouped nodes:", selectedNodes, "into", groupId);
}

function handleUngroupNode(groupId) {
    // Initialize working copy once
    if (!modified_nodes) {
        modified_nodes = new vis.DataSet(nodesData.get());
    }            
  if (!groupMap[groupId]) {
    console.warn("No group found for ID:", groupId);
    return;
  }

  const groupData = groupMap[groupId];

  // Remove the group node
  nodesData.remove(groupId);

  // Re-add original nodes and edges
  nodesData.add(groupData.nodes);
  edgesData.add(groupData.edges);

  // Delete from groupMap
  delete groupMap[groupId];

  console.log("Ungrouped:", groupId);
}

function restoreNodeState(nodeId) {
  if (!modified_nodes) return;
  const modNode = modified_nodes.get(nodeId);
  if (!modNode) return;

  // Reapply the modified node properties into nodesData
  nodesData.update(modNode);
}

function applyLimit(limitConfig, nodeListOverride = null) { // Static limit to be 300
  if (!limitConfig) return;

  const { key, sort, amount } = limitConfig;
  let limit = parseInt(amount, 10);
  if (isNaN(limit) || limit <= 0) return;

  // Clamp limit to max 300
  limit = Math.min(limit, 300);

  // Use override list if passed (e.g. search results)
  const allNodes = nodeListOverride || modified_nodes.get();
  const tempNodesData = new vis.DataSet(nodesData.get());

  let visible = [...allNodes];

  // Sort by key if provided
  if (key) {
    visible.sort((a, b) => {
      const valA = a[key];
      const valB = b[key];
      if (typeof valA === "string" && typeof valB === "string") {
        return sort === "desc"
          ? valB.localeCompare(valA)
          : valA.localeCompare(valB);
      } else {
        return sort === "desc" ? (valB ?? 0) - (valA ?? 0) : (valA ?? 0) - (valB ?? 0);
      }
    });
  }

  // Slice the array to the limit
  if (visible.length > limit) {
    visible = visible.slice(0, limit);
  }

  // Update dataset
  nodesData.clear();
  nodesData.add(visible);

  // Reapply labels & size
  nodesData.forEach(node => {
    const modifiedNode = modified_nodes.get(node.id);
    const tempNodeData = tempNodesData.get(node.id);

    nodesData.update({
      ...node,
      label: tempNodeData?.label || modifiedNode?.label || "",
      size: node.size ?? 15
    });
  });
}


function applyTitleToggle(state){
    if (state) {
        nodesData.forEach(node => {   
            const modified = modified_nodes.get(node.id);
            if (modified) {          
                let title = "";
                // Build tooltip from node’s keys and values
                title = Object.entries(modified)
                    .map(([key, value]) => `${key}: ${value}`)
                    .join("\n");
                    nodesData.update({
                        id: node.id,
                        title: title
                    }); 
                }                  
            });        
    }
    else{
        nodesData.forEach(node => {             
        let title = undefined;
            nodesData.update({
                id: node.id,
                title: title
            });
        });
    }
}

function applyLabelToggle() {
  nodesData.forEach(node => {
    if (showLabelsEnabled) {
      const original = modified_nodes.get(node.id);
      nodesData.update({
        id: node.id,
        label: original.label
      });
    } else {
      nodesData.update({ id: node.id, label: "" });
    }
  });
}

function copyNodes(selectedNodeIds) {
  localClipboard = selectedNodeIds.map(id => {
    const node = nodesData.get(id);
    return node ? { ...node } : null;
  }).filter(Boolean);
}

function pasteNodes(canvasPosition) {
  const copiedNodes = Array.isArray(localClipboard) ? localClipboard : [];
  copiedNodes.forEach((originalNode, i) => {
    let initial_node = original_nodes.get(originalNode.id);
    let modified_node = modified_nodes.get(originalNode.id);

    const newNodedatasNode = {
      ...originalNode,
      id: Date.now() + i,
      label: showLabelsEnabled ?  `${originalNode.label}_copy` : "", 
      x: canvasPosition.x + i * 20,
      y: canvasPosition.y + i * 20
    };
    const newModifiedNode = {
      ...originalNode,
      id: Date.now() + i,
      label: initial_node?.label  ? `${initial_node.label}_copy` : modified_node?.label ? `${modified_node.label}_copy` : "",
      x: canvasPosition.x + i * 20,
      y: canvasPosition.y + i * 20
    };
    nodesData.add(newNodedatasNode);
    modified_nodes.add(newModifiedNode);
  });
}

function weightEdges(state) {

  if (!modified_edges || !edgesData) return;
  
  const updates = modified_edges.get().map(edge => ({
    id: edge.id,
    width: state ? edge.value || 3 : 1
  }));

  edgesData.update(updates);

}


function showNodelabels(state) {
  if (!nodesData || !original_nodes) return;

  // Ensure modified_nodes exists but DO NOT mutate labels here
  if (!modified_nodes) {
    modified_nodes = new vis.DataSet(original_nodes.get());
  }

  const updates = [];

  nodesData.forEach(node => {
    const modified = modified_nodes.get(node.id);
    const original = original_nodes.get(node.id);

    const semanticLabel =
      modified?.label ?? original?.label ?? "";

    updates.push({
      id: node.id,
      label: state ? semanticLabel : ""
    });
  });

  nodesData.update(updates);
  network.redraw();
}



function showNodeInfos(node, state) {
  if (!node) return {};

  const excludeKeys = ["size", "x", "y", "vx", "vy", "index", "edges", "neighbors", "color", "shape", "borderWidth", "borderWidthSelected", "borderWidth1", "image", "imagePadding", "iconPath", "title"]; // keys to ignore
  const result = {};
  Object.entries(node).forEach(([key, value]) => {
    if (!excludeKeys.includes(key)) {
      if (key === "label" && (!value || value.trim() === "")) {
        value = "";
      }
      result[key] = value;
    }
  });
  // Send message to parent window
  window.parent.postMessage(
    { type: "nodeProperties", payload: result },
    "*"
  );
  return result;
}

function networkphysics(state) {
  network.setOptions({ physics: { enabled: state ? true : false } });
  updateGraphOption("graph_physics", state);
  network.stabilize();
}

function networkLayoutType(type) {
  if (!network) return;

  if (type === "hierarchical") {
    network.setOptions({
      layout: {
        hierarchical: {
          enabled: true,
          direction: "UD",
          sortMethod: "directed"
        }
      },
      physics: window.currentOptions.physics // use boolean directly
    });
  } else {
    // Reset layout completely to default
    network.setOptions({
      layout: { hierarchical: { enabled: false } },
      physics: window.currentOptions.physics // use boolean directly
    });
  }
  console.log("window.currentOptions.physics")
  // Update stored options
  updateGraphOption("layout_type", type);
  updateGraphOption("layout_direction", "UD");
  updateGraphOption("sort_method", "directed");
  updateGraphOption("graph_physics", window.currentOptions.physics);
  networkphysics(window.currentOptions.physics)

  network.fit();
  network.stabilize();
}


function networkLayoutDirection(direction) {
  if (!network) return;
  network.setOptions({
    layout: { hierarchical: { direction: direction } }
  });
  updateGraphOption("layout_direction", direction);
  network.stabilize();
}

function networkLayoutSort(sort) {
  if (!network) return;
  network.setOptions({
    layout: { hierarchical: { sortMethod: sort } }
  });
  updateGraphOption("layout_sort", sort);
  network.stabilize();
}

function graphSearch(payload) {
  const { id, keyword, option, keys, settings } = payload;

  // If no keyword, restore everything
  if (!keyword || keyword === "") {
    //Restoring to modified state
    nodesData.clear();
    edgesData.clear();
    nodesData.add(modified_nodes.get());
    edgesData.add(modified_edges.get());
    const amt = settings[2] || 25;
    applyLimit({ key: "", sort: "asc", amount: amt });
    network.fit();
    network.redraw();
    window.parent.postMessage(
      { type: "graph_filter_results", payload: {id, results:{nodes: "", edges: "" }} },
      "*"
    );
    return;
  }

  const allNodes = modified_nodes.get();
  const allEdges = modified_edges.get();
  const matchedNodes = new Set();

  const excludeKeys = [
    "size", "x", "y", "vx", "vy", "index", "edges", "neighbors",
    "color", "shape", "borderWidth", "borderWidthSelected",
    "borderWidth1", "image", "imagePadding", "iconPath", "title"
  ];

  // Numeric search support (e.g. >50, <=100)
  const opMatch = keyword.trim().match(/^(>=|<=|>|<|=)\s*(\d+(\.\d+)?)$/);
  let op = null, num = null;
  if (opMatch) {
    op = opMatch[1];
    num = parseFloat(opMatch[2]);
  }

  const keywordLower = keyword.toLowerCase();

  // Find all matching nodes
  for (const node of allNodes) {
    let found = false;

    // Only search through keys explicitly passed
    const searchKeys = keys && keys.length ? keys : Object.keys(node);
    for (const key of searchKeys) {
      if (excludeKeys.includes(key)) continue;
      const value = node[key];
      if (value == null) continue;

      if (op && typeof value === "number") {
        if (
          (op === ">" && value > num) ||
          (op === "<" && value < num) ||
          (op === ">=" && value >= num) ||
          (op === "<=" && value <= num) ||
          (op === "=" && value === num)
        ) {
          found = true;
          break;
        }
      } else {
        const valStr = String(value).toLowerCase();
        if (valStr.includes(keywordLower)) {
          found = true;
          break;
        }
      }
    }

    if (found) matchedNodes.add(node.id);
  }

  // Build visible nodes and edges
  let visibleNodes = allNodes.filter(n => matchedNodes.has(n.id));
  let visibleEdges = [];

  if (option) {
    // Context mode: include neighbors
    const matchedIds = new Set(visibleNodes.map(n => n.id));
    visibleEdges = allEdges.filter(e => matchedIds.has(e.from) || matchedIds.has(e.to));

    const neighborIds = new Set();
    visibleEdges.forEach(e => {
      if (matchedIds.has(e.from)) neighborIds.add(e.to);
      if (matchedIds.has(e.to)) neighborIds.add(e.from);
    });

    visibleNodes = allNodes.filter(n => matchedIds.has(n.id) || neighborIds.has(n.id));
  } else {
    // Strict mode: only edges between matched nodes
    const matchedIds = new Set(visibleNodes.map(n => n.id));
    visibleEdges = allEdges.filter(e => matchedIds.has(e.from) && matchedIds.has(e.to));
  }

  // Apply limit if any
  const limitNum = parseInt(settings[2]||25, 10);
  if (!isNaN(limitNum) && limitNum > 0 && visibleNodes.length > limitNum) {
    visibleNodes = visibleNodes.slice(0, limitNum);
    const limitedIds = new Set(visibleNodes.map(n => n.id));
    visibleEdges = visibleEdges.filter(e => limitedIds.has(e.from) && limitedIds.has(e.to));
  }

  // Update displayed data
  nodesData.clear();
  edgesData.clear();
  nodesData.add(visibleNodes);
  edgesData.add(visibleEdges);

  // Notify parent (optional)
  window.parent.postMessage(
    { type: "graph_filter_results", payload: { id, results:{nodes: visibleNodes, edges: visibleEdges }} },
    "*"
  );
}




async function exportGraph(type) {
  try {
    const graphData = {
      type: "Graph",
      nodes: window.nodesData.get(),
      edges: window.edgesData.get()
    };

    // grab current runtime options
    const networkOptions = window.currentOptions;

    const suggestedName = `LinkxGraph_export_${Date.now()}.${type}`;
    let blob;

    if (type === "json") {
      blob = new Blob(
        [JSON.stringify({ graphData, networkOptions }, null, 2)],
        { type: "application/json" }
      );

    } else if (type === "html") {
      const html = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Linkx Graph</title>
  <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
</head>
<body>
  <div id="mynetwork" style="width:100%; height:100vh;"></div>
  <script>
    const { graphData, networkOptions } = ${JSON.stringify({ graphData, networkOptions }, null, 2)};
    const nodesData = new vis.DataSet(graphData.nodes);
    const edgesData = new vis.DataSet(graphData.edges);
    const container = document.getElementById('mynetwork');
    const data = { nodes: nodesData, edges: edgesData };
    const network = new vis.Network(container, data, networkOptions);
  </script>
</body>
</html>`;
      blob = new Blob([html], { type: "text/html" });

    } else {
      console.warn("Unsupported export type:", type);
      return;
    }

    // Always trigger a browser download (shows in Downloads)
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = suggestedName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    console.log("Graph downloaded via browser:", suggestedName);

  } catch (err) {
    console.error("Error exporting graph:", err);
  }
}

async function loadGraphFromFile(id,file) {
  console.log("called")
  try {
    if (!file || !(file instanceof Blob)) {
      alert("Please select a valid graph file (JSON or HTML).");
      return;
    }

    // Determine file extension safely
    const ext = file.name ? file.name.split(".").pop().toLowerCase() : "";
    let text = (await file.text()).trim(); // remove whitespace/BOM

    let graphData = { nodes: [], edges: [] };
    let networkOptions = {};
    if (ext === "json") {
      const parsed = JSON.parse(text);
      if (
        parsed.graphData &&
        Array.isArray(parsed.graphData.nodes) &&
        Array.isArray(parsed.graphData.edges)
      ) {
        graphData = parsed.graphData;
        networkOptions = parsed.networkOptions || {};
      } else if (
        Array.isArray(parsed.nodes) &&
        Array.isArray(parsed.edges)
      ) {
        graphData = parsed;
        networkOptions = parsed.networkOptions || {};
      } else {
        throw new Error("This JSON file does not contain a valid graph.");
      }

    } else if (ext === "html") {
      // Look for embedded JSON in <script>
      const match = text.match(
        /const\s+\{\s*graphData\s*,\s*networkOptions\s*\}\s*=\s*(\{[\s\S]*?\});/
      );

      if (match) {
        const parsed = JSON.parse(match[1]);
        graphData = parsed.graphData || { nodes: [], edges: [] };
        networkOptions = parsed.networkOptions || {};
      } else {
        throw new Error("Could not extract graph data from HTML file.");
      }

    } else {
      throw new Error("Unsupported file type: " + ext);
    }

    // Validate final nodes/edges arrays
    if (!Array.isArray(graphData.nodes) || !Array.isArray(graphData.edges)) {
      throw new Error("Graph data must contain nodes and edges arrays.");
    }

    // Apply the graph
    createNewGraph({
      nodes: graphData.nodes,
      edges: graphData.edges,
      options: networkOptions
    });
    //Passing property keys
    getAllNodeKeys(id);
    console.log("Graph loaded successfully:", graphData, networkOptions);

  } catch (err) {
    console.error("Error loading graph file:", err);
    alert(
      "Failed to load graph file. It may be corrupted or invalid.\n" +
        err.message
    );
  }
}

//Save function here

function captureGraphSnapshot(filename = `LinkxGraph_snapshot_${Date.now()}.png`) {
  try {
    if (!network || !network.canvas || !network.canvas.frame) {
      alert("Graph is not ready to capture.");
      return;
    }

    // Check if graph is empty
    if (!network.body || !network.body.data || network.body.data.nodes.length === 0) {
      alert("Graph is empty. Nothing to capture.");
      return;
    }

    // Get PNG data URL of the current view
    const dataUrl = network.canvas.frame.canvas.toDataURL("image/png");

    // Trigger download
    const a = document.createElement("a");
    a.href = dataUrl;
    a.download = filename;
    a.click();

    console.log("Graph snapshot saved:", filename);
  } catch (err) {
    console.error("Error capturing graph snapshot:", err);
    alert("Could not capture the graph snapshot.");
  }
}

function printGraph() {
  const canvas = document.querySelector("#mynetwork canvas");

  if (!canvas) {
    alert("Graph canvas not found.");
    return;
  }

  // Check if graph is empty
  if (!network.body || !network.body.data || network.body.data.nodes.length === 0) {
    alert("Graph is empty. Nothing to print.");
    return;
  }

  const dataUrl = canvas.toDataURL("image/png");

  const printWindow = window.open("", "_blank");
  printWindow.document.write(`
    <html>
      <head>
        <title>Print Graph</title>
        <style>
          body { margin: 0; text-align: center; }
          img { max-width: 100%; height: auto; }
        </style>
      </head>
      <body>
        <img src="${dataUrl}" alt="Graph Snapshot"/>
        <script>
          window.onload = function() {
            window.print();
          };
        </script>
      </body>
    </html>
  `);
  printWindow.document.close();
}

function resetGraph(settings) {
  if (!network || !original_nodes || !original_edges) {
    console.warn("Network or original data not ready. Skipping reset.");
    return;
  }

  // Clear current datasets
  nodesData.clear();
  edgesData.clear();

  // Restore original nodes and edges
  nodesData.add(original_nodes.get());
  edgesData.add(original_edges.get());

  // Reset modified datasets
  modified_nodes = new vis.DataSet(original_nodes.get());
  modified_edges = new vis.DataSet(original_edges.get());
  // Apply saved settings immediately
  restoreSettings(settings);
  // Final redraw to ensure everything is applied
  network.redraw();
  //network.stabilize();
}

function fit_graph(){
  if(nodesData.length>0){
    network.fit()
  }
}

function getNetworkComponents(payload){
  // Post message with window id 
  console.log(4,payload)
  window.parent.postMessage(
    {
      type: "network_components",
      payload: {
        id: payload,          // include the window ID
        nodes: nodesData.get(),
        edges: edgesData.get()
      }
    },
    "*"
  );
}

function getNodeValue(node, key) {
  if (typeof key !== "string" || key.trim() === "") return null;
  const normalizedKey = key.trim().toLowerCase();

  const actualKey = Object.keys(node).find(
    k => k.toLowerCase() === normalizedKey
  );
  return actualKey ? node[actualKey] : null;
}

function labelNodesWith(payload) {
  if (!nodesData || !original_nodes) return;

  // Destructure payload safely
  const {labelkey, filterKey, filterSort = "asc", limitAmount = 25} = payload;

  if (typeof labelkey !== "string" || labelkey.trim() === "") return;

  // Initialize modified_nodes if not done already
  if (!modified_nodes) {
    modified_nodes = new vis.DataSet(original_nodes.get());
  }

  const updates = [];

  original_nodes.forEach(node => {
    const value = getNodeValue(node, labelkey);
    const semanticLabel = value != null ? String(value) : "";

    original_nodes.update({ id: node.id, label: semanticLabel });
    modified_nodes.update({ id: node.id, label: semanticLabel });

    updates.push({
      id: node.id,
      label: showLabelsEnabled ? semanticLabel : ""
    });
  });

  nodesData.update(updates);

  // FORCE VISIBILITY SYNC
  if (showLabelsEnabled) showNodelabels(true);

  // Apply the limit *after* labels are set
  const filterAmountClamped = Math.min(limitAmount, 300);
  applyLimit({ key: filterKey, sort: filterSort, amount: filterAmountClamped });
  console.log("limitAmount applied:", filterAmountClamped);

  network.redraw();
}



function initializer(id){
  // Optional: apply default settings (like labels/limits)
  let defaultLimit = 25;
  applyLimit({key:"",sort:"asc",amount:defaultLimit});
  getAllNodeKeys(id);
  network.fit();
  network.redraw();
}
function createNewGraph({ id, nodes = [], edges = [] }) {
  // Clear current state
  console.log("createNewGraph:",id,nodes.length,edges.length);
  nodesData.clear();
  edgesData.clear();

  // Add new datasets
  nodesData.add(nodes);
  edgesData.add(edges);

  // Reset modified datasets to match new ones
  modified_nodes = new vis.DataSet(nodes);
  modified_edges = new vis.DataSet(edges);

  // Store these as the new "originals"
  original_nodes = new vis.DataSet(nodes);
  original_edges = new vis.DataSet(edges);
  initializer(id)
}




