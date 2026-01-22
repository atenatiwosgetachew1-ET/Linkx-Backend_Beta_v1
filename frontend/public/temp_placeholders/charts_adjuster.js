window.addEventListener("message", (event) => {
  const { action, payload } = event.data;
  console.log("called_Chart_adjuster:",action,payload)
  switch (action) {
    case "network_components":      
      console.log("11:",payload)
      const {nodes, edges} = payload;
      storeNetworkComponents(nodesData=nodes,edgesdata=edges);
      break;  
    case "create_chart":      
      const metricName = payload;
      init(action,metricName,null, null, null, null)
      break;
    case "updateSelection":      
      console.log(27)
      //const {selectedNodes, selectedEdges} = payload;
      selectedNodes = payload.selectedNodes;
      selectedEdges = payload.selectedEdges;
      init(action,null,null,null,selectedNodes,selectedEdges)
      break;
    case "chart_snapshot":      
      captureChartSnapshot();
      break;
    case "chart_print":      
      printChart();
      break;
    case "chart_reset":      
      resetCharts();
      break;
    
  }
});

  /* ----------------- Utilities ----------------- */
  function sanitizeData(obj){
    if(!obj) return {};
    const out = {};
    Object.keys(obj).forEach(k=>{
      const v = obj[k];
      out[k] = (v === null || v === undefined || (typeof v === 'number' && isNaN(v))) ? 0 : v;
    });
    return out;
  }

  function safeMinMax(vals){
    const arr = vals.filter(v=>typeof v === 'number' && isFinite(v));
    if(arr.length === 0) return [0,1];
    const min = Math.min(...arr), max = Math.max(...arr);
    return min === max ? [min, max+1] : [min, max];
  }

  function getGrayscale(v, min, max){
    // clamp brightness 55..210
    const t = (v - min) / (max - min || 1);
    const intensity = 55 + Math.round(t * (210 - 55));
    return `rgb(${intensity},${intensity},${intensity})`;
  }

  /* ----------------- Color Schemas ----------------- */
  const colorSchemas = {
    //  classic perceptual map
    Viridis: t => `hsl(${260 - t * 220}, 70%, 48%)`,
    //  warmer diverging
    Inferno: t => `hsl(${30 + t * 250}, 85%, ${35 + t * 20}%)`,
    // vivid contrast from dark purple → yellow
    Magma: t => `hsl(${280 - t * 260}, 80%, ${30 + t * 25}%)`,
    // cool-to-warm
    Coolwarm: t => `hsl(${200 - t * 200}, 72%, 52%)`,
    // orange to blue diverging
    Sunset: t => `hsl(${30 + t * 180}, 80%, ${45 + t * 10}%)`,
    // green gradient
    Emerald: t => `hsl(${140 - t * 40}, 65%, ${40 + t * 25}%)`,
    // ocean blue shades
    Ocean: t => `hsl(${200 + t * 40}, 70%, ${35 + t * 20}%)`,
    // pastel rainbow (for softer visualizations)
    Pastel: t => `hsl(${360 * t}, 60%, 70%)`,
    // cubehelix-inspired purple→green
    Cubehelix: t => `hsl(${300 - t * 240}, ${50 + t * 30}%, ${40 + t * 20}%)`,
    // grayscale
    Grayscale: t => `hsl(0, 0%, ${30 + t * 50}%)`
  };

  /* ----------------- Chart metrics ----------------- */
  function calculateDegree(nodes, edges){
    const res = {}; nodes.forEach(n=>res[n.id]=0);
    edges.forEach(e=>{
      if(res[e.from] === undefined) res[e.from] = 0;
      if(res[e.to] === undefined) res[e.to] = 0;
      res[e.from]++; res[e.to]++;
    });
    return sanitizeData(res);
  }

  function calculateInOutDegree(nodes, edges){
    const inD = {}, outD = {};
    nodes.forEach(n=>{ inD[n.id] = 0; outD[n.id] = 0; });
    edges.forEach(e=>{
      if(outD[e.from]===undefined) outD[e.from]=0;
      if(inD[e.to]===undefined) inD[e.to]=0;
      outD[e.from]++; inD[e.to]++;
    });
    return { in: sanitizeData(inD), out: sanitizeData(outD) };
  }

  function betweennessCentrality(nodes, edges){
    // placeholder: random scores
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()])));
  }

  function calculateCloseness(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()])));
  }

  function calculateEigenvector(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()])));
  }

  function calculateKatz(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, 0.1 + Math.random()*0.9])));
  }

  function calculatePageRank(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()])));
  }

  function calculateClusteringCoefficient(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()])));
  }

  function calculateEccentricity(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.floor(Math.random()*5)+1])));
  }

  function calculateKCore(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.floor(Math.random()*4)+1])));
  }

  function calculateHITS(nodes, edges){ 
    return {
      auth: sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()]))),
      hub: sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()])))
    };
  }

  function calculateConstraint(nodes, edges){
    return sanitizeData(Object.fromEntries(nodes.map(n=>[n.id, Math.random()])));
  }

  function calculateEgoNetworkMetrics(nodes, edges){
  const results = {};
  nodes.forEach(n=>{
    const egoEdges = edges.filter(e=>e.from===n.id || e.to===n.id);
    const egoNodes = new Set(egoEdges.flatMap(e=>[e.from,e.to]));
    const size = egoNodes.size;
    const density = +(egoEdges.length / (size*(size-1) || 1)).toFixed(3);
    results[n.id] = { size, density };
  });
  return results;
  }

  function calculateEdgeBetweenness(nodes, edges){
    return sanitizeData(Object.fromEntries(edges.map(e=>[`${e.from}-${e.to}`, Math.random()])));
  }

  function calculateEdgeWeight(nodes, edges){
    return sanitizeData(Object.fromEntries(edges.map(e=>[`${e.from}-${e.to}`, +(Math.random()*10).toFixed(2)])));
  }

  function calculateEdgeEmbeddedness(nodes, edges){
    return sanitizeData(Object.fromEntries(edges.map(e=>[`${e.from}-${e.to}`, Math.floor(Math.random()*3)])));
  }

  function calculateEdgeSimilarity(nodes, edges, method="jaccard"){
    // placeholder similarities
    return sanitizeData(Object.fromEntries(edges.map(e=>[`${e.from}-${e.to}`, Math.random()])));
  }

  function calculateGlobalMetrics(nodes, edges){
    const N = nodes.length, E = edges.length;
    const density = N > 1 ? E / (N*(N-1)) : 0;
    return sanitizeData({
      "Number of Nodes": N,
      "Number of Edges": E,
      "Density": +density.toFixed(4),
      "Average Degree": +(E*2/N || 0).toFixed(3),
      "Average Path Length": +(1 + Math.random()*3).toFixed(2),
      "Diameter": +(2 + Math.random()*5).toFixed(2),
      "Average Clustering Coefficient": +Math.random().toFixed(3),
      "Transitivity": +Math.random().toFixed(3),
      "Assortativity": +((Math.random()-0.5)*2).toFixed(3),
      "Reciprocity (directed)": +Math.random().toFixed(3)
    });
  }

  function calculateCohesionMetrics(nodes, edges){
    return sanitizeData({
      "Connected Components": Math.max(1, Math.floor(Math.random()*4)),
      "Largest Component Ratio": +Math.random().toFixed(3),
      "Bridge Edges / Cut Vertices": Math.floor(Math.random()*6),
      "Modularity": +Math.random().toFixed(3),
      "Community Count": Math.floor(Math.random()*6)+1,
      "Community Density": +Math.random().toFixed(3),
      "Network Centralization": +Math.random().toFixed(3)
    });
  }

  function calculateTemporalMetrics(nodes, edges){
    return sanitizeData({
      "Node/Edge Churn": +(Math.random()*10).toFixed(2),
      "Centrality Over Time": +Math.random().toFixed(3),
      "Density Over Time": +Math.random().toFixed(3),
      "Modularity Over Time": +Math.random().toFixed(3)
    });
  }

  async function captureChartSnapshot(containerSelector = ".chart_containers", filenamePrefix = "ChartCardSnapshot") {
    try {
      const containers = Array.from(document.querySelectorAll(containerSelector))
        .filter(el => el.offsetParent !== null); // visible only

      if (containers.length === 0) {
        alert("No visible chart(s) found to capture.");
        return;
      }

      for (let i = 0; i < containers.length; i++) {
        const el = containers[i];

        // 🔹 Temporarily hide the controls before capture
        const controls = el.querySelector(".chart_controls");
        const oldDisplay = controls ? controls.style.display : null;
        if (controls) controls.style.display = "none";

        // Capture using html2canvas
        const canvas = await html2canvas(el, {
          scale: 2,
          backgroundColor: "#fff",
          useCORS: true
        });

        // Restore the controls after capture
        if (controls) controls.style.display = oldDisplay;

        // Trigger download
        const dataUrl = canvas.toDataURL("image/png");
        const a = document.createElement("a");
        a.href = dataUrl;
        a.download = `${filenamePrefix}_${i + 1}_${Date.now()}.png`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
      }

      console.log("Chart(s) snapshots saved.");
    } catch (err) {
      console.error("Error capturing chart(s) snapshot:", err);
      alert("Could not capture chart(s).");
    }
  }

  async function printChart(containerSelector = ".chart_containers") {
    try {
      const containers = Array.from(document.querySelectorAll(containerSelector))
        .filter(el => el.offsetParent !== null);

      if (containers.length === 0) {
        alert("No visible chart(s) found to print.");
        return;
      }

      const imageDataList = [];
      for (const el of containers) {
        const controls = el.querySelector(".chart_controls");
        const oldDisplay = controls ? controls.style.display : null;
        if (controls) controls.style.display = "none";

        const canvas = await html2canvas(el, {
          scale: 2,
          backgroundColor: "#fff",
          useCORS: true
        });

        if (controls) controls.style.display = oldDisplay;

        imageDataList.push(canvas.toDataURL("image/png"));
      }

      const printWindow = window.open("", "_blank");
      printWindow.document.write(`
        <html>
          <head>
            <title>Print Charts</title>
            <style>
              body { margin: 0; text-align: center; background: #fff; }
              img { max-width: 90%; margin: 20px auto; display: block; border: 1px solid #ccc; box-shadow: 0 0 6px rgba(0,0,0,0.2); }
            </style>
          </head>
          <body>
            ${imageDataList.map(src => `<img src="${src}" />`).join("")}
            <script>window.onload = () => window.print();</script>
          </body>
        </html>
      `);
      printWindow.document.close();
    } catch (err) {
      console.error("Error printing chart(s):", err);
      alert("Could not print chart(s).");
    }
  }

  function resetCharts(){    
    // Clear current charts
    const main = document.getElementById("main_container");
    main.innerHTML="";
  }


  /* ----------------- Chart factory ----------------- */
  function createMetricChart(metricName, nodes, edges, selectedNodes, selectedEdges) {
    const main = document.getElementById("main_container");
    let data = {}, chartType = "bar";

    // --- Metric selection logic ---
    switch (metricName) {
      case "Degree Centrality": data = calculateDegree(nodes, edges); break;
      case "Betweenness Centrality": data = betweennessCentrality(nodes, edges); break;
      case "Closeness Centrality": data = calculateCloseness(nodes, edges); break;
      case "Eigenvector Centrality": data = calculateEigenvector(nodes, edges); chartType = "line"; break;
      case "Clustering Coefficient": data = calculateClusteringCoefficient(nodes, edges); chartType = "scatter"; break;
      case "PageRank": data = calculatePageRank(nodes, edges); break;
      case "K-Core Number": data = calculateKCore(nodes, edges); break;
      case "HITS (Authority / Hub Scores)": data = calculateHITS(nodes, edges); chartType = "radar"; break;
      case "Constraint (Structural Holes)": data = calculateConstraint(nodes, edges); chartType = "scatter"; break;
      case "Ego Network Size / Density":
        const e = calculateEgoNetworkMetrics(nodes, edges);
        const s = {}, d = {};
        Object.keys(e).forEach(k => {
          s[k] = e[k].size;
          d[k] = e[k].density;
        });
        data = { Size: s, Density: d };
        chartType = "dualbar";
        break;
      case "Edge Betweenness": data = calculateEdgeBetweenness(nodes, edges); break;
      case "Edge Weight": data = calculateEdgeWeight(nodes, edges); break;
      case "Edge Embeddedness": data = calculateEdgeEmbeddedness(nodes, edges); break;
      case "Edge Similarity (Jaccard, Cosine)": data = calculateEdgeSimilarity(nodes, edges, "jaccard"); break;
      case "Global Metrics": data = calculateGlobalMetrics(nodes, edges); break;
      case "Cohesion Metrics": data = calculateCohesionMetrics(nodes, edges); break;
      case "Temporal Metrics": data = calculateTemporalMetrics(nodes, edges); chartType = "line"; break;
    }

    // --- DOM setup ---
    const card = document.createElement("div");
    card.className = "chart_containers";
    card.innerHTML = `
      <div class="chart_header">
        <h3>${metricName}</h3>
        <div class="chart_controls">
          <label class="switch color_toggle_wrap">
            <input type="checkbox" class="color_toggle" />
            <span class="slider"></span>
          </label>
          <select class="schema_select">
            <option value="Viridis">Viridis</option>
            <option value="Inferno">Inferno</option>
            <option value="Magma">Magma</option>
            <option value="Coolwarm">Coolwarm</option>
            <option value="Sunset">Sunset</option>
            <option value="Emerald">Emerald</option>
            <option value="Ocean">Ocean</option>
            <option value="Pastel">Pastel</option>
            <option value="Cubehelix">Cubehelix</option>
            <option value="Grayscale">Grayscale</option>
          </select>
          <button class="maximize_btn" title="Fullscreen">⤢</button>
          <button class="return_btn" title="Return">⤫</button>
          <button class="close_btn" title="Close">⤫</button>
        </div>
      </div>
      <div class="chart_content"><div class="chart_box"></div></div>`;
    main.appendChild(card);

    const box = card.querySelector(".chart_box");
    const chart = echarts.init(box, null, { renderer: "canvas" });
    const localToggle = card.querySelector(".color_toggle");
    const localSchema = card.querySelector(".schema_select");
    const globalToggle = document.getElementById("global_toggle");
    const globalSchema = document.getElementById("global_schema");

    // --- Helper: color picking ---
    function pickColor(vals, key, colorful, schema, selectedNodes = []) {
      // safety guard
      if (!vals || typeof vals !== "object" || vals === null) {
        console.warn("pickColor() got invalid vals:", vals);
        return "#ccc";
      }

      // ignore keys not present
      if (!(key in vals)) return "#ccc";
      if (typeof vals[key] !== "number") return "#ccc";

      const all = Object.values(vals).filter(v => typeof v === "number" && isFinite(v));
      if (all.length === 0) return "#ccc";

      const min = Math.min(...all);
      const max = Math.max(...all);
      const v = vals[key];

      // normalize selectedNodes
      let nodeMap = {};
      if (Array.isArray(selectedNodes)) {
        nodeMap = Object.fromEntries(selectedNodes.map(n => [n.id, n]));
      } else if (typeof selectedNodes === "object" && selectedNodes !== null) {
        nodeMap = selectedNodes;
      }

      const selectedCount = Object.keys(nodeMap).length;
      const nodeData = nodeMap[key];

      // selected node color
      if (nodeData && nodeData.color) {
        const c = nodeData.color;
        if (typeof c === "string") return c;
        if (typeof c === "object") {
          return c.highlight?.background || c.background || c.border || "#ff4444";
        }
      }

      // normal color
      if (colorful) {
        const t = (v - min) / (max - min || 1);
        return colorSchemas[schema](Math.max(0, Math.min(1, t)));
      } else {
        return getGrayscale(v, min, max);
      }
    }


    let lastState = { colorful: null, schema: null };
    const FADE_DURATION = 350;

    // --- Helper: Chart update ---
    function updateChart(isFromGlobal = false) {
      const isColorful = isFromGlobal ? globalToggle.checked : localToggle.checked;
      const schema = isFromGlobal ? globalSchema.value : localSchema.value;

      if (lastState.colorful === isColorful && lastState.schema === schema) return;
      lastState = { colorful: isColorful, schema };

      box.style.transition = `opacity ${FADE_DURATION}ms ease`;
      box.style.opacity = 0.2;

      const opts = {
        tooltip: { show: true },
        animationDurationUpdate: 400,
        animationEasingUpdate: "cubicOut",
        dataZoom: [
          { type: "slider", show: true, bottom: 20 },
          { type: "inside" },
        ],
      };

      if (chartType === "dualbar") {
        const labels = Object.keys(data.Size);
        chart.setOption({
          ...opts,
          legend: { top: 8 },
          grid: { top: 50, bottom: 60, left: 50, right: 30 },
          xAxis: { type: "category", data: labels, axisLabel: { show: false } },
          yAxis: {},
          series: [
            {
              name: "Size",
              type: "bar",
              data: labels.map(k => ({
                value: data.Size[k],
                itemStyle: {
                  color: pickColor(data.Size, k, isColorful, schema, selectedNodes), // fixed
                },
              })),
            },
            {
              name: "Density",
              type: "bar",
              data: labels.map(k => ({
                value: data.Density[k],
                itemStyle: {
                  color: pickColor(data.Density, k, isColorful, schema, selectedNodes), // fixed
                },
              })),
            },
          ],
        }, true);
      } 
      else if (chartType === "radar") {
        const labels = Object.keys(data.auth);
        const maxV = Math.max(...Object.values(data.auth), ...Object.values(data.hub));
        chart.setOption({
          ...opts,
          radar: {
            indicator: labels.map(l => ({ name: l, max: maxV || 1 })),
          },
          series: [{
            type: "radar",
            data: [
              { value: Object.values(data.auth), name: "Authority" },
              { value: Object.values(data.hub), name: "Hub" },
            ],
          }],
        }, true);
      } 
      else {
        const labels = Object.keys(data);
        const vals = Object.values(data);

        const series = [{
          type: chartType === "scatter" ? "scatter" : chartType,
          smooth: chartType === "line",
          lineStyle: chartType === "line" ? { color: "#333", width: 2 } : undefined,
          symbolSize: chartType === "line" ? 8 : undefined,
          data: labels.map((k, i) => {
            const val = chartType === "scatter" ? [i, vals[i]] : vals[i];
            return {
              value: val,
              itemStyle: {
                color: pickColor(data, k, isColorful, schema, selectedNodes), // fixed
              },
              emphasis: { itemStyle: { opacity: 1 } },
            };
          }),
        }];

        chart.setOption({
          ...opts,
          grid: { top: 40, bottom: 80, left: 30, right: 50, containLabel: true },
          xAxis: chartType === "scatter"
            ? { type: "value", show: false }
            : { type: "category", data: labels, axisLabel: { show: false } },
          yAxis: {},
          series,
        }, true);
      }

      chart.resize();
      setTimeout(() => { box.style.opacity = 1; }, FADE_DURATION - 100);
    }

    chart.updateChart = (isFromGlobal = false) => updateChart(isFromGlobal);

    // --- Local controls ---
    localToggle.addEventListener("change", e => { e.stopPropagation(); updateChart(false); });
    localSchema.addEventListener("change", e => { e.stopPropagation(); updateChart(false); });

    // --- Global sync (batched + debounced) ---
    let globalUpdateTimer;
    function scheduleGlobalUpdate() {
      clearTimeout(globalUpdateTimer);
      globalUpdateTimer = setTimeout(() => {
        const globalState = globalToggle.checked;
        const globalSchemaValue = globalSchema.value;
        const allCards = document.querySelectorAll(".chart_containers");
        requestAnimationFrame(() => {
          allCards.forEach(card => {
            const toggle = card.querySelector(".color_toggle");
            const schema = card.querySelector(".schema_select");
            if (toggle) toggle.checked = globalState;
            if (schema) schema.value = globalSchemaValue;
            const chartBox = card.querySelector(".chart_box");
            const chartInstance = echarts.getInstanceByDom(chartBox);
            if (chartInstance && chartInstance.updateChart)
              chartInstance.updateChart(true);
          });
        });
      }, 80);
    }
    globalToggle.addEventListener("change", scheduleGlobalUpdate);
    globalSchema.addEventListener("change", scheduleGlobalUpdate);

    // --- Fullscreen + close ---
    const maximizeBtn = card.querySelector(".maximize_btn");
    const returnBtn = card.querySelector(".return_btn");
    const closeBtn = card.querySelector(".close_btn");
    let previousScroll = 0;

    maximizeBtn.addEventListener("click", e => {
      e.stopPropagation();
      previousScroll = window.scrollY;
      card.classList.add("full_screen");
      document.body.style.overflow = "hidden";
      chart.resize({ animation: { duration: 300, easing: "cubicOut" } });
    });

    returnBtn.addEventListener("click", e => {
      e.stopPropagation();
      card.classList.remove("full_screen");
      document.body.style.overflow = "auto";
      window.scrollTo(0, previousScroll);
      chart.resize({ animation: { duration: 300, easing: "cubicOut" } });
    });

    closeBtn.addEventListener("click", e => {
      e.stopPropagation();
      card.remove();
      // Remove metricName from chartsList
      const index = chartsList.indexOf(metricName);
      if (index !== -1) {
        chartsList.splice(index, 1);
      }
    });

    // --- Initial render ---
    localToggle.checked = false;
    localSchema.value = "Viridis";
    updateChart();
    window.addEventListener("resize", () =>
      chart.resize({ animation: { duration: 250 } })
    );
  }

  function storeNetworkComponents(nodesdata,edgesdata){
    console.log(12)
    nodes = nodesdata;
    edges = edgesdata;  
  }

  function getNetworkComponents(id){
    const {nodes, edges} = payload;
    nodes = nodes;
    edges = edges;
    return {nodes,edges};
  }

  function init(id,metricName,newNodes,newEdges,selectedNodes,selectedEdges){
    if (id === "init_components"){
      const network=getNetworkComponents("Components");
    }
    if (id === "update_components"){
      null
    }
    if (id === "create_chart"){      
      // Only push if not already in the list
      if (!chartsList.includes(metricName)) {
        createMetricChart(metricName, nodes, edges, selectedNodes, selectedEdges);
        chartsList.push(metricName);
      }
      else{
        alert("Message: Chart already created!")
      }
    }
    if (id === "updateSelection"){
      console.log(28,chartsList.length)
      const main = document.getElementById("main_container");
      main.innerHTML="";
      chartsList.forEach(m => createMetricChart(m, nodes, edges, selectedNodes,selectedEdges));
    }




  /* ----------------- Metric List (full) ----------------- */
  // const metricList = [
  //   "Degree Centrality",
  //   "In / Out Degree",
  //   "Betweenness Centrality",
  //   "Closeness Centrality",
  //   "Eigenvector Centrality",
  //   "Katz Centrality",
  //   "PageRank",
  //   "Clustering Coefficient",
  //   "Local Eccentricity",
  //   "K-Core Number",
  //   "HITS (Authority / Hub Scores)",
  //   "Constraint (Structural Holes)",
  //   "Ego Network Size / Density",
  //   "Edge Betweenness",
  //   "Edge Weight",
  //   "Edge Embeddedness",
  //   "Edge Similarity (Jaccard, Cosine)",
  //   "Assortativity",
  //   "Reciprocity",
  //   "Global Metrics",
  //   "Cohesion Metrics",
  //   "Temporal Metrics"
  // ];
  // metricList.forEach(m => createMetricChart(m, nodes, edges, selectedColors));
  }