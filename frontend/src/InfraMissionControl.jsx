import {
  useState, useEffect, useRef, useCallback,
  useSyncExternalStore, memo, useMemo
} from "react";
import * as d3 from "d3";

/* ═══════════════════════════════════════════════════════════════════════════
   ATLAS SYSTEMS — SELF-HEALING AIOPS MISSION CONTROL
   Design System: Premium Obsidian Dark · Inter Typography · Stable Tier Layout
   ═══════════════════════════════════════════════════════════════════════════ */

/* ── COLOR PALETTE ───────────────────────────────────────────────────────── */
const P = {
  bg:       "#020617",      // Slate 950
  bgPanel:  "#0f172a",      // Slate 900
  bgCell:   "#070a13",      // Custom deep black/blue for metrics
  border:   "#1e293b",      // Slate 800
  borderLight: "#334155",   // Slate 700
  dim:      "#0f172a",

  teal:     "#00c8aa",      // Atlas Emerald
  tealDim:  "#0f766e",      // Teal 700
  cyan:     "#0ea5e9",      // Sky 500
  green:    "#10b981",      // Emerald 500
  amber:    "#f59e0b",      // Amber 500
  orange:   "#f97316",      // Orange 500
  red:      "#ef4444",      // Red 500
  redDim:   "#450a0a",      // Dark Red
  upstream: "#eab308",      // Yellow 500
  critPath: "#ef4444",      
  slowPath: "#f97316",      

  txt:      "#94a3b8",      // Slate 400
  txtDim:   "#475569",      // Slate 600
  txtBright:"#f8fafc",      // Slate 50
  label:    "#64748b",      // Slate 500
};

/* ── SERVICE TOPOLOGY & STABLE COORDINATES ───────────────────────────────── */
const TOPOLOGY = {
  nodes: [
    { id: "cdn-edge",         name: "CDN EDGE",         group: "edge"     },
    { id: "api-gateway",      name: "API GATEWAY",      group: "edge"     },
    { id: "auth-vault",       name: "AUTH VAULT",       group: "security" },
    { id: "k8s-orchestrator", name: "K8S ORCHESTRATOR", group: "compute"  },
    { id: "db-cluster",       name: "DB CLUSTER",       group: "data"     },
    { id: "cache-layer",      name: "CACHE LAYER",      group: "data"     },
    { id: "message-bus",      name: "MESSAGE BUS",      group: "infra"    },
    { id: "log-aggregator",   name: "LOG AGGREGATOR",   group: "observe"  },
    { id: "object-store",     name: "OBJECT STORE",     group: "data"     },
    { id: "metrics-pipeline", name: "METRICS PIPELINE",  group: "observe"  },
  ],
  edges: [
    { source:"cdn-edge",         target:"api-gateway"       },
    { source:"api-gateway",      target:"auth-vault"        },
    { source:"api-gateway",      target:"k8s-orchestrator"  },
    { source:"k8s-orchestrator", target:"db-cluster"        },
    { source:"k8s-orchestrator", target:"cache-layer"       },
    { source:"k8s-orchestrator", target:"message-bus"       },
    { source:"auth-vault",       target:"db-cluster"        },
    { source:"message-bus",      target:"log-aggregator"    },
    { source:"message-bus",      target:"object-store"      },
    { source:"log-aggregator",   target:"metrics-pipeline"  },
    { source:"db-cluster",       target:"object-store"      },
  ],
};

// Fixed positions arranged in architectural logical columns (Edge -> Compute -> Middleware -> Data)
const STABLE_LAYOUT = {
  "cdn-edge":         { x: 90,  y: 250 },
  "api-gateway":      { x: 230, y: 250 },
  "auth-vault":       { x: 390, y: 150 },
  "k8s-orchestrator": { x: 390, y: 350 },
  "db-cluster":       { x: 550, y: 120 },
  "cache-layer":      { x: 550, y: 250 },
  "message-bus":      { x: 550, y: 380 },
  "log-aggregator":   { x: 710, y: 250 },
  "object-store":     { x: 710, y: 380 },
  "metrics-pipeline": { x: 860, y: 315 }
};

/* ═══════════════════════════════════════════════════════════════════════════
   METRICS STORE  (surgical per-node subscriptions with rolling history)
   ═══════════════════════════════════════════════════════════════════════════ */
function createMetricsStore() {
  let state = {};
  const gSubs = new Set(), nSubs = {};
  return {
    update(p) {
      const prevNode = state[p.nodeId] ?? { history: [] };
      const newHistory = [...(prevNode.history ?? [])];
      
      newHistory.push({
        cpu: p.cpu,
        mem: p.mem,
        latencyP99: p.latencyP99,
        rps: p.rps,
        errorRate: p.errorRate,
        ts: Date.now()
      });
      
      // Bounded sliding window for 30 historical points
      if (newHistory.length > 30) {
        newHistory.shift();
      }
      
      state = {
        ...state,
        [p.nodeId]: {
          ...p,
          history: newHistory,
          ts: Date.now()
        }
      };
      
      gSubs.forEach(fn => fn());
      nSubs[p.nodeId]?.forEach(fn => fn());
    },
    subscribe:    fn => { gSubs.add(fn); return () => gSubs.delete(fn); },
    getSnapshot:  () => state,
    subscribeNode(id, fn) { (nSubs[id] ??= new Set()).add(fn); return () => nSubs[id]?.delete(fn); },
    snapshotNode: id => state[id] ?? null,
  };
}
const metricsStore = createMetricsStore();

function useNodeMetrics(nodeId) {
  return useSyncExternalStore(
    useCallback(fn => metricsStore.subscribeNode(nodeId, fn), [nodeId]),
    useCallback(() => metricsStore.snapshotNode(nodeId),      [nodeId]),
    () => null,
  );
}

/* ═══════════════════════════════════════════════════════════════════════════
   INCIDENT & METRICS SIMULATOR with Chaos Engineering Mode
   ═══════════════════════════════════════════════════════════════════════════ */
function useSSEStream(chaosMode) {
  const [incidents, setIncidents] = useState([]);
  
  // Set initial realistic baselines
  useEffect(() => {
    TOPOLOGY.nodes.forEach(n => {
      metricsStore.update({
        nodeId: n.id,
        cpu: 18 + Math.random() * 12,
        mem: 35 + Math.random() * 8,
        latencyP99: 6 + Math.random() * 8,
        rps: 150 + Math.floor(Math.random() * 120),
        health: 0.98 + Math.random() * 0.02,
        traffic: 0.4 + Math.random() * 0.1,
        errorRate: 0.0,
        incident: false,
        zScore: null,
      });
    });
  }, []);

  // Live simulation update loop
  useEffect(() => {
    const interval = setInterval(() => {
      TOPOLOGY.nodes.forEach(n => {
        const isSelectedChaos = isNodeInChaos(n.id, chaosMode);
        
        let cpu = 18 + Math.random() * 12;
        let mem = 35 + Math.random() * 8;
        let latencyP99 = 6 + Math.random() * 8;
        let rps = 150 + Math.floor(Math.random() * 120);
        let errorRate = 0.0;
        let health = 0.98 + Math.random() * 0.02;
        let incident = false;
        let zScore = null;

        // Apply specific Chaos modes with logical cascades
        if (isSelectedChaos) {
          incident = true;
          zScore = +(3.1 + Math.random() * 2.4).toFixed(2);
          health = 0.12 + Math.random() * 0.15;
          
          if (chaosMode === "db_spike" && n.id === "db-cluster") {
            latencyP99 = 410 + Math.random() * 70;
            errorRate = 0.68 + Math.random() * 0.12;
            cpu = 82 + Math.random() * 10;
          } else if (chaosMode === "db_spike" && n.id === "object-store") {
            // Cascaded degradation
            latencyP99 = 145 + Math.random() * 30;
            errorRate = 0.22 + Math.random() * 0.08;
          } else if (chaosMode === "auth_degrade" && n.id === "auth-vault") {
            latencyP99 = 370 + Math.random() * 50;
            errorRate = 0.48 + Math.random() * 0.15;
          } else if (chaosMode === "auth_degrade" && n.id === "api-gateway") {
            // Gateway queue backlog
            latencyP99 = 210 + Math.random() * 35;
            cpu = 72 + Math.random() * 12;
          } else if (chaosMode === "k8s_leak" && n.id === "k8s-orchestrator") {
            mem = 95.8 + Math.random() * 2.2;
            cpu = 88 + Math.random() * 8;
            latencyP99 = 85 + Math.random() * 40;
          } else if (chaosMode === "bus_partition" && n.id === "message-bus") {
            errorRate = 0.88 + Math.random() * 0.08;
            rps = 12 + Math.floor(Math.random() * 15);
          } else if (chaosMode === "bus_partition" && (n.id === "log-aggregator" || n.id === "object-store")) {
            rps = 8 + Math.floor(Math.random() * 12);
            errorRate = 0.32 + Math.random() * 0.12;
          }
        }

        // Extremely rare random baseline anomalies (looks highly realistic)
        const isRandomAnomaly = !chaosMode && Math.random() < 0.003;
        if (isRandomAnomaly) {
          incident = true;
          zScore = +(3.0 + Math.random() * 1.5).toFixed(2);
          latencyP99 = 160 + Math.random() * 80;
          errorRate = 0.12 + Math.random() * 0.08;
          health = 0.52;
        }

        metricsStore.update({
          nodeId: n.id, cpu, mem, latencyP99, rps, health, errorRate, incident, zScore
        });

        // Trigger incident logs
        if (incident && Math.random() < 0.12) {
          const serviceName = TOPOLOGY.nodes.find(node => node.id === n.id)?.name ?? n.id;
          setIncidents(prev => {
            const hasRecent = prev.some(inc => inc.nodeId === n.id && Date.now() - inc.id < 6000);
            if (hasRecent) return prev;
            return [
              {
                id: Date.now(),
                nodeId: n.id,
                name: serviceName,
                cpu,
                latencyP99,
                zScore,
                time: new Date().toLocaleTimeString("en-GB")
              },
              ...prev
            ].slice(0, 16);
          });
        }
      });
    }, 1200);

    return () => clearInterval(interval);
  }, [chaosMode]);

  return { incidents, setIncidents };
}

function isNodeInChaos(nodeId, chaosMode) {
  if (!chaosMode) return false;
  if (chaosMode === "db_spike" && (nodeId === "db-cluster" || nodeId === "object-store")) return true;
  if (chaosMode === "auth_degrade" && (nodeId === "auth-vault" || nodeId === "api-gateway")) return true;
  if (chaosMode === "k8s_leak" && nodeId === "k8s-orchestrator") return true;
  if (chaosMode === "bus_partition" && (nodeId === "message-bus" || nodeId === "log-aggregator" || nodeId === "object-store")) return true;
  return false;
}

/* ═══════════════════════════════════════════════════════════════════════════
   CLIENT-SIDE GRAPH COMPUTATION FALLBACK (Neo4j Dijkstra replication)
   ═══════════════════════════════════════════════════════════════════════════ */
function computeLocalBlastRadius(originId, snap) {
  // 1. Build adjacency list of dependencies
  const adj = {};
  TOPOLOGY.nodes.forEach(n => adj[n.id] = []);
  TOPOLOGY.edges.forEach(e => {
    adj[e.source].push(e.target);
  });

  // Downstream reachability traversal (BFS)
  const getDownstream = (start) => {
    const visited = new Set();
    const queue = [start];
    while (queue.length > 0) {
      const curr = queue.shift();
      const neighbors = adj[curr] || [];
      neighbors.forEach(nxt => {
        if (!visited.has(nxt) && nxt !== start) {
          visited.add(nxt);
          queue.push(nxt);
        }
      });
    }
    return Array.from(visited);
  };

  const downstreamCandidates = getDownstream(originId);

  // 2. Dijkstra Algorithm to find the shortest (fastest-to-degrade) path to any candidate
  const getDijkstraPath = (start, target) => {
    const dist = {};
    const prev = {};
    const nodesList = TOPOLOGY.nodes.map(n => n.id);
    
    nodesList.forEach(n => {
      dist[n] = Infinity;
      prev[n] = null;
    });
    dist[start] = 0;
    
    const unvisited = new Set(nodesList);
    while (unvisited.size > 0) {
      let u = null;
      let minDist = Infinity;
      unvisited.forEach(n => {
        if (dist[n] < minDist) {
          minDist = dist[n];
          u = n;
        }
      });
      
      if (!u || u === target) break;
      unvisited.delete(u);
      
      const neighbors = adj[u] || [];
      neighbors.forEach(v => {
        if (unvisited.has(v)) {
          const latency = snap[v]?.latencyP99 ?? 12;
          const alt = dist[u] + latency;
          if (alt < dist[v]) {
            dist[v] = alt;
            prev[v] = u;
          }
        }
      });
    }
    
    const path = [];
    let curr = target;
    if (prev[curr] || curr === start) {
      while (curr) {
        path.unshift(curr);
        curr = prev[curr];
      }
    }
    return { path, dist: dist[target] };
  };

  // Find the candidate path with minimum latency weight
  let bestPath = [];
  let minCost = Infinity;

  downstreamCandidates.forEach(targetId => {
    const { path, dist } = getDijkstraPath(originId, targetId);
    if (path.length > 0 && dist < minCost) {
      minCost = dist;
      bestPath = path;
    }
  });

  if (bestPath.length === 0) {
    bestPath = [originId];
    minCost = 0;
  }

  // 3. Secondary blast nodes (downstream excluding critical path)
  const critSet = new Set(bestPath);
  const secondaryBlastNodes = downstreamCandidates.filter(id => !critSet.has(id));

  // 4. Upstream callers (reverse traversal)
  const revAdj = {};
  TOPOLOGY.nodes.forEach(n => revAdj[n.id] = []);
  TOPOLOGY.edges.forEach(e => {
    revAdj[e.target].push(e.source);
  });

  const getUpstream = (start) => {
    const visited = new Set();
    const queue = [start];
    while (queue.length > 0) {
      const curr = queue.shift();
      const prevNodes = revAdj[curr] || [];
      prevNodes.forEach(p => {
        if (!visited.has(p) && p !== start) {
          visited.add(p);
          queue.push(p);
        }
      });
    }
    return Array.from(visited);
  };
  const upstreamCallers = getUpstream(originId);

  // 5. Impact Score and MTTR calculation (aligns with SlaSimulationService)
  const totalAffected = bestPath.length - 1 + secondaryBlastNodes.length;
  const latencyFactor = Math.min(minCost / 500.0, 1.0);
  const blastFactor = Math.min(totalAffected / 10.0, 1.0);
  const impactScore = Math.min(Math.round((latencyFactor * 0.6 + blastFactor * 0.4) * 100), 99);

  const originNode = TOPOLOGY.nodes.find(n => n.id === originId);
  const nodeGroup = originNode?.group ?? "unknown";
  const baseMttrMap = { data: 45, security: 35, compute: 25, infra: 20, edge: 12, observe: 8 };
  const baseMttr = baseMttrMap[nodeGroup] ?? 30;
  const critBonus = bestPath.length > 1 ? 10 : 0;
  const cascadeBonus = Math.min(bestPath.length - 1, 5) * 5;
  const estimatedMttrMinutes = baseMttr + critBonus + cascadeBonus;

  return {
    originId,
    impactScore,
    estimatedMttrMinutes,
    criticalPath: {
      nodes: bestPath,
      totalLatencyMs: minCost
    },
    secondaryBlastNodes,
    upstreamCallers,
    isLocalFallback: true
  };
}

/* ═══════════════════════════════════════════════════════════════════════════
   BLAST RADIUS FETCH HOOK with automatic client-side backup routing
   ═══════════════════════════════════════════════════════════════════════════ */
const API_BASE = "/api/v1/infra";
function useFetchBlastRadius(nodeId) {
  const [status,   setStatus]   = useState("idle");
  const [blastData,setBlastData]= useState(null);
  const [errorMsg, setErrorMsg] = useState(null);
  
  useEffect(() => {
    if (!nodeId) { setStatus("idle"); setBlastData(null); setErrorMsg(null); return; }
    
    setStatus("loading"); setBlastData(null); setErrorMsg(null);
    const ctrl = new AbortController();
    
    fetch(`${API_BASE}/blast-radius/${encodeURIComponent(nodeId)}`, {
      signal: ctrl.signal, headers:{ Accept:"application/json" },
    })
      .then(r => { if(!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d  => { setBlastData(d); setStatus("success"); })
      .catch(e => {
        if(e.name === "AbortError") return;
        
        // Backend/Neo4j offline - trigger the local graph pathfinding algorithm!
        console.warn(`[Atlas Systems] API unreachable. Invoking local Dijkstra pathfinding engine for: ${nodeId}`);
        const snap = metricsStore.getSnapshot();
        const localResult = computeLocalBlastRadius(nodeId, snap);
        
        // Minor delay for polished loading state transition
        setTimeout(() => {
          setBlastData(localResult);
          setStatus("success");
        }, 300);
      });
      
    return () => ctrl.abort();
  }, [nodeId]);
  
  return { status, blastData, errorMsg };
}

/* ── Health calculation helpers ── */
function healthState(m) {
  if (!m || m.incident || m.health < 0.40) return "critical";
  if (m.health < 0.75)                    return "warning";
  return "healthy";
}
const STATE_COLOR = { critical: P.red, warning: P.amber, healthy: P.green };
const nodeColor = m => STATE_COLOR[healthState(m)];

/* ═══════════════════════════════════════════════════════════════════════════
   TOPOLOGY GRAPH COMPONENT (Stable coordinates, modern node-cards)
   ═══════════════════════════════════════════════════════════════════════════ */
const PTCL = 4; // particles per connection

const TopologyGraph = memo(function TopologyGraph({ selectedNode, blastStatus, blastData, onNodeClick }) {
  const svgRef      = useRef(null);
  const nodeSelRef  = useRef(null);
  const edgeSelRef  = useRef(null);
  const edgeDataRef = useRef([]);
  const pGRef       = useRef(null);
  const rafRef      = useRef(null);

  /* One-time D3 Graph mounting */
  useEffect(() => {
    const el = svgRef.current;
    const svg = d3.select(el);
    svg.selectAll("*").remove();

    // Set logical layout coordinate system
    svg.attr("viewBox", "0 0 960 500")
       .attr("preserveAspectRatio", "xMidYMid meet");

    const defs = svg.append("defs");

    // Sleek premium glow filters
    [["glow-red", P.red, 6], ["glow-orange", P.orange, 5], ["glow-teal", P.teal, 4], ["glow-upstream", P.upstream, 4]].forEach(([id, col, sd]) => {
      const f = defs.append("filter").attr("id", id).attr("x", "-50%").attr("y", "-50%").attr("width", "200%").attr("height", "200%");
      f.append("feGaussianBlur").attr("stdDeviation", sd).attr("result", "blur");
      f.append("feFlood").attr("flood-color", col).attr("flood-opacity", "0.22").attr("result", "color");
      f.append("feComposite").attr("in", "color").attr("in2", "blur").attr("operator", "in").attr("result", "glow-blur");
      const merge = f.append("feMerge");
      merge.append("feMergeNode").attr("in", "glow-blur");
      merge.append("feMergeNode").attr("in", "SourceGraphic");
    });

    // Arrow markers
    [[`arr-dim`, P.border], [`arr-crit`, P.critPath], [`arr-slow`, P.slowPath]].forEach(([id, fill]) => {
      defs.append("marker").attr("id", id)
        .attr("viewBox", "0 -4 8 8").attr("refX", 72).attr("refY", 0)
        .attr("markerWidth", 5).attr("markerHeight", 5).attr("orient", "auto")
        .append("path").attr("d", "M0,-3L6,0L0,3").attr("fill", fill);
    });

    const root = svg.append("g");

    // Grid matrix background
    root.append("rect")
      .attr("width", 960)
      .attr("height", 500)
      .attr("fill", "transparent");

    // Setup node data with stable structures
    const links = TOPOLOGY.edges.map((e, idx) => ({ ...e, _id: idx }));
    const nodes = TOPOLOGY.nodes.map(n => {
      const stablePos = STABLE_LAYOUT[n.id] || { x: 100, y: 100 };
      return { ...n, x: stablePos.x, y: stablePos.y, fx: stablePos.x, fy: stablePos.y };
    });
    edgeDataRef.current = links;

    // Standard static layout force binding
    const sim = d3.forceSimulation(nodes)
      .force("link", d3.forceLink(links).id(d => d.id).distance(140))
      .force("center", d3.forceCenter(480, 250));

    // Connect D3 Zoom
    svg.call(d3.zoom().scaleExtent([0.45, 3]).on("zoom", e => root.attr("transform", e.transform)));
    svg.on("click", () => onNodeClick(null));

    /* ── Render connections ── */
    const edgeSel = root.append("g").selectAll("line").data(links).join("line")
      .attr("stroke", P.border)
      .attr("stroke-width", 1.2)
      .attr("stroke-dasharray", "4 4")
      .attr("opacity", 0.75)
      .attr("marker-end", "url(#arr-dim)");
    edgeSelRef.current = edgeSel;

    /* ── Glowing data particles ── */
    const pG = root.append("g");
    pGRef.current = pG;
    links.forEach(lk => {
      for (let i = 0; i < PTCL; i++) {
        pG.append("circle")
          .attr("class", `p${lk._id}-${i}`)
          .attr("r", 1.5)
          .attr("fill", P.teal)
          .attr("opacity", 0);
      }
    });

    /* ── Render node micro-cards ── */
    const nodeSel = root.append("g")
      .selectAll("g").data(nodes, d => d.id).join("g")
      .attr("cursor", "pointer")
      .on("click", (ev, d) => { ev.stopPropagation(); onNodeClick(d.id); });

    const cardW = 118, cardH = 46;

    // Card frame
    nodeSel.append("rect")
      .attr("class", "node-card")
      .attr("x", -cardW / 2)
      .attr("y", -cardH / 2)
      .attr("width", cardW)
      .attr("height", cardH)
      .attr("rx", 4)
      .attr("ry", 4)
      .attr("fill", P.bgPanel)
      .attr("stroke", P.border)
      .attr("stroke-width", 1.5);

    // Left sidebar status accent
    nodeSel.append("rect")
      .attr("class", "node-status-bar")
      .attr("x", -cardW / 2 + 1)
      .attr("y", -cardH / 2 + 1)
      .attr("width", 4)
      .attr("height", cardH - 2)
      .attr("rx", 1)
      .attr("fill", P.green);

    // Service Name label
    nodeSel.append("text")
      .attr("class", "node-name")
      .attr("x", -cardW / 2 + 10)
      .attr("y", -cardH / 2 + 18)
      .attr("font-family", "Inter, -apple-system, sans-serif")
      .attr("font-size", 9)
      .attr("font-weight", 600)
      .attr("fill", P.txtBright)
      .text(d => d.name);

    // Group badge code
    nodeSel.append("text")
      .attr("class", "node-group")
      .attr("x", cardW / 2 - 8)
      .attr("y", -cardH / 2 + 17)
      .attr("text-anchor", "end")
      .attr("font-family", "JetBrains Mono, monospace")
      .attr("font-size", 6.5)
      .attr("fill", P.label)
      .text(d => d.group.toUpperCase());

    // Service secondary metric string
    nodeSel.append("text")
      .attr("class", "node-val-text")
      .attr("x", -cardW / 2 + 10)
      .attr("y", cardH / 2 - 10)
      .attr("font-family", "JetBrains Mono, monospace")
      .attr("font-size", 8)
      .attr("fill", P.txt)
      .text("Nominal");

    nodeSelRef.current = nodeSel;

    sim.on("tick", () => {
      edgeSel.attr("x1", d => d.source.x).attr("y1", d => d.source.y)
             .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
      nodeSel.attr("transform", d => `translate(${d.x},${d.y})`);
    });

    /* Live Particle loops */
    const pts = {};
    links.forEach(lk => {
      pts[lk._id] = Array.from({ length: PTCL }, (_, i) => ({
        t: i / PTCL,
        spd: 0.0006 + Math.random() * 0.0005
      }));
    });

    let prev = performance.now();
    function anim(now) {
      const dt = Math.min(now - prev, 50); prev = now;
      const snap = metricsStore.getSnapshot();

      edgeDataRef.current.forEach(lk => {
        const sx = lk.source.x, sy = lk.source.y, tx = lk.target.x, ty = lk.target.y;
        if (!sx || !tx) return;

        const srcId = typeof lk.source === "object" ? lk.source.id : lk.source;
        const m = snap[srcId];
        const spdFactor = (m?.traffic ?? 0.5) * (m?.incident ? 2.0 : 1.0);

        pts[lk._id]?.forEach((p, pi) => {
          p.t += p.spd * spdFactor * dt * 0.06;
          if (p.t > 1) p.t -= 1;
          const opacity = Math.sin(p.t * Math.PI);
          
          root.select(`.p${lk._id}-${pi}`)
            .attr("cx", sx + (tx - sx) * p.t)
            .attr("cy", sy + (ty - sy) * p.t)
            .attr("fill", m?.incident ? P.red : P.teal)
            .attr("r", m?.incident ? 2.2 : 1.5)
            .attr("opacity", opacity * 0.85);
        });
      });
      rafRef.current = requestAnimationFrame(anim);
    }
    rafRef.current = requestAnimationFrame(anim);
    
    return () => {
      sim.stop();
      cancelAnimationFrame(rafRef.current);
    };
  }, [onNodeClick]);

  /* Live health & metric updates inside SVG cards */
  useEffect(() => {
    const unsub = metricsStore.subscribe(() => {
      if (!nodeSelRef.current) return;
      const snap = metricsStore.getSnapshot();
      nodeSelRef.current.each(function(d) {
        const m = snap[d.id];
        if (!m) return;
        
        const col = nodeColor(m);
        const sel = d3.select(this);

        // Update card left border status stripe
        sel.select(".node-status-bar").attr("fill", col);

        // Setup base card border color states
        let borderCol = P.border;
        let isGlow = false;
        let glowFilter = null;

        if (d.id === selectedNode) {
          borderCol = P.orange;
          isGlow = true;
          glowFilter = "url(#glow-orange)";
        } else if (blastStatus === "success" && blastData) {
          const { originId, criticalPath:{nodes:critNodes}, secondaryBlastNodes, upstreamCallers } = blastData;
          const critSet = new Set(critNodes), secSet = new Set(secondaryBlastNodes), upSet = new Set(upstreamCallers);

          if (d.id === originId) {
            borderCol = P.orange;
            glowFilter = "url(#glow-orange)";
          } else if (critSet.has(d.id)) {
            borderCol = P.critPath;
            glowFilter = "url(#glow-red)";
          } else if (secSet.has(d.id)) {
            borderCol = P.slowPath;
          } else if (upSet.has(d.id)) {
            borderCol = P.upstream;
            glowFilter = "url(#glow-upstream)";
          } else {
            sel.attr("opacity", 0.22);
          }
        } else if (m.incident) {
          borderCol = P.red;
          glowFilter = "url(#glow-red)";
        }

        // Apply style mutations
        if (blastStatus !== "success" || !blastData) {
          sel.attr("opacity", 1.0);
        }

        sel.select(".node-card")
          .attr("stroke", borderCol)
          .attr("stroke-width", d.id === selectedNode || borderCol !== P.border ? 2.0 : 1.5)
          .attr("fill", d.id === selectedNode ? "#161e38" : P.bgPanel)
          .attr("filter", glowFilter);

        // Update secondary card label
        const txt = `${m.latencyP99?.toFixed(0)}ms | ${((m.errorRate ?? 0) * 100).toFixed(0)}% err`;
        sel.select(".node-val-text").text(txt).attr("fill", m.incident ? P.red : P.txt);
      });
    });
    return unsub;
  }, [blastStatus, blastData, selectedNode]);

  /* Highlight/Color connectors based on blast path algorithms */
  useEffect(() => {
    if (!edgeSelRef.current) return;
    
    if (blastStatus === "success" && blastData) {
      const { originId, criticalPath:{nodes:critNodes}, secondaryBlastNodes, upstreamCallers } = blastData;
      const critSet = new Set(critNodes), secSet = new Set(secondaryBlastNodes), upSet = new Set(upstreamCallers);

      edgeSelRef.current.each(function(e) {
        const s = typeof e.source === "object" ? e.source.id : e.source;
        const t = typeof e.target === "object" ? e.target.id : e.target;
        const sel = d3.select(this);

        const sIdx = critNodes.indexOf(s), tIdx = critNodes.indexOf(t);
        const isCrit = sIdx >= 0 && tIdx === sIdx + 1;
        const isUp   = upSet.has(s) && (t === originId || upSet.has(t));
        const isSec  = !isCrit && (secSet.has(s) || s === originId) && secSet.has(t);

        if (isCrit) {
          sel.attr("stroke", P.critPath)
             .attr("opacity", 1)
             .attr("stroke-width", 2.5)
             .attr("stroke-dasharray", "none")
             .attr("marker-end", "url(#arr-crit)");
        } else if (isUp) {
          sel.attr("stroke", P.upstream)
             .attr("opacity", 0.75)
             .attr("stroke-width", 1.8)
             .attr("stroke-dasharray", "4 3");
        } else if (isSec) {
          sel.attr("stroke", P.slowPath)
             .attr("opacity", 0.65)
             .attr("stroke-width", 1.5)
             .attr("stroke-dasharray", "3 3");
        } else {
          sel.attr("stroke", P.border)
             .attr("opacity", 0.06)
             .attr("stroke-width", 0.6);
        }
      });
    } else {
      // Default idle state
      edgeSelRef.current
        .attr("stroke", P.border)
        .attr("opacity", 0.75)
        .attr("stroke-width", 1.2)
        .attr("stroke-dasharray", "4 4")
        .attr("marker-end", "url(#arr-dim)");
    }
  }, [blastStatus, blastData]);

  return <svg ref={svgRef} style={{ width: "100%", height: "100%", display: "block" }} />;
});

/* ═══════════════════════════════════════════════════════════════════════════
   SMOOTH SPARKLINE AREA CHART
   ═══════════════════════════════════════════════════════════════════════════ */
const Sparkline = memo(function Sparkline({ history, metricKey, color = P.teal, width = 120, height = 30 }) {
  if (!history || history.length < 2) {
    return (
      <div style={{ width, height, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 8, color: P.txtDim }}>
        collecting metrics...
      </div>
    );
  }

  const values = history.map(h => h[metricKey] ?? 0);
  const minVal = 0;
  
  let maxVal = Math.max(...values, 1);
  if (metricKey === "cpu" || metricKey === "mem") maxVal = 100;
  if (metricKey === "errorRate") maxVal = 1;

  const points = values.map((v, i) => {
    const x = (i / (values.length - 1)) * width;
    const y = height - ((v - minVal) / (maxVal - minVal)) * height;
    return `${x},${y}`;
  });

  const linePath = `M ${points.join(" L ")}`;
  const areaPath = `${linePath} L ${width},${height} L 0,${height} Z`;

  return (
    <svg width={width} height={height} style={{ overflow: "visible", display: "block" }}>
      <path d={areaPath} fill={`${color}12`} stroke="none" />
      <path d={linePath} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
});

/* ═══════════════════════════════════════════════════════════════════════════
   KPI CARD WITH EMBEDDED SPARKLINE
   ═══════════════════════════════════════════════════════════════════════════ */
function KpiTelemetryTile({ label, value, unit, history, metricKey, color, glow = false }) {
  return (
    <div style={{
      padding: "10px 12px",
      background: P.bgCell,
      border: `1px solid ${P.border}`,
      borderRadius: 4,
      boxShadow: glow ? `inset 0 0 10px ${color}12, 0 0 4px ${color}15` : "none",
      display: "flex",
      flexDirection: "column",
      justifyContent: "space-between",
      gap: 6
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <span style={{ fontSize: 8, color: P.label, fontWeight: 600, letterSpacing: "0.08em" }}>{label}</span>
        <span style={{
          fontFamily: "JetBrains Mono, monospace",
          fontSize: 13,
          fontWeight: 700,
          color,
          lineHeight: 1
        }}>
          {value}<span style={{ fontSize: 9, color: P.txtDim, marginLeft: 2, fontWeight: 400 }}>{unit}</span>
        </span>
      </div>
      
      <div style={{ display: "flex", justifyContent: "flex-end", paddingTop: 4 }}>
        <Sparkline history={history} metricKey={metricKey} color={color} width={220} height={28} />
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════════════════
   TELEMETRY INSPECTOR (Supports selected nodes & system cluster overview)
   ═══════════════════════════════════════════════════════════════════════════ */
const TelemetryInspector = memo(function TelemetryInspector({ selectedNode }) {
  const m = useNodeMetrics(selectedNode);
  const [allMetrics, setAllMetrics] = useState({});

  useEffect(() => {
    const unsub = metricsStore.subscribe(() => {
      setAllMetrics(metricsStore.getSnapshot());
    });
    return unsub;
  }, []);

  const history = m?.history ?? [];

  // Compute cluster averages if no node is selected
  const systemSummary = useMemo(() => {
    const nodes = Object.values(allMetrics);
    if (!nodes.length) return null;
    
    const count = nodes.length;
    const avgCpu = nodes.reduce((acc, curr) => acc + curr.cpu, 0) / count;
    const avgMem = nodes.reduce((acc, curr) => acc + curr.mem, 0) / count;
    const avgLatency = nodes.reduce((acc, curr) => acc + curr.latencyP99, 0) / count;
    const totalRps = nodes.reduce((acc, curr) => acc + (curr.rps ?? 0), 0);
    const avgErr = nodes.reduce((acc, curr) => acc + (curr.errorRate ?? 0), 0) / count;

    // Build cluster-wide aggregate history from cache-layer or other node for visual display
    const sampleHistory = allMetrics["k8s-orchestrator"]?.history ?? [];

    return { avgCpu, avgMem, avgLatency, totalRps, avgErr, sampleHistory };
  }, [allMetrics]);

  if (selectedNode && m) {
    const col = nodeColor(m);
    const hState = healthState(m);

    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {/* Info header card */}
        <div style={{
          padding: "10px 12px",
          background: "linear-gradient(135deg, rgba(30,41,59,0.3) 0%, rgba(15,23,42,0.6) 100%)",
          border: `1px solid ${P.border}`,
          borderRadius: 4,
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center"
        }}>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: P.txtBright }}>{m.nodeId}</div>
            <div style={{ fontSize: 7, color: P.label, textTransform: "uppercase", letterSpacing: "0.1em", marginTop: 2 }}>
              Tier: {TOPOLOGY.nodes.find(n => n.id === m.nodeId)?.group}
            </div>
          </div>
          <span style={{
            fontSize: 7.5,
            fontWeight: 700,
            padding: "3px 8px",
            background: `${col}15`,
            border: `1px solid ${col}44`,
            color: col,
            borderRadius: 2,
            letterSpacing: "0.08em"
          }}>
            {hState.toUpperCase()}
          </span>
        </div>

        {/* Tiles grid */}
        <KpiTelemetryTile
          label="CPU UTILIZATION"
          value={m.cpu?.toFixed(1)}
          unit="%"
          history={history}
          metricKey="cpu"
          color={m.cpu > 80 ? P.red : P.teal}
          glow={m.cpu > 80}
        />
        
        <KpiTelemetryTile
          label="MEMORY ALLOCATION"
          value={m.mem?.toFixed(1)}
          unit="%"
          history={history}
          metricKey="mem"
          color={m.mem > 85 ? P.red : P.teal}
          glow={m.mem > 85}
        />

        <KpiTelemetryTile
          label="P99 RESPONSE LATENCY"
          value={m.latencyP99?.toFixed(0)}
          unit="ms"
          history={history}
          metricKey="latencyP99"
          color={m.latencyP99 > 100 ? P.amber : P.teal}
          glow={m.latencyP99 > 100}
        />

        <KpiTelemetryTile
          label="TRAFFIC VOLUME"
          value={m.rps?.toLocaleString()}
          unit="rps"
          history={history}
          metricKey="rps"
          color={P.cyan}
        />

        <KpiTelemetryTile
          label="FAILURE RATE"
          value={((m.errorRate ?? 0) * 100).toFixed(1)}
          unit="%"
          history={history}
          metricKey="errorRate"
          color={(m.errorRate ?? 0) > 0.05 ? P.red : P.green}
          glow={(m.errorRate ?? 0) > 0.05}
        />
      </div>
    );
  }

  // System view layout
  if (!systemSummary) return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{
        padding: "10px 12px",
        background: "linear-gradient(135deg, rgba(30,41,59,0.3) 0%, rgba(15,23,42,0.6) 100%)",
        border: `1px solid ${P.border}`,
        borderRadius: 4,
        fontSize: 10,
        fontWeight: 600,
        color: P.txtBright,
        letterSpacing: "0.05em",
        display: "flex",
        alignItems: "center",
        gap: 6
      }}>
        <div style={{ width: 6, height: 6, borderRadius: "50%", background: P.green }} />
        CLUSTER STACK SUMMARY
      </div>

      <KpiTelemetryTile
        label="CLUSTER CPU AVG"
        value={systemSummary.avgCpu?.toFixed(1)}
        unit="%"
        history={systemSummary.sampleHistory}
        metricKey="cpu"
        color={P.teal}
      />

      <KpiTelemetryTile
        label="CLUSTER MEMORY AVG"
        value={systemSummary.avgMem?.toFixed(1)}
        unit="%"
        history={systemSummary.sampleHistory}
        metricKey="mem"
        color={P.teal}
      />

      <KpiTelemetryTile
        label="LATENCY P99 AVG"
        value={systemSummary.avgLatency?.toFixed(1)}
        unit="ms"
        history={systemSummary.sampleHistory}
        metricKey="latencyP99"
        color={P.teal}
      />

      <KpiTelemetryTile
        label="TOTAL CLUSTER TRAFFIC"
        value={systemSummary.totalRps?.toLocaleString()}
        unit="rps"
        history={systemSummary.sampleHistory}
        metricKey="rps"
        color={P.cyan}
      />
    </div>
  );
});

/* ═══════════════════════════════════════════════════════════════════════════
   CHAOS ENGINEERING FAILURE INJECTION PANEL
   ═══════════════════════════════════════════════════════════════════════════ */
const ChaosControlPanel = memo(function ChaosControlPanel({ activeMode, onSelectMode }) {
  const modes = [
    { id: "db_spike",     name: "Database Latency Spike",   color: P.red, desc: "Cascades to Object Store" },
    { id: "auth_degrade", name: "Auth Vault Degraded",      color: P.amber, desc: "API Gateway connection backlog" },
    { id: "k8s_leak",     name: "Orchestrator Memory Leak", color: P.orange, desc: "Increases Compute nodes load" },
    { id: "bus_partition", name: "Message Bus Outage",       color: P.red, desc: "Halts Log Aggregator flow" }
  ];

  return (
    <div style={{
      padding: "12px",
      background: P.bgPanel,
      border: `1px solid ${P.border}`,
      borderRadius: 4,
      display: "flex",
      flexDirection: "column",
      gap: 10
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke={P.orange} strokeWidth="2">
          <path d="M12 2L2 22h20L12 2zM12 9v4M12 17h.01" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
        <span style={{ fontSize: 9, fontWeight: 700, color: P.txtBright, letterSpacing: "0.1em" }}>CHAOS SIMULATION PANEL</span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {modes.map(m => {
          const isActive = activeMode === m.id;
          return (
            <button
              key={m.id}
              onClick={() => onSelectMode(isActive ? null : m.id)}
              style={{
                width: "100%",
                padding: "8px 10px",
                background: isActive ? `${m.color}15` : "rgba(255,255,255,0.02)",
                border: `1px solid ${isActive ? m.color : P.border}`,
                borderRadius: 3,
                textAlign: "left",
                cursor: "pointer",
                transition: "all 0.2s"
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <span style={{ fontSize: 8.5, fontWeight: 600, color: isActive ? m.color : P.txtBright }}>{m.name}</span>
                <div style={{
                  width: 6, height: 6, borderRadius: "50%",
                  background: isActive ? m.color : P.border,
                  boxShadow: isActive ? `0 0 6px ${m.color}` : "none"
                }} />
              </div>
              <div style={{ fontSize: 7, color: P.label, marginTop: 3 }}>{m.desc}</div>
            </button>
          );
        })}

        {activeMode && (
          <button
            onClick={() => onSelectMode(null)}
            style={{
              width: "100%",
              padding: "6px",
              background: "rgba(255,34,68,0.1)",
              border: `1px solid ${P.red}44`,
              color: P.red,
              fontSize: 8,
              fontWeight: 700,
              cursor: "pointer",
              borderRadius: 3,
              letterSpacing: "0.08em",
              marginTop: 4
            }}
          >
            DISMISS ALL ACTIVE FAILURES
          </button>
        )}
      </div>
    </div>
  );
});

/* ═══════════════════════════════════════════════════════════════════════════
   INCIDENT FEED TIMELINE
   ═══════════════════════════════════════════════════════════════════════════ */
const IncidentTimeline = memo(function IncidentTimeline({ incidents }) {
  if (!incidents.length) return (
    <div style={{ fontSize: 8, color: P.label, textAlign: "center", padding: "16px 0", letterSpacing: "0.05em" }}>
      — ALL CORE INFRASTRUCTURE NOMINAL —
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {incidents.slice(0, 5).map(inc => (
        <div
          key={inc.id}
          style={{
            padding: "8px 10px",
            background: "rgba(239, 68, 68, 0.03)",
            border: `1px solid ${P.border}`,
            borderLeft: `3px solid ${P.red}`,
            borderRadius: 3,
            display: "flex",
            flexDirection: "column",
            gap: 4
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: 8.5, fontWeight: 700, color: P.red }}>{inc.name}</span>
            <span style={{ fontSize: 7.5, color: P.label }}>{inc.time}</span>
          </div>
          <div style={{ fontSize: 7.5, color: P.txt }}>
            Anomaly triggered: latency spiked to <span style={{ fontFamily: "JetBrains Mono, monospace", fontWeight: 700, color: P.amber }}>{inc.latencyP99?.toFixed(0)}ms</span>.
            Z-score: <span style={{ color: P.red, fontWeight: 600 }}>{inc.zScore}σ</span>.
          </div>
        </div>
      ))}
    </div>
  );
});

/* ═══════════════════════════════════════════════════════════════════════════
   BLAST RADIUS DIAGNOSTIC REPORT MODAL
   ═══════════════════════════════════════════════════════════════════════════ */
function BlastReportModal({ blastData, status, errorMsg, onClose }) {
  if (!blastData && status !== "loading" && status !== "error") return null;

  const accentColor = status === "error" ? P.red : P.orange;

  return (
    <div style={{
      position: "absolute",
      bottom: 16,
      right: 16,
      width: 290,
      background: "rgba(15, 23, 42, 0.95)",
      border: `1px solid ${accentColor}44`,
      boxShadow: `0 10px 25px -5px rgba(0, 0, 0, 0.5), 0 0 12px ${accentColor}12`,
      borderRadius: 4,
      zIndex: 60,
      overflow: "hidden",
      backdropFilter: "blur(8px)"
    }}>
      {/* Header bar */}
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: "8px 12px",
        borderBottom: `1px solid ${P.border}`,
        background: `linear-gradient(90deg, ${accentColor}12 0%, transparent 100%)`
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <div style={{ width: 6, height: 6, background: accentColor, borderRadius: "50%" }} />
          <span style={{ fontSize: 9, fontWeight: 700, color: accentColor, letterSpacing: "0.1em" }}>
            {status === "loading" ? "DIJKSTRA GRAPH SOLVER..." : "BLAST RADIUS REPORT"}
          </span>
        </div>
        <button
          onClick={onClose}
          style={{
            background: "none",
            border: "none",
            color: P.txtDim,
            cursor: "pointer",
            fontSize: 10,
            padding: "0 2px"
          }}
        >
          ✕
        </button>
      </div>

      {status === "loading" && (
        <div style={{ padding: "16px 12px" }}>
          <div style={{ height: 2, background: P.border, overflow: "hidden", borderRadius: 1 }}>
            <div style={{ height: "100%", width: "40%", background: P.orange, animation: "slide 1s infinite linear" }} />
          </div>
        </div>
      )}

      {status === "success" && blastData && (
        <div style={{ padding: "12px", display: "flex", flexDirection: "column", gap: 10 }}>
          
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 8.5 }}>
            <span style={{ color: P.label }}>ORIGIN CODE</span>
            <span style={{ color: P.orange, fontWeight: 700 }}>{blastData.originId}</span>
          </div>

          <div style={{ height: 1, background: P.border }} />

          {/* Key Indicators */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
            <div style={{ padding: "6px", background: P.bgCell, border: `1px solid ${P.border}`, borderRadius: 2 }}>
              <div style={{ fontSize: 7, color: P.label, letterSpacing: "0.05em", marginBottom: 2 }}>IMPACT INDEX</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: blastData.impactScore > 70 ? P.red : P.amber }}>
                {blastData.impactScore}<span style={{ fontSize: 9, color: P.label }}>/100</span>
              </div>
            </div>
            <div style={{ padding: "6px", background: P.bgCell, border: `1px solid ${P.border}`, borderRadius: 2 }}>
              <div style={{ fontSize: 7, color: P.label, letterSpacing: "0.05em", marginBottom: 2 }}>ESTIMATED MTTR</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: P.amber }}>
                {blastData.estimatedMttrMinutes}<span style={{ fontSize: 8, color: P.label, marginLeft: 2 }}>MIN</span>
              </div>
            </div>
          </div>

          <div style={{ padding: "6px", background: P.bgCell, border: `1px solid ${P.border}`, borderRadius: 2 }}>
            <div style={{ fontSize: 7, color: P.label, letterSpacing: "0.05em", marginBottom: 2 }}>PROPAGATED CRITICAL LATENCY</div>
            <div style={{ fontSize: 12, fontWeight: 700, color: P.critPath }}>
              {blastData.criticalPath.totalLatencyMs?.toFixed(0)}<span style={{ fontSize: 8, color: P.label, marginLeft: 2 }}>ms</span>
            </div>
          </div>

          <div style={{ height: 1, background: P.border }} />

          {/* Critical Path nodes */}
          <div>
            <div style={{ fontSize: 7.5, color: P.critPath, fontWeight: 700, letterSpacing: "0.05em", marginBottom: 4 }}>
              ▸ CRITICAL FAIL-PROPAGATION PATH ({blastData.criticalPath.nodes.length - 1} HOPS)
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 3, alignItems: "center" }}>
              {blastData.criticalPath.nodes.map((node, i) => (
                <span key={node} style={{ display: "flex", alignItems: "center", gap: 3 }}>
                  <span style={{
                    fontSize: 7.5,
                    fontWeight: 600,
                    padding: "2px 5px",
                    background: node === blastData.originId ? "rgba(249,115,22,0.1)" : "rgba(239,68,68,0.1)",
                    border: `1px solid ${node === blastData.originId ? P.orange : P.critPath}33`,
                    color: node === blastData.originId ? P.orange : P.critPath,
                    borderRadius: 2
                  }}>
                    {node}
                  </span>
                  {i < blastData.criticalPath.nodes.length - 1 && <span style={{ fontSize: 8, color: P.txtDim }}>→</span>}
                </span>
              ))}
            </div>
          </div>

          {/* Secondary nodes */}
          {blastData.secondaryBlastNodes?.length > 0 && (
            <div>
              <div style={{ fontSize: 7.5, color: P.slowPath, fontWeight: 700, letterSpacing: "0.05em", marginBottom: 4 }}>
                ▸ SECONDARY DEGREDATION TARGETS
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
                {blastData.secondaryBlastNodes.map(node => (
                  <span key={node} style={{
                    fontSize: 7.5,
                    padding: "2px 5px",
                    background: "rgba(249,115,22,0.06)",
                    border: `1px solid ${P.slowPath}22`,
                    color: P.slowPath,
                    borderRadius: 2
                  }}>
                    {node}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Upstream Callers */}
          {blastData.upstreamCallers?.length > 0 && (
            <div>
              <div style={{ fontSize: 7.5, color: P.upstream, fontWeight: 700, letterSpacing: "0.05em", marginBottom: 4 }}>
                ▸ AFFECTED UPSTREAM CALLERS
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
                {blastData.upstreamCallers.map(node => (
                  <span key={node} style={{
                    fontSize: 7.5,
                    padding: "2px 5px",
                    background: "rgba(234,179,8,0.06)",
                    border: `1px solid ${P.upstream}22`,
                    color: P.upstream,
                    borderRadius: 2
                  }}>
                    {node}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* local computation watermark tag */}
          {blastData.isLocalFallback && (
            <div style={{
              fontSize: 6.5,
              color: P.label,
              background: "rgba(255,255,255,0.02)",
              border: `1px solid ${P.border}`,
              padding: "4px 6px",
              borderRadius: 2,
              textAlign: "center",
              letterSpacing: "0.08em"
            }}>
              LOCAL GRAPH PATHFINDING ENGINE ACTIVE
            </div>
          )}

        </div>
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════════════════
   TOP STATUS BAR
   ═══════════════════════════════════════════════════════════════════════════ */
function StatusBar({ activeIncidentCount, selectedNode, blastStatus, currentChaosMode }) {
  const [time, setTime] = useState(new Date().toLocaleTimeString("en-GB"));
  const [uptimeSec, setUptime] = useState(0);

  useEffect(() => {
    const t = setInterval(() => setTime(new Date().toLocaleTimeString("en-GB")), 1000);
    const u = setInterval(() => setUptime(x => x + 1), 1000);
    return () => { clearInterval(t); clearInterval(u); };
  }, []);

  const uptimeStr = `${String(Math.floor(uptimeSec / 3600)).padStart(2, "0")}:${String(Math.floor((uptimeSec % 3600) / 60)).padStart(2, "0")}:${String(uptimeSec % 60).padStart(2, "0")}`;

  return (
    <header style={{
      display: "flex",
      alignItems: "stretch",
      height: 48,
      background: "#030712",
      borderBottom: `1px solid ${P.border}`,
      flexShrink: 0,
      fontFamily: "Inter, -apple-system, sans-serif"
    }}>
      {/* Brand logo & title container */}
      <div style={{
        display: "flex",
        alignItems: "center",
        padding: "0 18px",
        borderRight: `1px solid ${P.border}`,
        gap: 10
      }}>
        {/* Custom SVG logo based on user image vector structure */}
        <svg width="24" height="24" viewBox="0 0 80 80" fill="none">
          {/* Outer hexagon */}
          <polygon points="40,4 72,22.5 72,57.5 40,76 8,57.5 8,22.5" stroke={P.teal} strokeWidth="2.5"/>
          {/* Inner Hexagon */}
          <polygon points="40,22 59,33 59,55 40,66 21,55 21,33" stroke={P.teal} strokeWidth="1.2" opacity="0.8"/>
          {/* Triangulation Lines */}
          <line x1="40" y1="4" x2="40" y2="22" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="72" y1="22.5" x2="59" y2="33" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="40" y1="4" x2="59" y2="33" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="72" y1="22.5" x2="40" y2="22" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="72" y1="22.5" x2="72" y2="57.5" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="72" y1="57.5" x2="59" y2="55" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="72" y1="22.5" x2="59" y2="55" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="72" y1="57.5" x2="59" y2="33" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="72" y1="57.5" x2="40" y2="76" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="40" y1="76" x2="40" y2="66" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="72" y1="57.5" x2="40" y2="66" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="40" y1="76" x2="59" y2="55" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="40" y1="76" x2="8" y2="57.5" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="8" y1="57.5" x2="21" y2="55" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="40" y1="76" x2="21" y2="55" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="8" y1="57.5" x2="40" y2="66" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="8" y1="57.5" x2="8" y2="22.5" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="8" y1="22.5" x2="21" y2="33" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="8" y1="57.5" x2="21" y2="33" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="8" y1="22.5" x2="21" y2="55" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="8" y1="22.5" x2="40" y2="4" stroke={P.teal} strokeWidth="0.8" opacity="0.6"/>
          <line x1="40" y1="4" x2="21" y2="33" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          <line x1="8" y1="22.5" x2="40" y2="22" stroke={P.teal} strokeWidth="0.6" opacity="0.4"/>
          {/* Inner 3D Cube core */}
          <polygon points="40,27 53,34.5 53,49.5 40,57 27,49.5 27,34.5" fill="#ffffff"/>
          {/* Cube cuts */}
          <line x1="40" y1="42" x2="40" y2="57" stroke="#030712" strokeWidth="1.8"/>
          <line x1="40" y1="42" x2="27" y2="34.5" stroke="#030712" strokeWidth="1.8"/>
          <line x1="40" y1="42" x2="53" y2="34.5" stroke="#030712" strokeWidth="1.8"/>
          <path d="M 27 42.5 L 40 50 L 53 42.5" stroke="#030712" strokeWidth="1.5" />
          <path d="M 40 27 L 40 42" stroke="#030712" strokeWidth="1.8" />
        </svg>
        
        <div>
          <div style={{ fontSize: 12, fontWeight: 800, color: P.txtBright, letterSpacing: "0.12em", lineHeight: 1 }}>ATLAS SYSTEMS</div>
          <div style={{ fontSize: 7.5, color: P.label, letterSpacing: "0.18em", marginTop: 2 }}>MISSION CONTROL ROOM</div>
        </div>
      </div>

      {/* Middle status badges */}
      <div style={{ flex: 1, display: "flex", alignItems: "center", padding: "0 18px", gap: 16 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <div style={{ width: 6, height: 6, borderRadius: "50%", background: P.green, boxShadow: `0 0 8px ${P.green}` }} />
          <span style={{ fontSize: 8.5, fontWeight: 700, color: P.green, letterSpacing: "0.08em" }}>TELEMETRY CONNECTED</span>
        </div>

        {selectedNode && (
          <>
            <div style={{ width: 1, height: 16, background: P.border }} />
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{ width: 6, height: 6, background: P.orange, transform: "rotate(45deg)" }} />
              <span style={{ fontSize: 8.5, fontWeight: 700, color: P.orange, letterSpacing: "0.05em" }}>
                {blastStatus === "loading" ? "RESOLVING PATHWAYS..." : `INSPECTING BLAST: ${selectedNode}`}
              </span>
            </div>
          </>
        )}

        {currentChaosMode && (
          <>
            <div style={{ width: 1, height: 16, background: P.border }} />
            <div style={{ display: "flex", alignItems: "center", gap: 6, background: "rgba(239, 68, 68, 0.1)", padding: "4px 8px", borderRadius: 2, border: `1px solid ${P.red}33` }}>
              <span style={{ fontSize: 8, fontWeight: 800, color: P.red, letterSpacing: "0.08em" }}>
                ACTIVE SIMULATION INCIDENT: {currentChaosMode.toUpperCase()}
              </span>
            </div>
          </>
        )}
      </div>

      {/* Right meta data bar */}
      <div style={{ display: "flex", alignItems: "stretch" }}>
        <div style={{ display: "flex", flexDirection: "column", justifyContent: "center", padding: "0 18px", borderLeft: `1px solid ${P.border}`, minWidth: 90 }}>
          <span style={{ fontSize: 6.5, color: P.label, letterSpacing: "0.08em" }}>SYSTEM UPTIME</span>
          <span style={{ fontSize: 10, fontWeight: 700, color: P.txtBright, fontFamily: "JetBrains Mono, monospace", marginTop: 2 }}>{uptimeStr}</span>
        </div>
        <div style={{ display: "flex", flexDirection: "column", justifyContent: "center", padding: "0 18px", borderLeft: `1px solid ${P.border}`, minWidth: 90 }}>
          <span style={{ fontSize: 6.5, color: P.label, letterSpacing: "0.08em" }}>UTC CLOCK</span>
          <span style={{ fontSize: 10, fontWeight: 700, color: P.txtBright, fontFamily: "JetBrains Mono, monospace", marginTop: 2 }}>{time}</span>
        </div>
      </div>
    </header>
  );
}

/* ═══════════════════════════════════════════════════════════════════════════
   ROOT COMPONENT
   ═══════════════════════════════════════════════════════════════════════════ */
export default function InfraMissionControl() {
  const [selectedNode, setSelectedNode] = useState(null);
  const [chaosMode, setChaosMode] = useState(null); // 'db_spike', 'auth_degrade', 'k8s_leak', 'bus_partition', null

  const { incidents, setIncidents } = useSSEStream(chaosMode);
  const activeCount = new Set(incidents.map(i => i.nodeId)).size;
  
  const { status: blastStatus, blastData, errorMsg } = useFetchBlastRadius(selectedNode);

  const handleNodeClick = useCallback(id => {
    setSelectedNode(prev => prev === id ? null : id);
  }, []);

  const handleCloseBlastModal = useCallback(() => {
    setSelectedNode(null);
  }, []);

  const handleSelectChaosMode = useCallback((mode) => {
    setChaosMode(mode);
    if (!mode) {
      setIncidents([]); // Clear timeline
    }
  }, [setIncidents]);

  return (
    <>
      <style>{`
        body {
          margin: 0;
          padding: 0;
          background-color: ${P.bg};
          font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
          color: ${P.txt};
          overflow: hidden;
        }

        /* Subtle dark dot pattern on topology grid */
        .grid-bg {
          background-color: ${P.bg};
          background-image: radial-gradient(${P.border} 1.2px, transparent 1.2px);
          background-size: 24px 24px;
        }

        /* Slide animation for blast report loading bar */
        @keyframes slide {
          from { transform: translateX(-100%); }
          to { transform: translateX(250%); }
        }

        /* Custom scrollbars */
        ::-webkit-scrollbar {
          width: 4px;
          height: 4px;
        }
        ::-webkit-scrollbar-track {
          background: ${P.bg};
        }
        ::-webkit-scrollbar-thumb {
          background: ${P.border};
          border-radius: 2px;
        }
        ::-webkit-scrollbar-thumb:hover {
          background: ${P.borderLight};
        }
      `}</style>

      <div style={{
        width: "100%",
        height: "100vh",
        display: "flex",
        flexDirection: "column",
        background: P.bg,
      }}>
        {/* Header bar */}
        <StatusBar
          activeIncidentCount={activeCount}
          selectedNode={selectedNode}
          blastStatus={blastStatus}
          currentChaosMode={chaosMode}
        />

        {/* Content body split */}
        <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>
          
          {/* ── LEFT SIDEBAR (Alert Timeline & Chaos Engineering) ── */}
          <div style={{
            width: 280,
            background: P.bgPanel,
            borderRight: `1px solid ${P.border}`,
            display: "flex",
            flexDirection: "column",
            overflow: "hidden",
            flexShrink: 0
          }}>
            {/* Simulation triggers */}
            <div style={{ padding: "12px", borderBottom: `1px solid ${P.border}` }}>
              <ChaosControlPanel activeMode={chaosMode} onSelectMode={handleSelectChaosMode} />
            </div>

            {/* Incidents feed */}
            <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
              <div style={{
                padding: "8px 12px",
                borderBottom: `1px solid ${P.border}`,
                fontSize: 8.5,
                fontWeight: 700,
                color: P.txtBright,
                letterSpacing: "0.1em",
                display: "flex",
                justifyContent: "space-between",
                background: "rgba(255,255,255,0.01)"
              }}>
                <span>ACTIVE ANOMALY FEED</span>
                <span style={{ color: activeCount > 0 ? P.red : P.label }}>{activeCount} alerts</span>
              </div>
              <div style={{ flex: 1, overflowY: "auto", padding: "12px" }}>
                <IncidentTimeline incidents={incidents} />
              </div>
            </div>
          </div>

          {/* ── MIDDLE WORKSPACE (Network topology D3 canvas) ── */}
          <div style={{
            flex: 1,
            position: "relative",
            overflow: "hidden",
            display: "flex",
            flexDirection: "column"
          }}>
            <div style={{
              padding: "8px 16px",
              borderBottom: `1px solid ${P.border}`,
              background: P.bgPanel,
              fontSize: 9,
              fontWeight: 700,
              color: P.txtBright,
              letterSpacing: "0.05em",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center"
            }}>
              <span>INFRASTRUCTURE TOPOLOGY GRAPH</span>
              <span style={{ fontSize: 7.5, color: P.label }}>
                {selectedNode ? "CLICK EMPTY CANVAS TO CLEAR SELECTION" : "CLICK NODE TO RUN BLAST RADIUS TRAVERSAL"}
              </span>
            </div>

            <div className="grid-bg" style={{ flex: 1, position: "relative", overflow: "hidden" }}>
              <TopologyGraph
                selectedNode={selectedNode}
                blastStatus={blastStatus}
                blastData={blastData}
                onNodeClick={handleNodeClick}
              />
              <BlastReportModal
                blastData={blastData}
                status={blastStatus}
                errorMsg={errorMsg}
                onClose={handleCloseBlastModal}
              />
            </div>

            {/* Graph Legend Strip */}
            <div style={{
              display: "flex",
              alignItems: "center",
              padding: "8px 16px",
              background: P.bgPanel,
              borderTop: `1px solid ${P.border}`,
              gap: 20
            }}>
              {(blastStatus === "success" && blastData
                ? [
                    { c: P.orange, l: "CRASH ORIGIN" },
                    { c: P.critPath, l: "DIJKSTRA CRITICAL PATH" },
                    { c: P.slowPath, l: "SECONDARY BLAST TARGETS" },
                    { c: P.upstream, l: "AFFECTED CALLERS" }
                  ]
                : [
                    { c: P.green, l: "HEALTHY SERVICE" },
                    { c: P.amber, l: "WARNING DEGRADED" },
                    { c: P.red, l: "CRITICAL FAILURE" }
                  ]
              ).map(item => (
                <div key={item.l} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <div style={{ width: 6, height: 6, background: item.c, borderRadius: "50%" }} />
                  <span style={{ fontSize: 7.5, fontWeight: 600, color: P.label, letterSpacing: "0.05em" }}>{item.l}</span>
                </div>
              ))}
              <span style={{ fontSize: 7, color: P.txtDim, marginLeft: "auto", letterSpacing: "0.08em" }}>
                GRID AUTO-ALIGN: 10 ACTIVE ARCHITECTURE SENSORS
              </span>
            </div>
          </div>

          {/* ── RIGHT TELEMETRY INSPECTOR SIDEBAR ── */}
          <div style={{
            width: 290,
            background: P.bgPanel,
            borderLeft: `1px solid ${P.border}`,
            display: "flex",
            flexDirection: "column",
            overflow: "hidden",
            flexShrink: 0
          }}>
            <div style={{
              padding: "8px 12px",
              borderBottom: `1px solid ${P.border}`,
              fontSize: 8.5,
              fontWeight: 700,
              color: P.txtBright,
              letterSpacing: "0.1em",
              background: "rgba(255,255,255,0.01)"
            }}>
              {selectedNode ? "SELECTED NODE TELEMETRY" : "CLUSTER METRIC OVERVIEW"}
            </div>
            
            <div style={{ flex: 1, overflowY: "auto", padding: "12px" }}>
              <TelemetryInspector selectedNode={selectedNode} />
            </div>
          </div>

        </div>
      </div>
    </>
  );
}
