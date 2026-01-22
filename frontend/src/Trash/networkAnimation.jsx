import React, { useRef, useEffect } from "react";

const MinimalLeftGlobe = () => {
  const canvasRef = useRef(null);
  const overlayRef = useRef(null);
  const animationRef = useRef(null);
  const widthRef = useRef(window.innerWidth);
  const heightRef = useRef(window.innerHeight);
  const timeRef = useRef(0);

  // Generate nodes for the network
  const nodesRef = useRef([]);
  const linksRef = useRef([]);

  // Initialize nodes and links
  const initNetwork = (width, height) => {
    const nodeCount = 150;
    const nodes = [];
    for (let i = 0; i < nodeCount; i++) {
      const size = 2 + Math.random() * 10; // size between 2 and 6
      // Larger size => higher contrast (brighter color)
      const contrastFactor = (size - 2) / 4; // 0 to 1
      const colorHue = 200; // blue hue for contrast
      const colorSaturation = 70 + contrastFactor * 30; // 70% to 100%
      const colorBrightness = 40 + contrastFactor * 40; // 40% to 80%
      nodes.push({
        id: i,
        x: Math.random() * width,
        y: Math.random() * height,
        vx: (Math.random() - 0.5) * 0.5,
        vy: (Math.random() - 0.5) * 0.5,
        size: size,
        color: `hsl(${colorHue}, ${colorSaturation}%, ${colorBrightness}%)`,
      });
    }
    // Create links based on proximity
    const links = [];
    for (let i = 0; i < nodeCount; i++) {
      for (let j = i + 1; j < nodeCount; j++) {
        const dx = nodes[i].x - nodes[j].x;
        const dy = nodes[i].y - nodes[j].y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 150) {
          links.push({ source: i, target: j });
        }
      }
    }
    nodesRef.current = nodes;
    linksRef.current = links;
  };

  const globeRadius = Math.max(window.innerWidth, window.innerHeight) * 0.65;
  const rotationSpeed = 0.0004;

  useEffect(() => {
    const canvas = canvasRef.current;
    const overlayCanvas = overlayRef.current;
    const ctx = canvas.getContext("2d");
    const overlayCtx = overlayCanvas.getContext("2d");
    const width = (widthRef.current = window.innerWidth);
    const height = (heightRef.current = window.innerHeight);
    canvas.width = overlayCanvas.width = width;
    canvas.height = overlayCanvas.height = height;

    // Initialize network with variable node sizes and colors
    initNetwork(width, height);

    const animate = () => {
      const { current: nodes } = nodesRef;
      const { current: links } = linksRef;
      const width = widthRef.current;
      const height = heightRef.current;
      timeRef.current += rotationSpeed;

      const R = Math.max(width, height) * 0.65;

      // Animate nodes
      nodes.forEach(node => {
        node.x += node.vx;
        node.y += node.vy;
        if (node.x < 0 || node.x > width) {
          node.vx *= -1;
        }
        if (node.y < 0 || node.y > height) {
          node.vy *= -1;
        }
      });

      // Draw background
      const gradient = ctx.createRadialGradient(
        width / 2,
        height / 2,
        0,
        width / 2,
        height / 2,
        width * 0.8
      );
      gradient.addColorStop(0, "#FFFFFF");
      gradient.addColorStop(1, "#EEEEEE");
      ctx.fillStyle = gradient;
      ctx.fillRect(0, 0, width, height);

      ctx.save();
      ctx.translate(width * 1, height / 2);

      const cosY = Math.cos(timeRef.current);
      const sinY = Math.sin(timeRef.current);
      const cosX = Math.cos(timeRef.current * 0.6);
      const sinX = Math.sin(timeRef.current * 0.6);

      ctx.strokeStyle = "rgba(0, 0, 0, 0.1)";
      ctx.lineWidth = 0.5;

      // Draw latitude lines
      for (let i = -80; i <= 80; i += 20) {
        ctx.beginPath();
        for (let lon = 0; lon <= 360; lon += 5) {
          const phi = (lon * Math.PI) / 180;
          const lat = (i * Math.PI) / 180;
          const x = Math.cos(lat) * Math.cos(phi);
          const y = Math.sin(lat);
          const z = Math.cos(lat) * Math.sin(phi);
          const xr = x * cosY - z * sinY;
          const zr = x * sinY + z * cosY;
          const yr = y * cosX - zr * sinX;
          const perspective = 400 / (400 + zr * R);
          if (perspective > 0) {
            const sx = xr * R * perspective;
            const sy = yr * R * perspective;
            if (lon === 0) ctx.moveTo(sx, sy);
            else ctx.lineTo(sx, sy);
          }
        }
        ctx.stroke();
      }

      // Draw longitude lines
      for (let lon = 0; lon < 360; lon += 20) {
        ctx.beginPath();
        for (let lat = -80; lat <= 80; lat += 5) {
          const phi = (lon * Math.PI) / 180;
          const theta = (lat * Math.PI) / 180;
          const x = Math.cos(theta) * Math.cos(phi);
          const y = Math.sin(theta);
          const z = Math.cos(theta) * Math.sin(phi);
          const xr = x * cosY - z * sinY;
          const zr = x * sinY + z * cosY;
          const yr = y * cosX - zr * sinX;
          const perspective = 400 / (400 + zr * R);
          if (perspective > 0) {
            const sx = xr * R * perspective;
            const sy = yr * R * perspective;
            if (lat === -80) ctx.moveTo(sx, sy);
            else ctx.lineTo(sx, sy);
          }
        }
        ctx.stroke();
      }
      ctx.restore();

      // Draw network links
      ctx.lineWidth = 0.2;
      ctx.strokeStyle = "rgba(0,0,0,0.1)";
      links.forEach(link => {
        const source = nodes[link.source];
        const target = nodes[link.target];
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.lineTo(target.x, target.y);
        ctx.stroke();
      });

      // Draw nodes with variable size and color
      nodes.forEach(node => {
        ctx.beginPath();
        ctx.arc(node.x, node.y, node.size, 0, Math.PI * 2);
        ctx.fillStyle = node.color;
        ctx.fill();
      });

      // Clear overlay for siren lights
      overlayCtx.clearRect(0, 0, width, height);

      // Draw siren-like lights across entire page (overlay)
      const numLights = 5;
      for (let i = 0; i < numLights; i++) {
        const angle = (timeRef.current * 0.8 + (i * Math.PI * 2) / numLights) % (Math.PI * 2);
        const radius = Math.max(width, height) * 0.7;
        const x = Math.cos(angle) * radius;
        const y = Math.sin(angle) * radius;

        // Pulsate and change hue
        const hue = (timeRef.current * 30 + i * 60) % 360;
        const opacity = 0.2 + Math.sin(timeRef.current + i * Math.PI) * 0.15;

        // Draw beam
        overlayCtx.save();
        overlayCtx.translate(width / 2 + x, height / 2 + y);
        overlayCtx.rotate(angle);
        const gradient = overlayCtx.createRadialGradient(0, 0, 0, 0, 0, 100);
        gradient.addColorStop(0, `hsla(${hue}, 100%, 50%, ${opacity})`);
        gradient.addColorStop(1, `hsla(${hue}, 100%, 50%, 0)`);
        overlayCtx.fillStyle = gradient;
        overlayCtx.beginPath();
        overlayCtx.moveTo(0, 0);
        overlayCtx.arc(0, 0, 100, -Math.PI / 8, Math.PI / 8);
        overlayCtx.lineTo(0, 0);
        overlayCtx.fill();
        overlayCtx.restore();
      }

      // Draw "Linkx" at bottom right with floating animation
      ctx.font = "bold 48px sans-serif";
      ctx.fillStyle = "rgba(0,0,0,0.15)";
      ctx.textAlign = "right";
      ctx.textBaseline = "bottom";
      const floatOffset = Math.sin(timeRef.current * 2) * 5;
      ctx.save();
      ctx.translate(width - 20, height - 20 + floatOffset);
      ctx.fillText("Linkx", 0, 0);
      ctx.restore();

      // Continue animation
      animationRef.current = requestAnimationFrame(animate);
    };

    animate();

    const handleResize = () => {
      const newWidth = window.innerWidth;
      const newHeight = window.innerHeight;
      widthRef.current = newWidth;
      heightRef.current = newHeight;
      canvas.width = overlayCanvas.width = newWidth;
      canvas.height = overlayCanvas.height = newHeight;
      initNetwork(newWidth, newHeight);
    };
    window.addEventListener("resize", handleResize);

    return () => {
      cancelAnimationFrame(animationRef.current);
      window.removeEventListener("resize", handleResize);
    };
  }, []);

  return (
    <>
      <canvas
        ref={canvasRef}
        style={{
          position: "fixed",
          top: 0,
          left: 0,
          width: "100vw",
          height: "100vh",
          zIndex: 0,
          backgroundColor: "#fff",
          display: "block",
        }}
      />
      <canvas
        ref={overlayRef}
        style={{
          position: "fixed",
          top: 0,
          left: 0,
          width: "100vw",
          height: "100vh",
          zIndex: 1,
          pointerEvents: "none",
        }}
      />
    </>
  );
};

export default MinimalLeftGlobe;