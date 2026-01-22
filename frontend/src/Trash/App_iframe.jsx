import { useRef, useState, useEffect } from "react";

export default function App() {
  const iframeRef = useRef(null);
  const [iframeReady, setIframeReady] = useState(false);

  // Listen for iframe ready message
  useEffect(() => {
    const handleMessage = (event) => {
      if (event.data?.type === "iframe-ready") {
        console.log("Iframe is ready!");
        setIframeReady(true);
      }
    };
    window.addEventListener("message", handleMessage);
    return () => window.removeEventListener("message", handleMessage);
  }, []);

  const handleToggleLabels = (checked) => {
    if (!iframeReady || !iframeRef.current?.contentWindow) {
      console.warn("Iframe not ready");
      return;
    }
    iframeRef.current.contentWindow.postMessage(
      { action: "toggleLabels", payload: checked },
      "*" // use "*" for local testing
    );
  };

  return (
    <div style={{ padding: "20px" }}>
      <h1>React Control Panel</h1>
      <label>
        <input
          type="checkbox"
          onChange={(e) => handleToggleLabels(e.target.checked)}
        />{" "}
        Show Labels
      </label>

      <div style={{ marginTop: "20px", height: "300px" }}>
        <iframe
          ref={iframeRef}
          src="/iframe.html"   // make sure this path is correct
          width="100%"
          height="100%"
          title="Graph Iframe"
          style={{ border: "1px solid #aaa" }}
        />
      </div>
    </div>
  );
}
