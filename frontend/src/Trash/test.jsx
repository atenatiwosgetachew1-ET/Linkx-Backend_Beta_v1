import React, { useRef, useEffect, useState } from 'react';

function LogViewer() {
  const textareaRef = useRef(null);
  const [sourceSessionLog, setSourceSessionLog] = useState('');

  // Simulate log updates
  useEffect(() => {
    const interval = setInterval(() => {
      setSourceSessionLog(prev => prev + 'New log entry...\n');
    }, 2000); // Add new log every 2 seconds

    return () => clearInterval(interval);
  }, []);

  // Scroll to bottom whenever sourceSessionLog updates
  useEffect(() => {
    if (textareaRef.current) {
      // Delay to ensure DOM has updated
      setTimeout(() => {
        textareaRef.current.scrollTo({
          top: textareaRef.current.scrollHeight,
          behavior: 'smooth',
        });
      }, 0);
    }
  }, [sourceSessionLog]);

  return (
    <div>
      <h2>Log Viewer</h2>
      <textarea
        ref={textareaRef}
        className="batch_files_dataframe_filter_log_textarea"
        readOnly
        value={sourceSessionLog}
        rows={10}
        style={{ width: '100%', height: '200px' }}
      />
    </div>
  );
}

export default LogViewer;