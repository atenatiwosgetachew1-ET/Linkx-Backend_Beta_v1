import React, { useRef } from 'react';

function Window({ id, title, children, position, onClose, onDrag }) {
  const windowRef = useRef();

  const handleMouseDown = (e) => {
    // Initiate drag
    const startX = e.clientX;
    const startY = e.clientY;

    const handleMouseMove = (moveEvent) => {
      const deltaX = moveEvent.clientX - startX;
      const deltaY = moveEvent.clientY - startY;
      onDrag(id, { x: position.x + deltaX, y: position.y + deltaY });
    };

    const handleMouseUp = () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
  };

  return (
    <div
      className="window"
      style={{
        position: 'absolute',
        top: position.y,
        left: position.x,
        border: '1px solid #ccc',
        background: '#fff',
        width: 300,
        height: 200,
        boxShadow: '2px 2px 10px rgba(0,0,0,0.2)',
        zIndex: 1000,
      }}
    >
      <div
        className="window-header"
        style={{
          background: '#eee',
          cursor: 'move',
          padding: '4px',
          display: 'flex',
          justifyContent: 'space-between',
        }}
        onMouseDown={handleMouseDown}
      >
        <span>{title}</span>
        <button onClick={() => onClose(id)} style={{ cursor: 'pointer' }}>X</button>
      </div>
      <div className="window-content" style={{ padding: '8px', height: 'calc(100% - 30px)', overflow: 'auto' }}>
        {children}
      </div>
    </div>
  );
}

export default Window;