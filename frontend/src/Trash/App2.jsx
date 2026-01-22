import React, { useState, useRef, useEffect, createContext, useContext, useCallback } from 'react';
import { io } from 'socket.io-client';

import './main.css'
import NetworkBackground from './networkAnimation.jsx'
//importing the icons function
import Icons from './icons.jsx'
// importing action functions
import ToggleMenuActions from './ToggleMenuActions.jsx'
import NavBarActions from './NavBarActions.jsx'
import WindowsActions from './WindowsActions.jsx'


function ToggleMenu({ onToggle, isToggleMenuOpen, action}){
  return(
    <div id='toggle_menu'>
      <div className="animated_logo">
        <span>
          <i>
            <a></a>
          </i>
        </span>
        <label>Linkx | <i>Web Analyzer</i></label>
      </div>
      <div className="toggle_menu_list_container" style={{
            width: isToggleMenuOpen ? '14.5vw' : '3vw'}}>
        <div className="toggle_menu_list" style={{
            width: isToggleMenuOpen ? '14.5vw' : '3vw'}}>
          <i className="tmicon"  
          style={{
            backgroundColor: isToggleMenuOpen ? '#222' : '#333'}} onClick={onToggle}>
            <u className={isToggleMenuOpen ? 'tmicon_svg_container_active' : 'tmicon_svg_container'}><Icons id="toggle_menu" className="tmicon_svg_container" type="new_window" condition="True"/></u></i>
          <div className="toogle_menu_btn_options" 
          style={{
            display: isToggleMenuOpen ? 'block' : 'none'}}>
            <ul>
              <div className="toogle_menu_btn_options_li_container">  
                <li onClick={() => action("toggle_menu_new_source_window")}>    
                  <i>
                    {/*<Icons id="window_side_bar" type="live_source" condition="True"/>*/}
                  </i>   
                  <span>+ &nbsp;Source window</span>
                </li>
                <li onClick={() => action("toggle_menu_new_graph_window")}>    
                  <i>
                    {/*<Icons id="window_side_bar" type="live_source" condition="True"/>*/}
                  </i>              
                  <span>+ &nbsp;Graph window</span>
                </li>
                <li onClick={() => action("toggle_menu_new_chart_window")}>    
                  <i>
                    {/*<Icons id="window_side_bar" type="live_source" condition="True"/>*/}
                  </i>              
                  <span>+ &nbsp;Chart window</span>
                </li>
                <li onClick={() => action("toggle_menu_new_table_window")}>    
                  <i>
                    {/*<Icons id="window_side_bar" type="live_source" condition="True"/>*/}
                  </i>              
                  <span>+ &nbsp;Tabular window</span>
                </li>
              </div>
            </ul>
          </div>
        </div>

      </div>
    </div>
  );
}
function NavBar({ onNavAction }) {
  return (
    <nav id='nav_bar'>
      <span onClick={() => onNavAction('login')}>Login</span>
      <span onClick={() => onNavAction('about')}>About</span>
    </nav>
  );
}
function Taskbar(){

}
function WindowVerticalSplitPanels({ initialTopHeight, minTopHeight, maxTopHeight,graphRelationships}) {
  const containerRef = useRef(null);
  const [containerHeight, setContainerHeight] = useState(0);
  const [topHeightPx, setTopHeightPx] = useState(0);
  const isDragging = useRef(false);
  const minHeightPx = useRef(0);
  const maxHeightPx = useRef(0);
  // Update container height
  useEffect(() => {
    const updateContainerHeight = () => {
      if (containerRef.current) {
        setContainerHeight(containerRef.current.clientHeight);
      }
    };
    updateContainerHeight();
    window.addEventListener('resize', updateContainerHeight);
    return () => window.removeEventListener('resize', updateContainerHeight);
  }, []);
  // Parse function 
  function parseSize(size, containerSize) { // This function is used to estimate the percentile relative to number base
  if (typeof size === 'string' && size.trim().endsWith('%')) {
    const percent = parseFloat(size);
    return (percent / 100) * containerSize;
  }
  return typeof size === 'number' ? size : 0;
  }
  // Parse sizes once (call the above function)
  useEffect(() => {
    if (containerHeight > 0) {
      const initHeight = parseSize(initialTopHeight, containerHeight);
      setTopHeightPx(initHeight);
      minHeightPx.current = parseSize(minTopHeight, containerHeight);
      maxHeightPx.current = parseSize(maxTopHeight, containerHeight);
    }
  }, [containerHeight, initialTopHeight, minTopHeight, maxTopHeight]);

  // when slider (separator) is grabed
  const handleMouseDown = () => {
    isDragging.current = true;
  };
  // when slider (separator) is released
  const handleMouseUp = () => {
    isDragging.current = false;
  };
  // when slider (separator) is dragged
  const handleMouseMove = (e) => {
    if (!isDragging.current) return;
    const containerTop = containerRef.current.getBoundingClientRect().top;
    const newHeight = e.clientY - containerTop;
    if (newHeight < minHeightPx.current) {
      setTopHeightPx(minHeightPx.current);
    } else if (newHeight > maxHeightPx.current) {
      setTopHeightPx(maxHeightPx.current);
    } else {
      setTopHeightPx(newHeight);
    }
  };
  // Attaching global listeners to call the above event functions (grabing,releasing,dragginf)
  useEffect(() => {
    const handleMouseUpGlobal = () => handleMouseUp();
    const handleMouseMoveGlobal = (e) => handleMouseMove(e);
    document.addEventListener('mouseup', handleMouseUpGlobal);
    document.addEventListener('mousemove', handleMouseMoveGlobal);
    return () => {
      document.removeEventListener('mouseup', handleMouseUpGlobal);
      document.removeEventListener('mousemove', handleMouseMoveGlobal);
    };
  }, []);
  // End result
  return (
    <div ref={containerRef} className="reference_container">
      {/* Top Panel */}
      <div className="top_panel" style={{height: topHeightPx}}>
        <div className="ppt_tabs_container">
          <input id="filters_tab_radio" name="ppt_tab_radio" type="radio"/>
          <label htmlFor="filters_tab_radio" className="ppt_tabs" title="Graph filters"><i><Icons id="properties_container" type="filter" condition="True"/></i><span>Filters</span></label>
          <input id="infos_tab_radio" name="ppt_tab_radio" type="radio"/>
          <label htmlFor="infos_tab_radio" className="ppt_tabs" title="Nodes informations"><i><Icons id="properties_container" type="info" condition="True"/></i><span>Informations</span></label>
        </div>
        <div className="ppt_tabs_body_container">
          <div className="graph_filters_container">
            <form className="filter_form">
              <label>Search <i><b>Note :</b> Make sure an attribute name is selected.</i></label>
              <div className="filter_form_search_container">
                <input type="text" placeholder="Type here to seach"/>
                <button title="Search"><Icons id="properties_container" type="search" condition="True"/></button>
              <div>
                  <span><input id="attribute_checkbox1" type="checkbox" name="check1"/><label htmlFor="attribute_checkbox1">Id</label></span>
                  <span><input id="attribute_checkbox2" type="checkbox" name="check2"/><label htmlFor="attribute_checkbox2">Name</label></span>
                  <span><input id="attribute_checkbox3" type="checkbox" name="check3"/><label htmlFor="attribute_checkbox3">Date of Birth</label></span>
                  <span><input id="attribute_checkbox4" type="checkbox" name="check4"/><label htmlFor="attribute_checkbox4">City</label></span>
                  <span><input id="attribute_checkbox5" type="checkbox" name="check5"/><label htmlFor="attribute_checkbox5">Account number</label></span>
                  <span><input id="attribute_checkbox6" type="checkbox" name="check6"/><label htmlFor="attribute_checkbox6">NameLabel</label></span>
                  <span><input id="attribute_checkbox7" type="checkbox" name="check7"/><label htmlFor="attribute_checkbox7">Id</label></span>
                  <span><input id="attribute_checkbox8" type="checkbox" name="check8"/><label htmlFor="attribute_checkbox8">Name</label></span>
                </div>
              </div>
            </form>
          </div>
        </div>
      </div>
      {/* Drag Separator */}
      <div className="panel_separator" onMouseDown={handleMouseDown}>
        <span>...</span>
      </div>
      {/* Bottom Panel */}
      <div className="bottom_panel">
        <label className="bottom_panel_title">Graph Relationships</label>
        <ul>
          <li>
            <input id="allrelationships" name="relationship"  type="radio"/>
            <label htmlFor="allrelationships">*</label>
          </li>
          {graphRelationships && graphRelationships.map((rel) => (
            <li key={rel.type}>
              <input
                id={rel.type}
                name="relationship"
                type="radio"
                // checked={/* yo ur logic */}
                // onChange={/* your handler */}
              />
              <label
                htmlFor={rel.type}
                style={{ backgroundColor: rel.bgcolor,color: rel.color }} // Use the color property
              >
                {rel.type}
              </label>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
function IframeEmbed({id}) {
  const baseUrl = import.meta.env.VITE_IFRAME_URL || "http://localhost:5173";
    if (id === "source_placeholder"){
      return (
        <div style={{ height: '96%', width: '101%' }}>
          <iframe
            src={`${baseUrl}/src/source_discription.html`}
            width="100%"
            height="98%"
            style={{ border: 'none' }}
            title="Network Graph"
          />
        </div>
      );
    }
    if (id == "graph"){
      return (
        <div style={{ height: '96%', width: '101%' }}>
          <iframe
            src={`${baseUrl}/src/temp_graph.html`}
            width="100%"
            height="98%"
            style={{ border: 'none' }}
            title="Network Graph"
          />
        </div>
      );
    }
}
function DraggableWindow({ children, initialPos = { top: 0, left: 0, bottom: 0, right:0 }, onDragStart, zIndex }) {
  const [pos, setPos] = useState(initialPos);
  const windowRef = useRef(null);
  const dragging = useRef(false);
  const offset = useRef({ x: 0, y: 0 });

  const onMouseDown = (e) => {
    dragging.current = true;
    // Add 'dragging' class to window element
    if (windowRef.current) {
      windowRef.current.classList.add('dragging');
    }
    offset.current = {
      x: e.clientX - pos.left,
      y: e.clientY - pos.top,
    };
    document.body.style.userSelect = 'none';
    if (onDragStart) onDragStart(true);
  };

  const onMouseMove = (e) => {
    if (!dragging.current) return;
    setPos({
      left: e.clientX - offset.current.x,
      top: e.clientY - offset.current.y,
    });
  };

  const onMouseUp = () => {
    dragging.current = false;
    // Remove 'dragging' class
    if (windowRef.current) {
      windowRef.current.classList.remove('dragging');
    }
    document.body.style.userSelect = 'auto';
    if (onDragStart) onDragStart(false);
  };

  useEffect(() => {
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
      document.body.style.userSelect = 'auto';
    };
  }, []);
 // useEffect(() => {
 //    console.log('Current zIndex:', zIndex);
 //  }, [zIndex]);
  return (
    <div
      ref={windowRef}
      className="window"
      style={{ top: pos.top, left: pos.left, position: 'absolute', userSelect: 'none', zIndex: zIndex }}
    >
    {children({ onBarMouseDown: onMouseDown })}
    </div>
  );
}
function Windows({ id, type, sessionId, loadscreenText, loadscreenState, isSideBarMenuOpen, action, selectedContent, selectedSubContent, windowResponseI,windowResponseII,formToolResponse,batchFilesSearchResults,searchResultsVisible,searchPlaceholder,batchFilesCollection, batchFilesDataframeInfoI, batchFilesDataframeInfoII, batchFilesDataframeActionValue, batchFilesDataframeSourceValue, batchFilesDataframeTargetValue, batchFilesDataframeRelationshipValue, batchFilesDataframeRuleValue, sourceSessionLog, sourceStreamState ,fileInputRef, textareaRef, onClose, onMove, zIndex, onFocus, graphLink, graphLinkId, graphRelationships }) {
  if (type === "source") {
    return (
      <DraggableWindow initialPos={{ top: 0, left: 0}} zIndex={zIndex}>
        {(dragProps) => (
          <div id={`window_${type}_${id}`} style={{ zIndex }} className="window" onMouseDown={() => onFocus && onFocus()}>
            <div id={`window_cover_${type}_${id}`} className="window_cover" />
            <div id={`window_loadscreen_${type}_${id}`} className="windows_loadscreen" 
              style={{ display: loadscreenState ? 'block' : 'none' }}>
              <Loadscreen loadingText={loadscreenText} />
            </div>   
            <div id={`window_bar_${type}_${id}`} className="window_bar"
              onMouseDown={dragProps.onBarMouseDown}
              style={{ cursor: 'grab', userSelect: 'none' }}>
              <div className="window_bar_title_container">Source Window<input placeholder="Add custom title" type="text"/></div>
              <div className="window_bar_btns_container">
                <span onClick={() => onClose(id)}>x</span>
                <span>-</span>
              </div>
            </div>
            <div id={`window_side_bar_${type}_${id}`} className='side_bar'>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'new_source_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'new_source_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","new_source_options","")}>
                  <Icons id="window_side_bar" type="new" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'new_source_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action(id,"live_source_options","update")}>      
                        <span>Live source</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                      <li onClick={() => action(id,"upload_source_options","update")}>      
                        <span>File Upload</span>                        
                        {/*<i>
                          <Icons id="window_side_bar" type="upload" condition="True"/>
                        </i>*/}             
                      </li>
                      <li onClick={() => action(id,"load_source_options","update")}>      
                        <span>Load session</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="load_session" condition="True"/>
                        </i>*/}
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
            </div>
            <div id={`window_content_${type}_${id}`}  className='content_container'>
                {selectedContent === "live_source_options" && (
                  <div className="live_source_options_container">
                    <div className="" style={{fontSize:'2.5vh',borderBottom:'1px solid #ccc', padding:'2vh',textAlign:'left',paddingLeft:'0vw',margin:'1.5vw'}}>Pick a source input</div>
                    <div className="live_source_option">
                      <span className="live_source_option_icon">
                        <Icons id="window_live_source_option" type="realTime_input" condition="True"/> 
                      </span>
                      <span className="live_source_option_details">
                        <label>Real-time input</label>
                        <p>Connect to a Broker/API and consume data as a Real-time messages.</p>
                      </span>
                    </div>
                    <div className="live_source_option" onClick={() => action(id,"batch_input","update")}>
                      <span className="live_source_option_icon">
                        <Icons id="window_live_source_option" type="batch_input" condition="True"/> 
                      </span>
                      <span className="live_source_option_details">
                        <label>Batch input</label>
                        <p>Connect to a Broker/API and fetch for datas as a batch query.</p>
                      </span>
                    </div>
                  </div>
                )}
                {selectedContent === "upload_source_options" && (
                  <div className="upload_source_options_container">
                    <div className="upload_source_options_header">Upload a dataset</div>
                    <div className="upload_source_option">
                      <span className="upload_source_option_icon">
                        <Icons id="window_upload_source_option" type="upload_input" condition="True"/> 
                      </span>
                      <div className="upload_source_option_details">
                        <label>Upload files or Drag and drop here.</label>
                        <p>
                          <i><b>Note: </b>Only csv, parquet, json and xlsx file types are allowed.</i>
                        </p>
                      </div>
                      <input id="upload_source_option_input" multiple accept=".csv,.json,.parquet,.xlsx" type="file" ref={fileInputRef} onChange={(e) => action(id, "upload_source_files", "upload", { files: e.target.files })}/>
                      <button onClick={() => fileInputRef.current.click()}>Choose files</button>
                    </div>
                  </div>
                )}
                {selectedContent === null && (
                  <div className="placeholder">
                    <IframeEmbed id="source_placeholder"/>
                  </div>
                )}
                {selectedContent === "batch_input" && (
                  <div className="live_source_options_container">
                    <div className="" style={{fontSize:'2.5vh',borderBottom:'1px solid #ccc', padding:'2vh',textAlign:'left',paddingLeft:'0vw',margin:'1.5vw',marginBottom:0}}>Pick a source input</div>
                    <div className="live_source_option_passive" style={{position:'absolute',top:'calc( 3vh - 3vw)',left:'0vw'}}>
                      <span className="live_source_option_icon">
                        <Icons id="window_live_source_option" type="batch_input" condition="True"/> 
                      </span>
                      <span className="live_source_option_details">
                        <label>Batch input</label>
                        <p>Connect to a Broker/API or upload datasets and fetch for data in batch.</p>
                      </span>
                    </div>
                    <div className="batch_connection_form_container">
                      {selectedSubContent === "batch_input_form_pageI" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : windowResponseI === "Dataset uploaded!" ? '#090' : '',
                              backgroundColor: windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : windowResponseI === "Dataset uploaded!" ? 'rgba(0, 255, 0, 0.2)' :''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" :
                                  windowResponseI === "Dataset uploaded!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form onSubmit={(e) => { e.preventDefault(); 
                            if (windowResponseI === "Connection established!") {
                              // Disconnect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              action(id, "batch_input_form", "disconnect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                                session_id:sessionId                                
                              });
                            }
                            else if (windowResponseI === "Disconnecting failed!") {
                              // Disconnect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              action(id, "batch_input_form", "disconnect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                                session_id:sessionId,
                              });
                            }
                            else {
                              // Connect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              console.log("session id2",sessionId)
                              action(id, "batch_input_form", "connect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                                session_id:sessionId                                
                              });
                            }
                            }}
                            style={{
                              pointerEvents: windowResponseI === "Dataset uploaded!" ? 'none' : 'auto',
                              opacity: windowResponseI === "Dataset uploaded!" ? 0.5 : 1 
                            }}>
                            <fieldset>
                              <legend><b>Broker/API</b> Connection</legend>
                              <div className="box_inputs_container">
                                <input id="broker_address_radio" className="radioinput" type="radio" name="source_input_address_type" defaultChecked />
                                <label htmlFor="broker_address_radio">Kafka Broker</label>
                                <input id="api_address_radio" className="radioinput" type="radio" name="source_input_address_type" disabled/>
                                <label htmlFor="api_address_radio">REST API</label>
                              </div>
                              <input id="source_input_address_text" placeholder="Enter Broker/API Address" className="textinput" type="text" required
                              disabled={
                                windowResponseI === "Connecting..." ? 'True' : 
                                windowResponseI === "Connection established!" ? 'True': ''
                              }
                              style={{
                                color: windowResponseI === "Connection established!" ? '#AAA' : '',
                                backgroundColor: windowResponseI === "Connection established!" ? '#EEE' : '',
                                borderColor: windowResponseI === "Connection established!" ? '#DDD' : ''
                              }} defaultValue="localhost:9092"/>
                            </fieldset>
                            <fieldset>
                              <legend><b>HDFS</b> Connection</legend>
                              <div className="box_inputs_container">
                                <input id="hadoop_address_radio" className="radioinput" type="radio" name="source_storage_address_type" defaultChecked />
                                <label htmlFor="hadoop_address_radio">Hadoop Cluster</label>
                              </div>
                              <input id="source_storage_address_text" placeholder="Enter HDFS Address" className="textinput" type="text" required
                              disabled={
                                windowResponseI === "Connecting..." ? 'True' : 
                                windowResponseI === "Connection established!" ? 'True': ''
                              }
                              style={{
                                color: windowResponseI === "Connection established!" ? '#AAA' : '',
                                backgroundColor: windowResponseI === "Connection established!" ? '#EEE' : '',
                                borderColor: windowResponseI === "Connection established!" ? '#DDD' : ''
                              }} defaultValue="localhost:9870"/>
                            </fieldset>                        
                            <button type="submit"><span><Icons id="window_live_source_option" type="connect" condition="True"/></span>
                              <span>
                                {
                                  windowResponseI === null
                                    ? "Connect":
                                  windowResponseI === "Not connected."
                                    ? "Connect":
                                  windowResponseI === "Disconnecting..."
                                    ? "Disconnecting...":
                                  windowResponseI === "Disconnected!"
                                    ? "Connect":
                                  windowResponseI === "Connecting..."
                                    ? "Connecting...": 
                                  windowResponseI === "Connection established!"
                                    ? "Disconnect":
                                  windowResponseI === "Connection failed!"
                                    ? "Connect":
                                  windowResponseI === "Connection failed! No storage found."
                                    ? "Connect":
                                  windowResponseI === "Disconnecting failed!"
                                    ? "Retry":
                                  windowResponseI === "Dataset uploaded!"
                                    ? "Connect":""
                                }
                              </span>
                             </button>
                          </form>
                          <form onSubmit={(e) => { e.preventDefault();
                            if (windowResponseI === "Connection established!" && formToolResponse !== "Connected!" || windowResponseI === "Dataset uploaded!" && formToolResponse !== "Connected!" ) {
                              // Disconnect logic
                              const toolUrl = document.getElementById("tool_url").value;
                              const toolUsername = document.getElementById("tool_username").value;
                              const toolPassword = document.getElementById("tool_password").value;
                              const toolDatabase = document.getElementById("tool_database").value;
                              action(id, "tool_integration_form", "connect", {
                                tool_name: 'neo4j',
                                url: toolUrl,
                                username: toolUsername,
                                password: toolPassword,
                                database: toolDatabase,
                                session_id:sessionId                                                                
                              });
                            }
                            else{
                              const toolUrl = document.getElementById("tool_url").value;
                              const toolUsername = document.getElementById("tool_username").value;
                              const toolPassword = document.getElementById("tool_password").value;
                              const toolDatabase = document.getElementById("tool_database").value;
                              action(id, "tool_integration_form", "disconnect", {
                                tool_name: 'neo4j',
                                url: toolUrl,
                                username: toolUsername,
                                password: toolPassword,
                                database: toolDatabase,
                                session_id:sessionId                                                                
                              });
                            }
                            }}>
                            <fieldset>
                              <legend><b>Tool/Database</b> Integration</legend>
                              <div className="box_inputs_container">
                                <input id="tool_neo4j_radio" className="radioinput" type="radio" name="analysis_tool_type" defaultChecked />
                                <label htmlFor="tool_neo4j_radio">Neo4j</label>
                              </div>
                              <input id="tool_url" placeholder="Url" className="textinput" type="text"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_username" placeholder="Username" className="textinput" type="text"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_password" placeholder="Password" className="textinput" type="password"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_database" placeholder="Database name" className="textinput" type="text" disabled/>
                              <div className="tool_form_response">
                                <Icons
                                  id="window_live_source_option"
                                  type={
                                    formToolResponse === null ? "warningx" :
                                    formToolResponse === "Not connected!" ? "warningx" :
                                    formToolResponse === "Connecting..." ? "loadingx" :
                                    formToolResponse === "Connected!" ? "correctx" : "errorx"
                                  }
                                  condition="True"
                                />
                                <span 
                                  style={{
                                  color: 
                                    formToolResponse === null ? '#CA4' :
                                    formToolResponse === "Not connected!" ? '#CA4' :
                                    formToolResponse === "Connected!" ? '#0B0' :
                                    '' // Default color
                                  }}>
                                  {
                                    formToolResponse === null
                                      ? "Not connected!":
                                    formToolResponse === "Not connected!"
                                      ? "Not connected!":
                                    formToolResponse === "Disconnecting..."
                                      ? "Disconnecting...":
                                    formToolResponse === "Disconnected!"
                                      ? "Disconnected!":
                                    formToolResponse === "Connecting..."
                                      ? "Connecting...": 
                                    formToolResponse === "Connected!"
                                      ? "Connected!":
                                    formToolResponse === "Connection failed!"
                                      ? "Connection failed!":
                                    formToolResponse === "Disconnecting failed!"
                                      ? "Disconnecting failed!"
                                      : ""
                                  }
                                </span>
                              </div>
                              <button 
                                disabled={
                                windowResponseI === "Connection established!" ? '':
                                windowResponseI !== "Connection established!" && formToolResponse === "Connected!" ? '':
                                windowResponseI === "Dataset uploaded!" ? '': 'False'
                              }>
                                {
                                  formToolResponse === null ? "Connect":
                                  formToolResponse === "Not connected!" ? "Connect":
                                  formToolResponse === "Connected!" ? "Disconnect" :
                                  formToolResponse === "Disconnected!" ? "Connect" :
                                  formToolResponse === "Connecting..." ? "Connecting..." : "Disconnecting..."                           
                                }
                              </button>
                            </fieldset>
                          </form>
                        </div>
                      )}
                      {selectedSubContent === "batch_input_form_pageII" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : '',
                              backgroundColor: windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form className="batch_files_search_form" onSubmit={(e) => { e.preventDefault()}}>
                            <fieldset className="batch_files_search_form_fieldset">
                              <legend><b>Files</b> on storage</legend>
                              <div className="batch_files_search_form" onSubmit={(e) => { e.preventDefault()}}>
                                <div id="batch_files_search_container" className="batch_files_search_container">
                                  <input id="batch_files_search_input" type="text" placeholder="Type here to seach" onChange={() => action(id,"batch_files_search_input","search",document.getElementById("batch_files_search_input").value)}/>
                                  <button id="batch_files_search_button" title="Search" >
                                   <Icons id="window_live_source_option" type="search" condition="True"/>
                                  </button>
                                  <button title="Search"><Icons id="window_live_source_option" type="inbox-files" condition="True"/></button>
                                </div>
                                <div id="batch_files_search_result_container"
                                    className="batch_files_search_result_container"
                                    style={{
                                      '--searching-text': `'${searchPlaceholder}'`,
                                      display: searchResultsVisible && batchFilesSearchResults? "block" : "none",
                                    }}
                                  >
                                  <ul>
                                    {batchFilesSearchResults && batchFilesSearchResults.length > 0 ? (
                                      batchFilesSearchResults.map((file, index) => (
                                        <li key={file.name} 
                                          style={{
                                            backgroundColor: batchFilesCollection.some((selectedFile) => selectedFile.name === file.name) ? '#EEE':'',
                                            color: batchFilesCollection.some((selectedFile) => selectedFile.name === file.name) ? '#999':''
                                          }} 
                                          onClick={(e) => {
                                            var payload={"name":file.name,"date":file.date,"size":file.size}
                                            action(id, "batch_files_select_file", "toggle_select",payload);}}>
                                          <span>{file.name}</span>
                                          <span>{file.size} Kb</span>
                                        </li>
                                      ))
                                    ) : (
                                      ''
                                    )}
                                  </ul>
                                </div>
                              </div>
                            </fieldset>
                            <fieldset className="batch_files_table_form_fieldset">
                              <legend><b>Selected Files</b></legend>
                              <table>
                                <thead>
                                  <tr>
                                    <th>ID</th>
                                    <th>File name</th>
                                    <th>Date</th>
                                    <th>Size</th>
                                    <th></th>
                                  </tr>
                                </thead>
                                <tbody>
                                    {batchFilesCollection.length > 0  ? (
                                      batchFilesCollection.map((file, index) => (
                                        <tr key={index}>
                                          <td>{index + 1}</td>
                                          <td>{file.name}</td>
                                          <td>{file.date}</td>
                                          <td>{file.size} Kb</td>
                                          <td onClick={(e) => {
                                            e.stopPropagation();
                                            var payload={"name":file.name,"date":file.date,"size":file.size}
                                            action(id, "batch_files_select_file", "toggle_select",payload);}}>
                                            <Icons
                                              id="window_live_source_option"
                                              type="minus"
                                              condition="True"
                                              onClick={() => handleRemoveFile(index)} // Logic to remove the file
                                            />
                                          </td>
                                        </tr>
                                      ))
                                    ) : (
                                    <tr>
                                        <td colSpan="5" style={{ textAlign: "center" }}>
                                          No files selected
                                        </td>
                                      </tr>
                                    )}
                                </tbody>
                              </table>
                            </fieldset>
                          </form>
                        </div>
                      )}
                      {selectedSubContent === "batch_input_form_pageIII" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : windowResponseI === "Dataset uploaded!" ? '#090' : '',
                              backgroundColor: windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : windowResponseI === "Dataset uploaded!" ? 'rgba(0, 255, 0, 0.2)' :''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" :
                                  windowResponseI === "Dataset uploaded!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form className="batch_files_dataframe_form" onSubmit={(e) => { e.preventDefault()}}>
                            <fieldset className="batch_files_dataframe_form_fieldset">
                              <legend><b>Dataframe</b> Infomation</legend>
                              <div className="batch_files_dataframe_infos">
                               <div id="batch_files_dataframe_infos_left_container" className="dataframe_infos_left_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_left_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Source files</td>
                                        <td>
                                          <ul>
                                            {batchFilesCollection.map((file, index) => {
                                              return <li key={index}>{file.name || JSON.stringify(file)}</li>;
                                            })}
                                          </ul>
                                        </td>
                                      </tr>
                                      <tr>
                                        <td>Total rows</td>
                                        <td>{batchFilesDataframeInfoI[4]}</td>
                                      </tr>
                                      <tr>
                                        <td>Total columns</td>
                                        <td>{batchFilesDataframeInfoI[3]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                               <div id="batch_files_dataframe_infos_right_container" className="dataframe_infos_right_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_right_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Broker/API</td>
                                        <td>{batchFilesDataframeInfoI[1]}</td>
                                      </tr>
                                      <tr>
                                        <td>Storage</td>
                                        <td>{batchFilesDataframeInfoI[6]}</td>
                                      </tr>
                                      <tr>
                                        <td>Tool</td>
                                        <td>{batchFilesDataframeInfoI[7]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                              </div>
                            </fieldset>
                            <fieldset className="batch_files_dataframe_filter_and_actions">
                              <legend><b> Filters & Actions </b></legend>
                              <div className="batch_files_dataframe_filter_and_actions_container">
                                <div className="batch_files_dataframe_action_inputs">
                                  <label className="actions_label"><i><b>Note :</b> This fields are mandatory.</i></label>
                                  <div className="actions_partition">
                                    <label>Dataframe Action</label>
                                    <select id="batch_files_dataframe_action_select" value={batchFilesDataframeActionValue ? batchFilesDataframeActionValue:''} disabled={batchFilesDataframeInfoI[0] ? false:true} onChange={(e) => action(id,"batch_files_actions_select","change",e.target.value)} className="actions_select_options">
                                      <option value="" disabled>Select Action</option>
                                      {batchFilesDataframeInfoI && batchFilesDataframeInfoI[0] ? (
                                        batchFilesDataframeInfoI[0].map((item, index) => (
                                            <option key={index} value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No actions</option>
                                      )}
                                    </select>
                                  </div>
                                  <div className="actions_partition">
                                    <label>Source/Target</label>                                    
                                    <select id="batch_files_dataframe_source_select" value={batchFilesDataframeSourceValue ? batchFilesDataframeSourceValue:''} disabled={batchFilesDataframeInfoI[2]  && batchFilesDataframeActionValue === "Source / Target Relationship" ? false:true} onChange={(e) => action(id,"batch_files_source_select","change",e.target.value)} style={{float:'left',position:'relative',width:'32.5%',borderRight:'0.1vh dashed #EEE',marginRight:'0.1vw'}}>
                                      <option value="" disabled>Select Source</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index} value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                    <select id="batch_files_dataframe_target_select" value={batchFilesDataframeTargetValue ? batchFilesDataframeTargetValue:''} disabled={batchFilesDataframeInfoI[2] && batchFilesDataframeActionValue === "Source / Target Relationship" ? false:true} onChange={(e) => action(id,"batch_files_target_select","change",e.target.value)} style={{float:'left',position:'relative',width:'32%',borderRight:'0.1vh dashed #EEE',marginRight:'0.1vw'}}>
                                      <option value="" disabled>Select Target</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index} value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                  </div>
                                  <div className="actions_partition">
                                    <label>Relationship label</label>                                    
                                    <input placeholder="HAS_RELATIONSHIP" value={batchFilesDataframeRelationshipValue ? batchFilesDataframeRelationshipValue:''} disabled={batchFilesDataframeActionValue === "Source / Target Relationship" && batchFilesDataframeSourceValue && batchFilesDataframeTargetValue ? false:true} onChange={(e) => action(id,"batch_files_relationship_select","change",e.target.value)}
                                     type='text'/>
                                  </div>
                                  <div className="actions_partition">
                                    <label>Rule to apply</label>                                    
                                    <select id="batch_files_dataframe_rule_select" value={batchFilesDataframeRuleValue ? batchFilesDataframeRuleValue:''} disabled={batchFilesDataframeInfoI[5] && batchFilesDataframeActionValue === "Link Analysis" ? false:true} onChange={(e) => action(id,"batch_files_rule_select","change",e.target.value)}>
                                      <option value="" disabled>Select Analysis rule</option>
                                      {batchFilesDataframeInfoI[5] ? (
                                        batchFilesDataframeInfoI[5].map((item, index) => (
                                            <option key={index} value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No Rules</option>
                                      )}
                                    </select>
                                  </div>                                                                                                      
                                </div>
                                <div className="batch_files_dataframe_filter_inputs">
                                  <label className="filters_label"><i><b>Note :</b> Changes require applying inorder to take effect.</i></label>
                                  <div className="filters_partition">
                                    <select id="batch_files_dataframe_filter_selectI" disabled={batchFilesDataframeInfoI[2] ? false:true} onChange={(e) => action(id,"batch_files_target_select","change",e.target.value)} defaultValue="">
                                      <option value="" disabled>Select column</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index} value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                    <input type='text'/>
                                  </div>
                                  <div className="filters_partition">
                                    <select id="batch_files_dataframe_filter_selectI" disabled={batchFilesDataframeInfoI[2] ? false:true} onChange={(e) => action(id,"batch_files_target_select","change",e.target.value)} defaultValue="">
                                      <option value="" disabled>Select column</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index} value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                    <input type='text'/>
                                  </div>
                                  <div className="filters_query_partition">
                                    <label>Query area</label>
                                    <textarea placeholder="Type a query to filter with."></textarea>
                                  </div>
                                </div>
                              </div>
                              <div className="batch_files_dataframe_filter_menu">
                                <span className="batch_files_dataframe_filter_menu_rows">{batchFilesDataframeInfoI[4]} Data rows</span>
                                <span className="batch_files_dataframe_filter_menu_add_btn">
                                  <button>Apply filter</button>
                                </span>
                              </div>
                            </fieldset>
                          </form>
                        </div>
                      )}
                      {selectedSubContent === "batch_input_form_pageIV" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Session running..." ? '#009' : windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : '',
                              backgroundColor: windowResponseI === "Session running..." ? 'rgba(0, 0, 255, 0.2)' : windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Session running..." ? "streamx" :
                                  windowResponseI === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form className="batch_files_dataframe_form" onSubmit={(e) => { e.preventDefault()}}>
                            <fieldset className="batch_files_dataframe_form_fieldset">
                              <legend><b>Dataframe</b> Infomation</legend>
                              <div className="batch_files_dataframe_infos">
                               <div id="batch_files_dataframe_infos_left_container" className="dataframe_infos_left_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_left_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Source files</td>
                                        <td>
                                          <ul>
                                            {batchFilesCollection.map((file, index) => {
                                              return <li key={index}>{file.name || JSON.stringify(file)}</li>;
                                            })}
                                          </ul>
                                        </td>
                                      </tr>
                                      <tr>
                                        <td>Total rows</td>
                                        <td>{batchFilesDataframeInfoI[4]}</td>
                                      </tr>
                                      <tr>
                                        <td>Total columns</td>
                                        <td>{batchFilesDataframeInfoI[3]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                               <div id="batch_files_dataframe_infos_right_container" className="dataframe_infos_right_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_right_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Broker/API</td>
                                        <td>{batchFilesDataframeInfoI[1]}</td>
                                      </tr>
                                      <tr>
                                        <td>Storage</td>
                                        <td>{batchFilesDataframeInfoI[6]}</td>
                                      </tr>
                                      <tr>
                                        <td>Tool</td>
                                        <td>{batchFilesDataframeInfoI[7]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                              </div>
                            </fieldset>
                            <fieldset className="batch_files_dataframe_form_fieldset">
                              <legend><b> Session log </b></legend>
                              <textarea ref={textareaRef} className="batch_files_dataframe_filter_log_textarea" readOnly value={sourceSessionLog !== null ? sourceSessionLog:''}></textarea>
                            </fieldset>
                          </form>
                        </div>
                      )}
                    </div>
                    <div className="batch_connection_form_pager_container">
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageII"){
                            action(id, "batch_input_form_swap_passive", "page_I",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII" && windowResponseI === "Connection established!"){
                            action(id, "batch_input_form_swap_passive", "page_II",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII" && windowResponseI === "Dataset uploaded!"){
                            action(id, "batch_input_form_swap_passive", "page_I",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV"){
                            action(id, "batch_input_form_swap_passive", "page_III",null);
                          }
                          else{
                            return null;
                          }
                        }}
                        disabled={
                          selectedSubContent !== "batch_input_form_pageI" && selectedSubContent !== "batch_input_form_pageIV" ||
                          selectedSubContent === "batch_input_form_pageIV" && !sourceStreamState ? '': 'True' 
                        }>
                        {"Back"}    
                      </button>
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Connection established!"){
                            action(id, "batch_input_form_swap", "page_II",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Dataset uploaded!"){
                            action(id, "batch_input_form_swap", "page_III",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageII"){
                            action(id, "batch_input_form_swap", "page_III",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII"){
                            action(id, "batch_input_form_swap", "page_IV",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV" && sourceStreamState){
                            action(id, "batch_input_stream_terminate", "page_IV",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV" && !sourceStreamState){
                            action(id, "batch_input_form_swap", "page_IV",null);
                          }
                          else{
                            console.log("NoPage to swap")
                            return null;
                          }
                        }}
                        disabled={
                          selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Connection established!" ||
                          selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Dataset uploaded!" && formToolResponse === "Connected!" ||
                          selectedSubContent === "batch_input_form_pageII" && windowResponseI === "Dataset uploaded!" && formToolResponse === "Connected!" ||
                          selectedSubContent === "batch_input_form_pageII" && batchFilesCollection.length> 0  || 
                          selectedSubContent !== "batch_input_form_pageIV" && batchFilesDataframeActionValue === "Store data" || 
                          selectedSubContent !== "batch_input_form_pageIV" && batchFilesDataframeActionValue === "Source / Target Relationship" && batchFilesDataframeSourceValue && batchFilesDataframeTargetValue ||
                          selectedSubContent !== "batch_input_form_pageIV" && batchFilesDataframeActionValue === "Link Analysis" && batchFilesDataframeRuleValue || 
                          selectedSubContent === "batch_input_form_pageIV" && sourceStreamState? '': 'True' 
                        }>
                        {selectedSubContent === "batch_input_form_pageIII" || selectedSubContent === "batch_input_form_pageIV" && !sourceStreamState ? "Stream Graph":
                        selectedSubContent === "batch_input_form_pageIV" ? "Terminate"  : "Next"}
                      </button>
                    </div>
                  </div>
                )}
            </div>
            <div id={`window_properties_${type}_${id}`}  className="properties_container">
              {selectedContent === "null1" && (
                <div className="live_source_options_properties">
                  prop
                </div>
              )}
              {selectedContent === "null2" && (
                <div className="placeholder">
                  <IframeEmbed id="source_placeholder"/>
                </div>
              )}
            </div>
            <div id={`window_footer_${type}_${id}`}  className='window_footer'>
              <span>
                <b>window Id : </b>
                <i>{id}</i>
              </span>
            </div>
          </div>
        )}
      </DraggableWindow>
    )
  }
  if (type === "graph"){ //Graph window
    return (
      <DraggableWindow initialPos={{ top: 0, left: 0}} zIndex={zIndex}>
        {(dragProps) => (
          <div id={`window_${type}_${id}`} style={{ zIndex }} className="window" onMouseDown={() => onFocus && onFocus()}>
            <div id={`window_cover_${type}_${id}`} className="window_cover" />
            <div id={`window_loadscreen_${type}_${id}`} className="windows_loadscreen" 
              style={{ display: loadscreenState ? 'block' : 'none' }}>
              <Loadscreen loadingText={loadscreenText} />
            </div>   
            <div id={`window_bar_${type}_${id}`} className="window_bar"
              onMouseDown={dragProps.onBarMouseDown}
              style={{ cursor: 'grab', userSelect: 'none' }}>
              <div className="window_bar_title_container">Graph Window<input placeholder="Add custom title" type="text"/></div>
              <div className="window_bar_btns_container">
                <span onClick={() => onClose(id)}>x</span>
                <span>-</span>
              </div>
            </div>
            <div id={`window_side_bar_${type}_${id}`} className='side_bar'>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'link_graph_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'link_graph_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","link_graph_options","")}>
                  <Icons id="window_side_bar" type="link" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'link_graph_options' ? 'block' : 'none'}}>
                  <ul>
                      <li>      
                        <span>
                          <form onSubmit={(e) => { e.preventDefault(); 
                            if (graphLink == true) {
                              // unlinking logic
                              action(id, "graph_link_form", "unlink", {
                                sourceId: graphLinkId,
                              });
                            }
                            else{
                              const newGraphLinkId = document.getElementById(`graph_link_id_input_${id}`).value;
                              action(id, "graph_link_form", "link", {
                                sourceId: newGraphLinkId,
                              });
                            }
                          }}>
                            <input id={`graph_link_id_input_${id}`} type="text" placeholder={graphLink ? graphLinkId : 'Enter window ID'}
                              disabled={
                                graphLink ? 'True' : '' 
                              }
                            />
                            <button>
                              {graphLink ? 'Unlink' : 'Link'}
                            </button>
                          </form>
                        </span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'save_graph_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'save_graph_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","save_graph_options","")}>
                  <Icons id="window_side_bar" type="save" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'save_graph_options' ? 'block' : 'none'}}>
                  <ul>
                      <li onClick={() => action("save_graph_options","update")}>      
                        <span>Save Graph</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'snap_graph_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'snap_graph_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","snap_graph_options","")}>
                  <Icons id="window_side_bar" type="capture" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'snap_graph_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("snap_graph_options","update")}>      
                        <span>Take a snap</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'print_graph_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'print_graph_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","print_graph_options","")}>
                  <Icons id="window_side_bar" type="print" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'print_graph_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("print_graph_options","update")}>      
                        <span>Print Graph</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'expand_graph_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'expand_graph_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","expand_graph_options","")}>
                  <Icons id="window_side_bar" type="expand" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'expand_graph_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("expand_graph_options","update")}>      
                        <span>Expand view</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'reset_graph_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'reset_graph_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","reset_graph_options","")}>
                  <Icons id="window_side_bar" type="reset" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'reset_graph_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("reset_graph_options","update")}>      
                        <span>Reset Graph</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'export_graph_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'export_graph_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action("side_bar_menu_list","export_graph_options","")}>
                  <Icons id="window_side_bar" type="export" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'export_graph_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("export_graph_options","update")}>      
                        <span>Export JSON</span>
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i>*/}
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
            </div>
            <div id={`window_content_${type}_${id}`}  className='content_container'>
                {selectedContent === "graph_content" && (
                  <IframeEmbed id="graph"/>
                )}
                {selectedContent === "upload_source_options" && (
                  <div className="upload_source_options_container">
                    <div className="upload_source_options_header">Upload a dataset</div>
                    <div className="upload_source_option">
                      <span className="upload_source_option_icon">
                        <Icons id="window_upload_source_option" type="upload_input" condition="True"/> 
                      </span>
                      <div className="upload_source_option_details">
                        <label>Upload files or Drag and drop here.</label>
                        <p>
                          <i><b>Note: </b>Only csv, parquet, json and xlsx file types are allowed.</i>
                        </p>
                      </div>
                      <input id="upload_source_option_input" multiple accept=".csv,.json,.parquet,.xlsx" type="file" ref={fileInputRef} onChange={(e) => action(id, "upload_source_files", "upload", { files: e.target.files })}/>
                      <button onClick={() => fileInputRef.current.click()}>Choose files</button>
                    </div>
                  </div>
                )}
                {selectedContent === null && (
                  <div className="placeholder">
                    <IframeEmbed id="source_placeholder"/>
                  </div>
                )}
                {selectedContent === "batch_input" && (
                  <div className="live_source_options_container">
                    <div className="" style={{fontSize:'2.5vh',borderBottom:'1px solid #ccc', padding:'2vh',textAlign:'left',paddingLeft:'0vw',margin:'1.5vw',marginBottom:0}}>Pick a source input</div>
                    <div className="live_source_option_passive" style={{position:'absolute',top:'calc( 3vh - 3vw)',left:'0vw'}}>
                      <span className="live_source_option_icon">
                        <Icons id="window_live_source_option" type="batch_input" condition="True"/> 
                      </span>
                      <span className="live_source_option_details">
                        <label>Batch input</label>
                        <p>Connect to a Broker/API or upload datasets and fetch for data in batch.</p>
                      </span>
                    </div>
                    <div className="batch_connection_form_container">
                      {selectedSubContent === "batch_input_form_pageI" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : windowResponseI === "Dataset uploaded!" ? '#090' : '',
                              backgroundColor: windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : windowResponseI === "Dataset uploaded!" ? 'rgba(0, 255, 0, 0.2)' :''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" :
                                  windowResponseI === "Dataset uploaded!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form onSubmit={(e) => { e.preventDefault(); 
                            if (windowResponseI === "Connection established!") {
                              // Disconnect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              action(id, "batch_input_form", "disconnect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                              });
                            }
                            else if (windowResponseI === "Disconnecting failed!") {
                              // Disconnect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              action(id, "batch_input_form", "disconnect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                              });
                            }
                            else {
                              // Connect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              action(id, "batch_input_form", "connect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                              });
                            }
                            }}
                            style={{
                              pointerEvents: windowResponseI === "Dataset uploaded!" ? 'none' : 'auto',
                              opacity: windowResponseI === "Dataset uploaded!" ? 0.5 : 1 
                            }}>
                            <fieldset>
                              <legend><b>Broker/API</b> Connection</legend>
                              <div className="box_inputs_container">
                                <input id="broker_address_radio" className="radioinput" type="radio" name="source_input_address_type" defaultChecked />
                                <label htmlFor="broker_address_radio">Kafka Broker</label>
                                <input id="api_address_radio" className="radioinput" type="radio" name="source_input_address_type" disabled/>
                                <label htmlFor="api_address_radio">REST API</label>
                              </div>
                              <input id="source_input_address_text" placeholder="Enter Broker/API Address" className="textinput" type="text" required
                              disabled={
                                windowResponseI === "Connecting..." ? 'True' : 
                                windowResponseI === "Connection established!" ? 'True': ''
                              }
                              style={{
                                color: windowResponseI === "Connection established!" ? '#AAA' : '',
                                backgroundColor: windowResponseI === "Connection established!" ? '#EEE' : '',
                                borderColor: windowResponseI === "Connection established!" ? '#DDD' : ''
                              }} defaultValue="localhost:9092"/>
                            </fieldset>
                            <fieldset>
                              <legend><b>HDFS</b> Connection</legend>
                              <div className="box_inputs_container">
                                <input id="hadoop_address_radio" className="radioinput" type="radio" name="source_storage_address_type" defaultChecked />
                                <label htmlFor="hadoop_address_radio">Hadoop Cluster</label>
                              </div>
                              <input id="source_storage_address_text" placeholder="Enter HDFS Address" className="textinput" type="text" required
                              disabled={
                                windowResponseI === "Connecting..." ? 'True' : 
                                windowResponseI === "Connection established!" ? 'True': ''
                              }
                              style={{
                                color: windowResponseI === "Connection established!" ? '#AAA' : '',
                                backgroundColor: windowResponseI === "Connection established!" ? '#EEE' : '',
                                borderColor: windowResponseI === "Connection established!" ? '#DDD' : ''
                              }} defaultValue="localhost:9870"/>
                            </fieldset>                        
                            <button type="submit"><span><Icons id="window_live_source_option" type="connect" condition="True"/></span>
                              <span>
                                {
                                  windowResponseI === null
                                    ? "Connect":
                                  windowResponseI === "Not connected."
                                    ? "Connect":
                                  windowResponseI === "Disconnecting..."
                                    ? "Disconnecting...":
                                  windowResponseI === "Disconnected!"
                                    ? "Connect":
                                  windowResponseI === "Connecting..."
                                    ? "Connecting...": 
                                  windowResponseI === "Connection established!"
                                    ? "Disconnect":
                                  windowResponseI === "Connection failed!"
                                    ? "Connect":
                                  windowResponseI === "Connection failed! No storage found."
                                    ? "Connect":
                                  windowResponseI === "Disconnecting failed!"
                                    ? "Retry":
                                  windowResponseI === "Dataset uploaded!"
                                    ? "Connect":""
                                }
                              </span>
                             </button>
                          </form>
                          <form onSubmit={(e) => { e.preventDefault();
                            if (windowResponseI === "Connection established!" && formToolResponse !== "Connected!" || windowResponseI === "Dataset uploaded!" && formToolResponse !== "Connected!" ) {
                              // Disconnect logic
                              const toolUrl = document.getElementById("tool_url").value;
                              const toolUsername = document.getElementById("tool_username").value;
                              const toolPassword = document.getElementById("tool_password").value;
                              const toolDatabase = document.getElementById("tool_database").value;
                              action(id, "tool_integration_form", "connect", {
                                tool_name: 'neo4j',
                                url: toolUrl,
                                username: toolUsername,
                                password: toolPassword,
                                database: toolDatabase
                              });
                            }
                            else{
                              const toolUrl = document.getElementById("tool_url").value;
                              const toolUsername = document.getElementById("tool_username").value;
                              const toolPassword = document.getElementById("tool_password").value;
                              const toolDatabase = document.getElementById("tool_database").value;
                              action(id, "tool_integration_form", "disconnect", {
                                tool_name: 'neo4j',
                                url: toolUrl,
                                username: toolUsername,
                                password: toolPassword,
                                database: toolDatabase
                              });
                            }
                            }}>
                            <fieldset>
                              <legend><b>Tool/Database</b> Integration</legend>
                              <div className="box_inputs_container">
                                <input id="tool_neo4j_radio" className="radioinput" type="radio" name="analysis_tool_type" defaultChecked />
                                <label htmlFor="tool_neo4j_radio">Neo4j</label>
                              </div>
                              <input id="tool_url" placeholder="Url" className="textinput" type="text"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_username" placeholder="Username" className="textinput" type="text"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_password" placeholder="Password" className="textinput" type="password"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_database" placeholder="Database name" className="textinput" type="text" disabled/>
                              <div className="tool_form_response">
                                <Icons
                                  id="window_live_source_option"
                                  type={
                                    formToolResponse === null ? "warningx" :
                                    formToolResponse === "Not connected!" ? "warningx" :
                                    formToolResponse === "Connecting..." ? "loadingx" :
                                    formToolResponse === "Connected!" ? "correctx" : "errorx"
                                  }
                                  condition="True"
                                />
                                <span 
                                  style={{
                                  color: 
                                    formToolResponse === null ? '#CA4' :
                                    formToolResponse === "Not connected!" ? '#CA4' :
                                    formToolResponse === "Connected!" ? '#0B0' :
                                    '' // Default color
                                  }}>
                                  {
                                    formToolResponse === null
                                      ? "Not connected!":
                                    formToolResponse === "Not connected!"
                                      ? "Not connected!":
                                    formToolResponse === "Disconnecting..."
                                      ? "Disconnecting...":
                                    formToolResponse === "Disconnected!"
                                      ? "Disconnected!":
                                    formToolResponse === "Connecting..."
                                      ? "Connecting...": 
                                    formToolResponse === "Connected!"
                                      ? "Connected!":
                                    formToolResponse === "Connection failed!"
                                      ? "Connection failed!":
                                    formToolResponse === "Disconnecting failed!"
                                      ? "Disconnecting failed!"
                                      : ""
                                  }
                                </span>
                              </div>
                              <button 
                                disabled={
                                windowResponseI === "Connection established!" ? '':
                                windowResponseI !== "Connection established!" && formToolResponse === "Connected!" ? '':
                                windowResponseI === "Dataset uploaded!" ? '': 'False'
                              }>
                                {
                                  formToolResponse === null ? "Connect":
                                  formToolResponse === "Not connected!" ? "Connect":
                                  formToolResponse === "Connected!" ? "Disconnect" :
                                  formToolResponse === "Disconnected!" ? "Connect" :
                                  formToolResponse === "Connecting..." ? "Connecting..." : "Disconnecting..."                           
                                }
                              </button>
                            </fieldset>
                          </form>
                        </div>
                      )}
                      {selectedSubContent === "batch_input_form_pageII" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : '',
                              backgroundColor: windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form className="batch_files_search_form" onSubmit={(e) => { e.preventDefault()}}>
                            <fieldset className="batch_files_search_form_fieldset">
                              <legend><b>Files</b> on storage</legend>
                              <div className="batch_files_search_form" onSubmit={(e) => { e.preventDefault()}}>
                                <div id="batch_files_search_container" className="batch_files_search_container">
                                  <input id="batch_files_search_input" type="text" placeholder="Type here to seach" onChange={() => action(id,"batch_files_search_input","search",document.getElementById("batch_files_search_input").value)}/>
                                  <button id="batch_files_search_button" title="Search" >
                                   <Icons id="window_live_source_option" type="search" condition="True"/>
                                  </button>
                                  <button title="Search"><Icons id="window_live_source_option" type="inbox-files" condition="True"/></button>
                                </div>
                                <div id="batch_files_search_result_container"
                                    className="batch_files_search_result_container"
                                    style={{
                                      '--searching-text': `'${searchPlaceholder}'`,
                                      display: searchResultsVisible && batchFilesSearchResults? "block" : "none",
                                    }}
                                  >
                                  <ul>
                                    {batchFilesSearchResults && batchFilesSearchResults.length > 0 ? (
                                      batchFilesSearchResults.map((file, index) => (
                                        <li key={file.name} 
                                          style={{
                                            backgroundColor: batchFilesCollection.some((selectedFile) => selectedFile.name === file.name) ? '#EEE':'',
                                            color: batchFilesCollection.some((selectedFile) => selectedFile.name === file.name) ? '#999':''
                                          }} 
                                          onClick={(e) => {
                                            var payload={"name":file.name,"date":file.date,"size":file.size}
                                            action(id, "batch_files_select_file", "toggle_select",payload);}}>
                                          <span>{file.name}</span>
                                          <span>{file.size} Kb</span>
                                        </li>
                                      ))
                                    ) : (
                                      ''
                                    )}
                                  </ul>
                                </div>
                              </div>
                            </fieldset>
                            <fieldset className="batch_files_table_form_fieldset">
                              <legend><b>Selected Files</b></legend>
                              <table>
                                <thead>
                                  <tr>
                                    <th>ID</th>
                                    <th>File name</th>
                                    <th>Date</th>
                                    <th>Size</th>
                                    <th></th>
                                  </tr>
                                </thead>
                                <tbody>
                                    {batchFilesCollection.length > 0  ? (
                                      batchFilesCollection.map((file, index) => (
                                        <tr key={index}>
                                          <td>{index + 1}</td>
                                          <td>{file.name}</td>
                                          <td>{file.date}</td>
                                          <td>{file.size} Kb</td>
                                          <td onClick={(e) => {
                                            e.stopPropagation();
                                            var payload={"name":file.name,"date":file.date,"size":file.size}
                                            action(id, "batch_files_select_file", "toggle_select",payload);}}>
                                            <Icons
                                              id="window_live_source_option"
                                              type="minus"
                                              condition="True"
                                              onClick={() => handleRemoveFile(index)} // Logic to remove the file
                                            />
                                          </td>
                                        </tr>
                                      ))
                                    ) : (
                                    <tr>
                                        <td colSpan="5" style={{ textAlign: "center" }}>
                                          No files selected
                                        </td>
                                      </tr>
                                    )}
                                </tbody>
                              </table>
                            </fieldset>
                          </form>
                        </div>
                      )}
                      {selectedSubContent === "batch_input_form_pageIII" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : windowResponseI === "Dataset uploaded!" ? '#090' : '',
                              backgroundColor: windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : windowResponseI === "Dataset uploaded!" ? 'rgba(0, 255, 0, 0.2)' :''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" :
                                  windowResponseI === "Dataset uploaded!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form className="batch_files_dataframe_form" onSubmit={(e) => { e.preventDefault()}}>
                            <fieldset className="batch_files_dataframe_form_fieldset">
                              <legend><b>Dataframe</b> Infomation</legend>
                              <div className="batch_files_dataframe_infos">
                               <div id="batch_files_dataframe_infos_left_container" className="dataframe_infos_left_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_left_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Source files</td>
                                        <td>
                                          <ul>
                                            {batchFilesCollection.map((file, index) => {
                                              console.log('File object:', file);
                                              return <li key={index}>{file.name || JSON.stringify(file)}</li>;
                                            })}
                                          </ul>
                                        </td>
                                      </tr>
                                      <tr>
                                        <td>Total rows</td>
                                        <td>{batchFilesDataframeInfoI[4]}</td>
                                      </tr>
                                      <tr>
                                        <td>Total columns</td>
                                        <td>{batchFilesDataframeInfoI[3]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                               <div id="batch_files_dataframe_infos_right_container" className="dataframe_infos_right_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_right_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Broker/API</td>
                                        <td>{batchFilesDataframeInfoI[1]}</td>
                                      </tr>
                                      <tr>
                                        <td>Storage</td>
                                        <td>{batchFilesDataframeInfoI[6]}</td>
                                      </tr>
                                      <tr>
                                        <td>Tool</td>
                                        <td>{batchFilesDataframeInfoI[7]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                              </div>
                            </fieldset>
                            <fieldset className="batch_files_dataframe_filter_and_actions">
                              <legend><b> Filters & Actions </b></legend>
                              <div className="batch_files_dataframe_filter_and_actions_container">
                                <div className="batch_files_dataframe_action_inputs">
                                  <label className="actions_label"><i><b>Note :</b> This fields are mandatory.</i></label>
                                  <div className="actions_partition">
                                    <label>Dataframe Action</label>
                                    <select id="batch_files_dataframe_action_select" disabled={batchFilesDataframeInfoI[0] ? false:true} onChange={(e) => action(id,"batch_files_actions_select","change",e.target.value)} className="actions_select_options">
                                      <option value="" disabled selected>Select Action</option>
                                      {batchFilesDataframeInfoI[0] ? (
                                        batchFilesDataframeInfoI[0].map((item, index) => (
                                            <option value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No actions</option>
                                      )}
                                    </select>
                                  </div>
                                  <div className="actions_partition">
                                    <label>Source/Target</label>                                    
                                    <select id="batch_files_dataframe_source_select" disabled={batchFilesDataframeInfoI[2]  && batchFilesDataframeActionValue === "Source / Target Relationship" ? false:true} onChange={(e) => action(id,"batch_files_source_select","change",e.target.value)} style={{float:'left',position:'relative',width:'32.5%',borderRight:'0.1vh dashed #EEE',marginRight:'0.1vw'}}>
                                      <option value="" disabled selected>Select Action</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                    <select id="batch_files_dataframe_target_select" disabled={batchFilesDataframeInfoI[2] && batchFilesDataframeActionValue === "Source / Target Relationship" ? false:true} onChange={(e) => action(id,"batch_files_target_select","change",e.target.value)} style={{float:'left',position:'relative',width:'32%',borderRight:'0.1vh dashed #EEE',marginRight:'0.1vw'}}>
                                      <option value="" disabled selected>Select Action</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                  </div>
                                  <div className="actions_partition">
                                    <label>Relationship label</label>                                    
                                    <input placeholder="HAS_RELATIONSHIP" disabled={batchFilesDataframeActionValue === "Source / Target Relationship" && batchFilesDataframeSourceValue && batchFilesDataframeTargetValue ? false:true} onChange={(e) => action(id,"batch_files_relationship_select","change",e.target.value)}
                                     type='text'/>
                                  </div>
                                  <div className="actions_partition">
                                    <label>Rule to apply</label>                                    
                                    <select id="batch_files_dataframe_rule_select" disabled={batchFilesDataframeInfoI[5] && batchFilesDataframeActionValue === "Link Analysis" ? false:true} onChange={(e) => action(id,"batch_files_rule_select","change",e.target.value)}>
                                      <option value="" disabled selected>Select Analysis rule</option>
                                      {batchFilesDataframeInfoI[5] ? (
                                        batchFilesDataframeInfoI[5].map((item, index) => (
                                            <option key={index}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No Rules</option>
                                      )}
                                    </select>
                                  </div>                                                                                                      
                                </div>
                                <div className="batch_files_dataframe_filter_inputs">
                                  <label className="filters_label"><i><b>Note :</b> Changes require applying inorder to take effect.</i></label>
                                  <div className="filters_partition">
                                    <select id="batch_files_dataframe_filter_selectI" disabled={batchFilesDataframeInfoI[2] ? false:true} onChange={(e) => action(id,"batch_files_target_select","change",e.target.value)}>
                                      <option value="" disabled selected>Select column</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                    <input type='text'/>
                                  </div>
                                  <div className="filters_partition">
                                    <select id="batch_files_dataframe_filter_selectI" disabled={batchFilesDataframeInfoI[2] ? false:true} onChange={(e) => action(id,"batch_files_target_select","change",e.target.value)}>
                                      <option value="" disabled selected>Select column</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                    <input type='text'/>
                                  </div>
                                  <div className="filters_query_partition">
                                    <label>Query area</label>
                                    <textarea placeholder="Type a query to filter with."></textarea>
                                  </div>
                                </div>
                              </div>
                              <div className="batch_files_dataframe_filter_menu">
                                <span className="batch_files_dataframe_filter_menu_rows">{batchFilesDataframeInfoI[4]} Data rows</span>
                                <span className="batch_files_dataframe_filter_menu_add_btn">
                                  <button>Apply filter</button>
                                </span>
                              </div>
                            </fieldset>
                          </form>
                        </div>
                      )}
                      {selectedSubContent === "batch_input_form_pageIV" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: windowResponseI === "Session running..." ? '#009' : windowResponseI === "Connecting..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : '',
                              backgroundColor: windowResponseI === "Session running..." ? 'rgba(0, 0, 255, 0.2)' : windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "Session running..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "Not connected."}</span>
                          </div>
                          <form className="batch_files_dataframe_form" onSubmit={(e) => { e.preventDefault()}}>
                            <fieldset className="batch_files_dataframe_form_fieldset">
                              <legend><b>Dataframe</b> Infomation</legend>
                              <div className="batch_files_dataframe_infos">
                               <div id="batch_files_dataframe_infos_left_container" className="dataframe_infos_left_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_left_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Source files</td>
                                        <td>
                                          <ul>
                                            {batchFilesCollection.map((file, index) => {
                                              console.log('File object:', file);
                                              return <li key={index}>{file.name || JSON.stringify(file)}</li>;
                                            })}
                                          </ul>
                                        </td>
                                      </tr>
                                      <tr>
                                        <td>Total rows</td>
                                        <td>{batchFilesDataframeInfoI[4]}</td>
                                      </tr>
                                      <tr>
                                        <td>Total columns</td>
                                        <td>{batchFilesDataframeInfoI[3]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                               <div id="batch_files_dataframe_infos_right_container" className="dataframe_infos_right_container">
                                {batchFilesDataframeInfoI && batchFilesDataframeInfoI.length > 0 && (
                                  <table id="batch_files_dataframe_infos_right_table" cellPadding='0'>
                                    <tbody>
                                      <tr>
                                        <td>Broker/API</td>
                                        <td>{batchFilesDataframeInfoI[1]}</td>
                                      </tr>
                                      <tr>
                                        <td>Storage</td>
                                        <td>{batchFilesDataframeInfoI[6]}</td>
                                      </tr>
                                      <tr>
                                        <td>Tool</td>
                                        <td>{batchFilesDataframeInfoI[7]}</td>
                                      </tr>
                                    </tbody>
                                  </table>
                                )}
                               </div>
                              </div>
                            </fieldset>
                            <fieldset className="batch_files_dataframe_form_fieldset">
                              <legend><b> Session log </b></legend>
                              <textarea ref={textareaRef} className="batch_files_dataframe_filter_log_textarea" readOnly value={sourceSessionLog}></textarea>
                            </fieldset>
                          </form>
                        </div>
                      )}
                    </div>
                    <div className="batch_connection_form_pager_container">
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageII"){
                            action(id, "batch_input_form_swap_passive", "page_I",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII" && windowResponseI === "Connection established!"){
                            action(id, "batch_input_form_swap_passive", "page_II",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII" && windowResponseI === "Dataset uploaded!"){
                            action(id, "batch_input_form_swap_passive", "page_I",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV"){
                            action(id, "batch_input_form_swap_passive", "page_III",null);
                          }
                          else{
                            return null;
                          }
                        }}
                        disabled={
                          selectedSubContent !== "batch_input_form_pageI" && selectedSubContent !== "batch_input_form_pageIV" ||
                          selectedSubContent === "batch_input_form_pageIV" && !sourceStreamState ? '': 'True' 
                        }>
                        {"Back"}    
                      </button>
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Connection established!"){
                            action(id, "batch_input_form_swap", "page_II",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Dataset uploaded!"){
                            action(id, "batch_input_form_swap", "page_III",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageII"){
                            action(id, "batch_input_form_swap", "page_III",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII"){
                            action(id, "batch_input_form_swap", "page_IV",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV" && sourceStreamState){
                            action(id, "batch_input_stream_terminate", "page_IV",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV" && !sourceStreamState){
                            action(id, "batch_input_form_swap", "page_IV",null);
                          }
                          else{
                            console.log("NoPage to swap")
                            return null;
                          }
                        }}
                        disabled={
                          selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Connection established!" ||
                          selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Dataset uploaded!" && formToolResponse === "Connected!" ||
                          selectedSubContent === "batch_input_form_pageII" && windowResponseI === "Dataset uploaded!" && formToolResponse === "Connected!" ||
                          selectedSubContent === "batch_input_form_pageII" && batchFilesCollection.length> 0  || 
                          selectedSubContent !== "batch_input_form_pageIV" && batchFilesDataframeActionValue === "Store data" || 
                          selectedSubContent !== "batch_input_form_pageIV" && batchFilesDataframeActionValue === "Source / Target Relationship" && batchFilesDataframeSourceValue && batchFilesDataframeTargetValue ||
                          selectedSubContent !== "batch_input_form_pageIV" && batchFilesDataframeActionValue === "Link Analysis" && batchFilesDataframeRuleValue || 
                          selectedSubContent === "batch_input_form_pageIV" && sourceStreamState? '': 'True' 
                        }>
                        {selectedSubContent === "batch_input_form_pageIII" || selectedSubContent === "batch_input_form_pageIV" && !sourceStreamState ? "Stream Graph":
                        selectedSubContent === "batch_input_form_pageIV" ? "Terminate"  : "Next"}
                      </button>
                    </div>
                  </div>
                )}
            </div>
            <div id={`window_properties_${type}_${id}`}  className="properties_container">
              {selectedContent === "graph_content" && (
                <WindowVerticalSplitPanels
                  initialTopHeight={'80%'}
                  minTopHeight={'20%'}
                  maxTopHeight={'100%'}
                  graphRelationships={graphRelationships}
                />
                //,alert(graphRelationships)
               )}
              {selectedContent === "null2" && (
                <div className="placeholder">
                  <IframeEmbed id="source_placeholder"/>
                </div>
              )}
            </div>
            <div id={`window_footer_${type}_${id}`}  className='window_footer'>
              <div className='window_footer'>To zoom on the graph 1. click on the graph 2. toggle capslock button. &nbsp; Right click on the graph to save as Image.
                <span>
                  <b>Window Id : </b>
                  <i>{id}</i>
                </span>
              </div>
            </div>
          </div>
        )}
      </DraggableWindow>
    )
  }
  if (type === "chart"){ //Chart window
        return (
        <DraggableWindow initialPos={{ top: 0, left: 0}} zIndex={zIndex}>
        {(dragProps) => (
          <div
            id={`window_${type}_${id}`}
            className="window"
            style={{ zIndex }}
            onMouseDown={() => onFocus && onFocus()}>
            <div className="window_cover" />
            <div
              className="window_bar"
              onMouseDown={dragProps.onBarMouseDown}
              style={{ cursor: 'grab', userSelect: 'none' }}>
              Data Source
              <div className="window_bar_btns_container">
                <span onClick={() => onClose(id)}>x</span>
                <span>-</span>
              </div>
            </div>
            <div id={`side_bar_${type}_${id}`} className='side_bar'>
              <input id="side_bar_menu_list_link_source" name="size_bar_menu_list" type="radio" onChange={() => action("side_bar_menu")}/>
              <label className="side_bar_menu" htmlFor="side_bar_menu_list_link_source">
                <i className="sbicon"><Icons id="window_side_bar" type="link" condition="True"/></i>
                <div className="side_bar_btn_options">
                  <ul>
                    <label>  
                      <li onClick={() => action("side_bar_menu_list_save")}>    
                        {/*<i>
                          <Icons id="window_side_bar" type="" condition="True"/>
                        </i>*/}              
                        <span>Link a source</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <input id="side_bar_menu_list_capture" name="size_bar_menu_list" type="radio" onChange={() => action("side_bar_menu")}/>
              <label className="side_bar_menu" htmlFor="side_bar_menu_list_capture">
                <i className="sbicon"><Icons id="window_side_bar" type="capture" condition="True"/></i>
                <div className="side_bar_btn_options">
                  <ul>
                    <label>  
                      <li onClick={() => action("side_bar_menu_list_capture")}>                 
                        <span>Take a Snap</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <input id="side_bar_menu_list_print" name="size_bar_menu_list" type="radio" onChange={() => action("side_bar_menu")}/>
              <label className="side_bar_menu" htmlFor="side_bar_menu_list_print">
                <i className="sbicon"><Icons id="window_side_bar" type="print" condition="True"/></i>
                <div className="side_bar_btn_options">
                  <ul>
                    <label>  
                      <li onClick={() => action("side_bar_menu_list_print")}>                 
                        <span>Print</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
            </div>
            <div className='graph_container'>
            </div>
            <div className='properties_container'>
           </div>
           <div className='window_footer'><span><b>Window Id : </b><i>434123</i></span></div>
          </div>
        )}
      </DraggableWindow>
    )
  }
}
function Main({ windows, setWindows, openWindows }) {
  const hasRunRef = useRef(false);

  useEffect(() => {
    if (hasRunRef.current) return;
    hasRunRef.current = true;
    console.log("Running initial openWindows calls");
    openWindows('graph', '');
    // openWindows('graph', 'upload');
    //openWindows('chart', 'upload');
  }, []);  
  return (
    <main id='main'>
    </main>
  );
}
const Loadscreen = ({ loadingText }) => {
    const [dotCount, setDotCount] = useState(0);

    useEffect(() => {
      const interval = setInterval(() => {
        setDotCount(prev => (prev + 1) % 4);
      }, 500);
      return () => clearInterval(interval);
    }, []);

    return (
      <div className="loadscreen">
        {loadingText}
        {'.'.repeat(dotCount)}
      </div>
    );
};
function Root() {
  const [windows, setWindows] = useState([]);
  const [zIndexCounter, setZIndexCounter] = useState(1); // zIndex counter
  const [sessionId, setSessionId] = useState(null);
  const [isToggleMenuOpen, setIsToggleMenuOpen] = useState(false);
  const [loadscreenState, setloadscreenState] = useState(false);
  const [loadscreenText, setloadscreenText] = useState('');
  const [isSideBarMenuOpen, setIsSideBarMenuOpen] = useState(false);
  const [selectedContent, setSelectedContent] = useState(null); // content to show inside the windows
  const [selectedSubContent, setSelectedSubContent] = useState(null); // content to show inside the windows
  const [batchFilesSearchResults, setBatchFilesSearchResults] = useState({ results: [], message: "" });
  const [searchResultsVisible, setSearchResultsVisible] = useState(null);
  const [searchPlaceholder, setSearchPlaceholder] = useState('');
  const [batchFilesDataframeInfoI, setBatchFilesDataframeInfoI] = useState([]);
  const [batchFilesDataframeInfoII, setBatchFilesDataframeInfoII] = useState([]);
  const [batchFilesDataframeActionValue, setBatchFilesDataframeActionValue] = useState(null);
  const [batchFilesDataframeRelationshipValue, setBatchFilesDataframeRelationshipValue] = useState(null);
  const [batchFilesDataframeSourceValue, setBatchFilesDataframeSourceValue] = useState(null);
  const [batchFilesDataframeTargetValue, setBatchFilesDataframeTargetValue] = useState(null);  
  const [batchFilesDataframeRuleValue, setBatchFilesDataframeRuleValue] = useState(null);
  const [sourceStreamState, setSourceStreamState] = useState(null);  
  const [sourceStreamListener, setSourceStreamListener] = useState(false);
  const [sourceSessionLog, setSourceSessionLog] = useState('');
  const [sourceSessionLogFile, setSourceSessionLogFile] = useState(null);     
  const fileInputRef = useRef(null);  
  const socketRef = useRef(null);
  const logRef = useRef(''); // for accumulating logs    
  const textareaRef = useRef(null);
  const debounceRef = useRef(null);
  const windowIdRef = useRef(0);
  const [graphLinkstate, setGraphLinkState] = useState(false);
  const [graphRelationshipsListener, setGraphRelationshipsListener] = useState(false);
  const [graphRelationships, setGraphRelationships] = useState([]); 
  const [graphLinkSource, setGraphLinkSource] = useState(null);   
  
  

  // ---------------------------------------------------------------------------- Basic useEffects ---
  // ---------------------------------------------------------------------------- sockets ---
  useEffect(() => {
    socketRef.current = io("http://localhost:5000");
    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
      }
    };
  }, []);

  useEffect(() => {
    const socket = socketRef.current;
    if (!socket || !sourceStreamListener || !sourceSessionLogFile) return;

    const fetchlogs = (data) => {
      if (data.error) {
        console.error("ErrorLog:", data.error);
      } else {
        logRef.current += '\n' + data.data;
        setSourceSessionLog(logRef.current);
      }
    };
    socket.on("stream_logs", fetchlogs);
    if (socketRef.current) {
      const state = socketRef.current.readyState;
      if (state === WebSocket.CLOSING || state === WebSocket.CLOSED) {
        // Drop the socket if needed
        socketRef.current.close();
        socketRef.current = null;  
      } 
      else{
        socketRef.current.emit("log_stream_plug", { filename: sourceSessionLogFile });
      }
    }
    return () => {
      if (socketRef.current && socketRef.current.connected) {
        socketRef.current.emit('log_stream_unplug');
        socketRef.current.off("stream_logs", fetchlogs);
        //socketRef.current.disconnect(); // optional, if you want to disconnect entirely
      }
    };
  }, [sourceStreamListener, sourceSessionLogFile, socketRef.current]);
  useEffect(() => {
      // console.log("previous-sourceSessionLog: ",sourceSessionLog)
      if (sourceSessionLog) {
        // console.log("returned-sourceSessionLog: ",sourceSessionLog)
        setSourceSessionLog(sourceSessionLog)
        setWindows(prev =>
          prev.map(w =>
            w.sessionId === sessionId ? { ...w, sourceSessionLog } : w
          )
        );
      }
    }, [sourceSessionLog]);  
  // ------------------------------------------------------- relationships socket ---
  useEffect(() => {
    const socket = socketRef.current;
    if (graphRelationshipsListener && socket) {
      const fetchRelationships = (data) => {
        if (data.error) {
          console.error("ErrorLog:", data.error);
        } else {
          setGraphRelationships(data.data)
        }
      };
      // Listen
      socket.on("relationships", fetchRelationships);
      const intervalId = setInterval(() => {
        socket.emit("relationships_fetch_plug", { session_id: graphLinkSource });
      }, 500);
      // Cleanup
      return () => {
        clearInterval(intervalId);
        socket.emit('relationships_fetch_unplug')
        socket.off("relationships", fetchRelationships);
        setGraphRelationshipsListener(false)
      };
    }
  }, [graphRelationshipsListener,graphLinkSource, sessionId]);
  useEffect(() => {
    //console.log("previous-graphRelationships: ",graphRelationships)
    if (graphRelationships) {
      //console.log("returned-graphRelationships: ",graphRelationships)
      setGraphRelationships(graphRelationships)
      setWindows(prev =>
        prev.map(w =>
          w.sessionId === sessionId ? { ...w, graphRelationships } : w
        )
      );
      // console.log("Updated relationships:", graphRelationships);
    }
  }, [graphRelationships]);
  // ---------------------------------------------------------------------------- textarea auto scroll (to bottom) --
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.scrollTo({
        top: textareaRef.current.scrollHeight,
        // behavior: 'smooth',
      });
      setWindows(prev =>
        prev.map(w =>
          w.sessionId === sessionId ? { ...w, textareaRef } : w
        )
      );
    }
  }, [sourceSessionLog]); // Run only when sourceSessionLog updates
  //------------------------------------------------------------------------------ Storage search result container visiblity
  useEffect(() => {
    // console.log("batchFilesSearchResults changed:", batchFilesSearchResults);
    if (batchFilesSearchResults.results) {
      //console.log("Setting searchResultsVisible to true");
      setSearchResultsVisible(true);
      //console.log("on")
    } else {
      //console.log("Setting searchResultsVisible to false");
      setSearchResultsVisible(false);
    }
  }, [batchFilesSearchResults]);
  useEffect(() => {
    if (batchFilesSearchResults.results.length > 0) {
      const handleClickOutside = (event) => {
        const searchButton = document.getElementById('batch_files_search_button');
        const resultContainer = document.getElementById('batch_files_search_result_container');
        console.log('Click detected', event.target);
        if (searchButton && resultContainer) {
          console.log('Container exists:', searchButton);
          if (!searchButton.contains(event.target) && !resultContainer.contains(event.target) ) {
            console.log('Click outside container',event.target);
            resultContainer.style.display = 'none';
            setSearchResultsVisible(false);
            document.removeEventListener("click", handleClickOutside);
          }
        } else {
          console.log('Container not found');
          document.removeEventListener("click", handleClickOutside);
        }
      };
      document.addEventListener("click", handleClickOutside);
      return () => {
        document.removeEventListener("click", handleClickOutside);
      };
      }
    }, []);
  //check the below useeffect's vitality 
  // useEffect(() => {
  //   console.log("searchResultsVisible changed:", searchResultsVisible);
  // }, [searchResultsVisible]);

  //-------------------------------------------------------------------------------- Debounce
  useEffect(() => {
    // Cleanup function to clear the debounce timer when component unmounts or dependencies change
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, []);

  // ---------------------------------------------------------------------------- Windows management ---
  const generateWindowId = () => {
    windowIdRef.current += 1;
    return windowIdRef.current
  };
  const handleCreateWindows = (type) => {
    if (type==="source"){
      const id = generateWindowId(); //Window Id
      debounceRef.current = setTimeout(() => {
        const payload = {id: "init",value:id};
        fetch("http://localhost:5000/init", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        })
        .then((res) => res.json())
        .then((data) => {   
          if (data.message == "success!"){
            const session=data.results     
            //result is the session id received
            setSessionId(session);
            // console.log("session id",session)
            const maxZ = Math.max(0, zIndexCounter);
            setWindows(prev => [...prev, { id:session, type:type, zIndex: maxZ + 1, sessionId:session, selectedContent: null, selectedSubContent: null, formData: {}, windowResponseI: null, formToolResponse: null, batchFilesSearchResults:null}]);
            setZIndexCounter(prev => prev + 1);
          }
          else{
            alert(data.results)
          }
        })
        .catch((err) => {
          console.error(err);
        })
      }, 300); // debounce delay 
    }
    else if (type==="graph"){
      const id = generateWindowId(sessionId);
      setWindows(prev => [...prev, { id, type:type, zIndex: zIndexCounter,sessionId:sessionId, selectedContent: null, selectedSubContent: null}]);
      setZIndexCounter(prev => prev + 1);
    }
  };
  const handleOpenWindows = (type, link) => {
    //Only source window is aplicable
    if (type==="source"){
      if (link===""){
        //If theres no link just creates the window
        handleCreateWindows(type);
      }
      else{
        return;
      }
    }
    if (type==="graph"){
      if (link===""){
        //If theres no link just creates the window
        handleCreateWindows(type);
      }
      else{
        return;
      }
    }
  };
  const handleCloseWindow = (id) => {
    setWindows(prev => prev.filter(w => w.id !== id));
  };
  const handleMoveWindow = (id, newPos) => {
    setWindows(prev =>
      prev.map(w => (w.id === id ? { ...w, position: newPos } : w))
    );
  };

  // ---------------------------------------------------------------------------- Windows functions ---
  // --- Basic Windows actions ---
  const handleWindowActions = (id, menuId, action, payload) => {
    setIsSideBarMenuOpen(prev => prev === menuId ? null : menuId); // Toggle open/close
    setWindows(prev =>
      prev.map(w => {
        if (w.id !== id) return w;
        let loadscreenState = w.loadscreenState;
        let newContent = w.selectedContent;
        let newSubContent = w.selectedSubContent;
        let newBatchSearchResult = w.batchFilesSearchResults;
        let newBatchFilesCollection = w.batchFilesCollection || []; // Makes Ensure selectedFiles is initialized
        let windowResponseI = w.windowResponseI 
        // ------------------------------------------------------------------- Source window contents handling
        if (menuId === "live_source_options" && action === "update") {
          newContent = "live_source_options"; // This will show live source UI
        }
        if (menuId === "upload_source_options" && action === "update") {
          newContent = "upload_source_options"; // This will show upload source UI
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, fileInputRef:fileInputRef} : w
            )
          );
        }
        if (menuId === "upload_source_files" && action === "upload") {
          // Set new timeout for debounce
          if (debounceRef.current) clearTimeout(debounceRef.current);
          debounceRef.current = setTimeout(() => {
            const newLoadscreenText="Uploading Dataset "
            setloadscreenState(true)
            setWindows(prev =>
              prev.map(w =>
                w.id === id ? { ...w,loadscreenState: true,loadscreenText:newLoadscreenText} : w
              )
            );
            const allowedExtensions = [".csv", ".json", ".parquet", ".xlsx"];
            const uploadedFiles = payload.files;
            const invalidFiles = [];
            const validFiles = [];
            for (const file of uploadedFiles) {
              const fileName = file.name.toLowerCase();
              const isValid = allowedExtensions.some(ext => fileName.endsWith(ext));
              if (isValid) {
                validFiles.push(file);
              } else {
                invalidFiles.push(file.name);
              }
            }
            if (invalidFiles.length > 0) { // IF invalid files found
              alert(`Unsupported file types:\n\n${invalidFiles.join("\n")}\n\nAllowed types: CSV, JSON, Parquet.`);
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w,loadscreenState: false ,loadscreenText:null} : w
                )
              );
            }
            else{// Proceed with the uploading
              //Passing files
              const formData = new FormData();
              for (let i = 0; i < validFiles.length; i++) {
                formData.append("file", validFiles[i]);
              }   
              formData.append("session_id",sessionId)
              fetch("http://localhost:5000/upload_batch_files", {
                method: "POST",
                body: formData,
              })
              .then((res) => res.json())
              .then((data) => {
                if (data.message === "success!") {
                  alert("Dataset uploaded!")
                  //calling the tool integration
                  newContent = "batch_input";
                  newSubContent = "batch_input_form_pageI";
                  newBatchFilesCollection = validFiles.map(file => file.name);
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id ? { ...w,loadscreenState: false ,loadscreenText:null,selectedContent:newContent,selectedSubContent:newSubContent,windowResponseI:"Dataset uploaded!",batchFilesCollection:newBatchFilesCollection} : w
                    )
                  );
                } 
                else {
                  alert(data.message)
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id ? { ...w,loadscreenState: false ,loadscreenText:null} : w
                    )
                  );
                }
              })
              .catch((err) => {
                console.error("err",err);
                setWindows(prev =>
                  prev.map(w =>
                    w.id === id ? { ...w,loadscreenState: false ,loadscreenText:null} : w
                  )
                );
              });
            } 
          }, 300); // debounce delay
        }
        if (menuId === "live_source_options_passive" && action === "update") {
          newContent = "live_source_options";
        }
        if (menuId === "batch_input" && action === "update") {
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageI";
          //setting page for batch (default)
          if (windowResponseI === "Dataset uploaded!"){
            setWindows(prev =>
              prev.map(w =>
                w.id === id ? { ...w,loadscreenState: false ,loadscreenText:null,selectedContent:newContent,selectedSubContent:newSubContent,windowResponseI:null,batchFilesCollection:newBatchFilesCollection} : w
              )
            );
          }
          else{
            setWindows(prev =>
              prev.map(w =>
                w.id === id ? { ...w,loadscreenState: false ,loadscreenText:null,selectedContent:newContent,selectedSubContent:newSubContent,windowResponseI:windowResponseI,batchFilesCollection:newBatchFilesCollection} : w
              )
            );
          }        
        }
        if (menuId === "batch_input_form" && action === "connect" && payload) {
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, windowResponseI: "Connecting..." } : w
            )
          );
          fetch("http://localhost:5000/connect_to_source", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          })
          .then((res) => res.json())
          .then((data) => {
            setWindows(prev =>
              prev.map(w =>
                w.id === id ? { ...w, windowResponseI: data.message } : w
              )
            );
          })
          .catch((err) => {
            console.error(err);
            setWindows(prev =>
              prev.map(w =>
                w.id === id ? { ...w, windowResponseI: "Connection failed!" } : w
              )
            );
          });        
        }
        if (menuId === "batch_input_form" && action === "disconnect" && payload) {
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, windowResponseI: "Disconnecting..." } : w
            )
          );
          fetch("http://localhost:5000/disconnect_source", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          })
            .then((res) => res.json())
            .then((data) => {
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, windowResponseI: data.message } : w
                )
              );
            })
            .catch((err) => {
              console.error(err);
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, windowResponseI: "Disconnecting failed!" } : w
                )
              );
            });        
        }
        if (menuId === "tool_integration_form" && action === "connect" && payload) {
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, formToolResponse: "Connecting..." } : w
            )
          );
          fetch("http://localhost:5000/connect_to_tool", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          })
            .then((res) => res.json())
            .then((data) => {              
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, formToolResponse: data.message } : w
                )
              );
            })
            .catch((err) => {
              console.error(err);
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, formToolResponse: "Connecting failed!" } : w
                )
              );
            });        
        }
        if (menuId === "tool_integration_form" && action === "disconnect" && payload) {
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, formToolResponse: "Disconnecting..." } : w
            )
          );
          fetch("http://localhost:5000/disconnect_tool", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          })
            .then((res) => res.json())
            .then((data) => {              
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, formToolResponse: data.message } : w
                )
              );
            })
            .catch((err) => {
              console.error(err);
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, formToolResponse: "Disonnecting failed!" } : w
                )
              );
            });        
        }
        if (menuId === "batch_input_form_swap" && action === "page_I") {
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageI";
          //console.log("here", newSubContent); // Debugging
        }
        if (menuId === "batch_input_form_swap_passive" && action === "page_I") {
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageI";
        }
        if (menuId === "batch_input_form_swap" && action === "page_II") {
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageII";
        }
        if (menuId === "batch_input_form_swap_passive" && action === "page_II") {
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageII";
        }
        if (menuId === "batch_files_search_input" && action === "search") {
          // Set new timeout for debounce
          debounceRef.current = setTimeout(() => {
            // Update windows state to clear previous results immediately
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? { ...w, batchFilesSearchResults: [], searchResultsVisible: true,searchPlaceholder:'Searching...' }
                  : w
              )
            );

            const search_value = payload;
            const newPayload = {
              id: "search",
              value: search_value,
              session_id:sessionId
            };
            setSearchPlaceholder('Searching...');
            fetch("http://localhost:5000/live_batch_files", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(newPayload),
            })
              .then((res) => res.json())
              .then((data) => {
                setBatchFilesSearchResults(data);
                if (data.message === "Results found!") {
                  setSearchPlaceholder('');
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id ? { ...w, batchFilesSearchResults: data.results,searchPlaceholder:data.message } : w
                    )
                  );
                } else if (data.message === "Search failed!") {
                  setSearchPlaceholder(data.message); // or set to some other text
                } else {
                  // No result
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id ? { ...w, batchFilesSearchResults: [], searchPlaceholder:data.message} : w
                    )
                  );
                }
              })
              .catch((err) => {
                console.error(err);
                setWindows(prev =>
                  prev.map(w =>
                    w.id === id ? { ...w, batchFilesSearchResults: [] } : w
                  )
                );
              });
          }, 300); // debounce delay
        }
        if (menuId === "batch_files_select_file" && action === "toggle_select") {
          var filename=payload["name"];
          console.log("newBatchFilesCollection:",newBatchFilesCollection);
          console.log("gotPayload:",payload);
          console.log("gotfilename:",filename);

          const isAlreadySelected = newBatchFilesCollection.some(
            (file) => file.name === filename
          );
          if (isAlreadySelected) {
              // File is already selected, remove it
              console.log(`Deselecting file: ${filename}`);
              newBatchFilesCollection = newBatchFilesCollection.filter(
                (file) => file.name !== filename
              );
            } else {
              // File is not selected, add it
              console.log(`Selecting file: ${filename}`);
              newBatchFilesCollection = [...newBatchFilesCollection, payload];
            }
            console.log("Updated batchFilesCollection:", newBatchFilesCollection);
            return { ...w, batchFilesCollection: newBatchFilesCollection};
        }
        if (menuId === "batch_input_form_swap" && action === "page_III") {
          //Request a dataframe creation with the selected file           
          // Set new timeout for debounce
          debounceRef.current = setTimeout(() => {
            const newLoadscreenText="Creating Dataframe "
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w, batchFilesDataframeInfoI:[],batchFilesDataframeActionValue,loadscreenState: true,loadscreenText:newLoadscreenText} : w
              )
            );
            //Requesting dataframe creation
            let dataSourceKind="spark";
            if (windowResponseI == "Dataset uploaded!"){
                dataSourceKind = "file"
            }
            const payload = {
                id: "create_DF",
                kind:dataSourceKind,
                sessionId: sessionId,
                value: newBatchFilesCollection,
              };
            fetch("http://localhost:5000/live_batch_files", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            })
            .then((res) => res.json())
            .then((data) => {    
                var arrayData=Object.values(data.results)
                setBatchFilesDataframeInfoI(arrayData)
                if (data.message == "success!"){
                  //Changing window content
                  alert("Dataframe created")
                  newContent = "batch_input";
                  newSubContent = "batch_input_form_pageIII";
                  //Setting DataframeInfoI        
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id ? { ...w, batchFilesDataframeInfoI:arrayData,loadscreenState: false, loadscreenText:null, selectedContent:newContent,selectedSubContent:newSubContent,batchFilesDataframeActionValue:null } : w
                    )
                  );
                }
                else {
                  alert("faild to create a dataframe1",data.message)
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id ? { ...w, batchFilesDataframeInfoI:[],loadscreenState: false } : w
                    )
                  );
                }
              })
            .catch((err) => {
              console.error(err);
              alert("requestID8234692 failed!");
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeInfoI: null, loadscreenState: false} : w
                )
              );
            });
          }, 300); // debounce delay       
        }
        if (menuId === "batch_input_form_swap_passive" && action === "page_III") {
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, batchFilesDataframeActionValue:null,batchFilesDataframeSourceValue:null,batchFilesDataframeTargetValue:null,batchFilesDataframeRelationshipValue:null,batchFilesDataframeRuleValue:null} : w
            ))
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageIII";
        }
        if (menuId === "batch_files_actions_select" && action === "change") {
          setBatchFilesDataframeActionValue(null)
          setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeActionValue: null,loadscreenState:true,loadscreenText:null} : w
                )
          );
           debounceRef.current = setTimeout(() => {
            setBatchFilesDataframeActionValue(payload)
            setBatchFilesDataframeRelationshipValue(true)
            setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeActionValue: payload,loadscreenState:false} : w
                )
          );                                    
          }, 300);
        } 
        if (menuId === "batch_files_source_select" && action === "change") {
          setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeSourceValue: null,loadscreenState:true} : w
                )
          );
           debounceRef.current = setTimeout(() => {
            setBatchFilesDataframeSourceValue(payload)
            setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeSourceValue: payload,loadscreenState:false} : w
                )
          );                                    
          }, 300);
        }
        if (menuId === "batch_files_target_select" && action === "change") {
          setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeTargetValue: null,loadscreenState:true} : w
                )
          );
           debounceRef.current = setTimeout(() => {
            setBatchFilesDataframeTargetValue(payload)
            setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeTargetValue: payload,loadscreenState:false} : w
                )
          );                                    
          }, 300);
        }
        if (menuId === "batch_files_relationship_select" && action === "change") {
          //No need for debounce or loadscreen
          setBatchFilesDataframeRelationshipValue(payload)
            setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeRelationshipValue: payload} : w
                )
          );
        }
        if (menuId === "batch_files_rule_select" && action === "change") {
          setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeRuleValue: null,loadscreenState:true} : w
                )
          );
           debounceRef.current = setTimeout(() => {
            setBatchFilesDataframeRuleValue(payload)
            setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeRuleValue: payload,loadscreenState:false} : w
                )
          );                                    
          }, 300);
        }        
        if (menuId === "batch_input_form_swap" && action === "page_IV") { //This is the component that initalizes the graph streaming
          // Set new timeout for debounce
          debounceRef.current = setTimeout(() => {
            const newLoadscreenText="Initalizing "
            // Initialize a variable to accumulate logs
            let accumulatedLogs = '';
            setSourceStreamState(false);
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w, windowResponseI:null,sourceStreamState:false,sourceSessionLog:null,loadscreenState: true, loadscreenText:newLoadscreenText } : w
              )
            );
            //Requesting session start
            let action=batchFilesDataframeActionValue;
            let source=batchFilesDataframeSourceValue;
            let target=batchFilesDataframeTargetValue;
            let relationship = batchFilesDataframeRelationshipValue && batchFilesDataframeRelationshipValue !== '' &&   batchFilesDataframeRelationshipValue !== true
              ? batchFilesDataframeRelationshipValue : 'HAS_RELATIONSHIP';            
            let tool=batchFilesDataframeInfoI[7]; //Tools values sent from dataframe info
            let rule=batchFilesDataframeRuleValue;
            //Template stance if failed to start the session (keeps the page from swaping)
            newContent = "batch_input";
            newSubContent = "batch_input_form_pageIII";
            const payload = {
                id: "start_session",
                value:{"window_id":id,"session_id":sessionId,"tool":tool,"action":action,"source":source,"target":target,"relationship":relationship,"rule":rule}// Add filter request here
              };
            fetch("http://localhost:5000/live_batch_files", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            })
            .then((res) => res.json())
            .then((data) => { 
              //var response=Object.values(data.results)
              if (data!=null){
                if (data.message==="success!"){
                  const logFile = data.results;
                  const socket = socketRef.current;
                  if (!socket || !logFile) return;
                  setSourceStreamListener(true);
                  setSourceSessionLogFile(logFile);
                  setSourceStreamState(true)
                  newContent = "batch_input";
                  newSubContent = "batch_input_form_pageIV";
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id
                        ? {
                            ...w,
                            windowResponseI: "Session running...",
                            loadscreenState: false,
                            selectedContent: newContent,
                            selectedSubContent: newSubContent,
                            sourceStreamState: true
                          }
                        : w
                    )
                  );
                }
                else{
                  alert("failed!1",data.exception)
                }
              }
            })
            .catch((err) => {
              console.error("err",err);
              alert("requestID8234692 failed!");
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeInfoI, loadscreenState: false} : w
                )
              );
            });
          }, 300); // debounce delay   
        }
        if (menuId === "batch_input_stream_terminate" && action === "page_IV") {
          // Set new timeout for debounce
          debounceRef.current = setTimeout(() => {
            const newLoadscreenText="Terminating "
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w, windowResponseI:null, sourceStreamState:sourceStreamState, sourceSessionLog:null,loadscreenState: true, loadscreenText:newLoadscreenText } : w
              )
            );
            //Turning the socket off 
            const socket = socketRef.current;
            if (socket && socket.connected) {
              socket.off("stream_logs");
              socket.emit('log_stream_unplug');
            }
            //Refresh the options (the previous content options)
            let oldBatchFilesDataframeInfoI=batchFilesDataframeInfoI
            setBatchFilesDataframeInfoI(null)

            const payload = {
                id: "end_session",
                value:{"window_id":id,"session_id":sessionId,"log_file":sourceSessionLogFile}
              };
            fetch("http://localhost:5000/live_batch_files", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            }) 
            .then((res) => res.json())
            .then((data) => { 
              if (data!=null){
                if (data.message==="success!"){
                  if (socket) socket.off("stream_logs"); 
                  setSourceStreamListener(false);
                  setSourceSessionLogFile(null);
                  setSourceStreamState(false);
                  setBatchFilesDataframeInfoI(oldBatchFilesDataframeInfoI)
                  setWindows(prev => prev.map(w =>
                    w.id === id
                    ? { ...w,windowResponseI:null,loadscreenState: false,batchFilesDataframeInfoI:oldBatchFilesDataframeInfoI,sourceStreamState:false, sourceSessionLog: null, setSourceSessionLogFile:null,setSourceStreamState:false}
                      : w
                  ));
                  //alert("alert_message:",data.message)
                }
                else{
                  setWindows(prev => prev.map(w =>
                    w.id === id
                    ? { ...w,loadscreenState: false,sourceStreamState:sourceStreamState, sourceSessionLog: null,  setSourceSessionLogFile:null,setSourceStreamState:false}
                      : w
                  ));
                  alert("No streaming found!",data.results)
                }
                //Return to back page
                newContent = "batch_input";
                newSubContent = "batch_input_form_pageIII";
              }
              let newWindowResponseI
              if (setBatchFilesDataframeInfoI){
                newWindowResponseI="Dataset uploaded!"
              }
              else{
                newWindowResponseI="The dataset was lost. Please try with a new source window."
              }
              console.log("newWindowResponseI:",newWindowResponseI)
              setWindows(prev =>
                prev.map(w =>
                  w.id === id
                    ? {
                        ...w,
                        windowResponseI:newWindowResponseI,
                        loadscreenState: false,
                        selectedContent: newContent,
                        selectedSubContent: newSubContent
                      }
                    : w
                )
              );
            })
            .catch((err) => {
              console.error("err",err);
              alert("requestID8234692 failed!");
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeInfoI, loadscreenState: false} : w
                )
              );
            });
          }, 300); // debounce delay   
        }
        // ------------------------------------------------------------------- Graph window contents handling
        if (menuId === "graph_link_form" && action === "link") {
          // Set new timeout for debounce
          debounceRef.current = setTimeout(() => {
            const newLoadscreenText="Linking window "
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w,loadscreenState: true, loadscreenText:newLoadscreenText } : w
              )
            );
            const sourceId = payload["sourceId"];
            const newPayload = {
                id: "link",
                source_id:sourceId
              };
            fetch("http://localhost:5000/graph_link", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(newPayload),
            })
            .then((res) => res.json())
            .then((data) => { 
              if (data!=null){
                if (data.message==="success!"){
                  setGraphLinkState(true)
                  setGraphLinkSource(sourceId)
                  setGraphRelationshipsListener(true)
                  alert("Linked")                                              
                  newContent = "graph_content";
                  newSubContent = "";
                  setWindows(prev => prev.map(w =>
                    w.id === id
                      ? { ...w,selectedContent:newContent,graphLink:true,loadscreenState: false}
                      : w
                  ));
                }
                else{
                  setGraphLinkState(false)
                  setGraphLinkSource(null)
                  setGraphRelationshipsListener(false)                
                  setIsSideBarMenuOpen("link_graph_options")
                  alert("No streaming found!",data.message)
                  setWindows(prev =>
                    prev.map(w =>
                     w.id === id ? { ...w, selectedContent:selectedContent,graphLink:false,loadscreenState: false} : w
                    )
                  );
                }
              }
            })
            .catch((err) => {
              setGraphLinkState(false)
              setGraphLinkSource(null)
              setGraphRelationshipsListener(false)                
              setIsSideBarMenuOpen("link_graph_options")
              alert("Linking failed!",data.message)
              setWindows(prev =>
                prev.map(w =>
                 w.id === id ? { ...w, selectedContent:selectedContent,graphLink:false,loadscreenState: false} : w
                )
              );
            });
          }, 300); // debounce delay            
        }
        if (menuId === "graph_link_form" && action === "unlink") {
            setGraphLinkState(false)
            setGraphLinkSource(null)
            setGraphRelationshipsListener(false)
            alert("message box: any unsaved progress is lost!")                
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w, selectedContent:null,graphLink:false,loadscreenState: false} : w
              )
            );
        }
        if (menuId === "reset_options" && action === "clear") {
          newContent = null;
        }

        return { ...w, 
          selectedContent: newContent, 
          selectedSubContent: newSubContent, 
          batchFilesSearchResults: newBatchSearchResult, 
          searchResultsVisible: searchResultsVisible,
          batchFilesCollection : newBatchFilesCollection,
          searchPlaceholder: searchPlaceholder,
          batchFilesDataframeInfoI: batchFilesDataframeInfoI,
          batchFilesDataframeInfoII: batchFilesDataframeInfoII,
          batchFilesDataframeActionValue: batchFilesDataframeActionValue,
          loadscreenState: loadscreenState,
          sourceStreamState: sourceStreamState,
          textareaRef: textareaRef
        };
      })
    );
  };
  // --- Bring window to front ---
  const bringToFront = (id) => {
    setWindows(prev => {
      const maxZ = Math.max(0, zIndexCounter);
      return prev.map(w =>
        w.id === id ? { ...w, zIndex: maxZ + 1 } : w
      );
    });
    setZIndexCounter(prev => prev + 1);
  };
  // --- Navigation Menu Actions ---
  const handleNavAction = (action) => {
    //alert("nav_menu")
    // handle specific nav actions here
  };
  // --- Toggle Menu Actions ---
  const handleToggleMenu = (id) => {
    setIsToggleMenuOpen(prev => !prev);
    if(id=="toggle_menu_new_source_window"){
      handleOpenWindows("source","");
    }
    else if(id=="toggle_menu_new_graph_window"){
      handleOpenWindows("graph","");
    }
    else if(id=="toggle_menu_new_chart_window"){
      handleOpenWindows("chart","");
    }
    else if(id=="toggle_menu_new_tabel_window"){
      handleOpenWindows("table","");
    }
    else{
      return null;
    }
  };
  // --- Root Return ---
  return (
    <div style={{ position: 'relative', minHeight: '100vh', overflow: 'hidden' }}>
      <NetworkBackground />
      <div style={{ position: 'absolute',top:0,bottom:0,left:0,right:0,zIndex: 1 }}> 
        <NavBar onNavAction={handleNavAction}/>
        <ToggleMenu onToggle={handleToggleMenu} isToggleMenuOpen={isToggleMenuOpen} action={handleToggleMenu}/>
        <Main windows={windows} setWindows={setWindows} openWindows={handleOpenWindows}/>      
        <Taskbar />
        {/* Render windows */}
        {windows.map(window => (
          <Windows
            key={window.id}
            id={window.id}
            {...window}
            type={window.type}
            sessionId={window.sessionId}
            isToggleMenuOpen={isToggleMenuOpen}
            isSideBarMenuOpen={isSideBarMenuOpen}
            loadscreenState={window.loadscreenState}
            loadscreenText={window.loadscreenText}
            action={handleWindowActions}
            selectedContent={window.selectedContent}
            selectedSubContent={window.selectedSubContent}
            windowResponseI={window.windowResponseI}
            windowResponseII={window.windowResponseII}
            formToolResponse={window.formToolResponse}
            batchFilesSearchResults={window.batchFilesSearchResults}
            searchResultsVisible={window.searchResultsVisible}
            searchPlaceholder={window.searchPlaceholder}
            batchFilesCollection={window.batchFilesCollection}
            batchFilesDataframeInfoI={window.batchFilesDataframeInfoI}
            batchFilesDataframeInfoII={window.batchFilesDataframeInfoII}
            batchFilesDataframeActionValue={window.batchFilesDataframeActionValue}
            batchFilesDataframeRelationshipValue={window.batchFilesDataframeRelationshipValue}
            sourceSessionLog={window.sourceSessionLog}
            sourceStreamState={window.sourceStreamState}
            fileInputRef={window.fileInputRef}
            textareaRef={window.textareaRef}
            onClose={handleCloseWindow}
            onMove={handleMoveWindow}
            zIndex={window.zIndex}
            onFocus={() => bringToFront(window.id)}
            graphLink={window.graphLink}
            graphRelationships={window.graphRelationships}
          />
        ))}
      </div>
    </div>
  )
}

export default Root;

