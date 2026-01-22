import React, { useState, useRef, useEffect, createContext, useContext, useCallback } from 'react';
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
      <div className="toggle_menu_list_container">
        <div className="toggle_menu_list">
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
  return (
    <div id='bottom_slider' style={{color:'#B00'}}>
      Note:This site Is still under development, Working on : <b>Toggle menu</b>
    </div>
  )
}
function WindowVerticalSplitPanels({ initialTopHeight, minTopHeight, maxTopHeight}) {
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
            <input id="relation1" name="relationship" type="radio"/>
            <label htmlFor="relation1">NEW_RELATIONSHIP</label>
          </li>
          <li>
            <input id="relation2" name="relationship"  type="radio"/>
            <label htmlFor="relation2">HAS_LIKES</label>
          </li>
          <li>
            <input id="relation3" name="relationship"  type="radio"/>
            <label htmlFor="relation3">RETWEETS</label>
          </li>
          <li>
            <input id="relation4" name="relationship" type="radio"/>
            <label htmlFor="relation4">FHASKAU</label>
          </li>
          <li>
            <input id="relation5" name="relationship"  type="radio"/>
            <label htmlFor="relation5">MY_LIKES</label>
          </li>
          <li>
            <input id="relation6" name="relationship"  type="radio"/>
            <label htmlFor="relation6">FDS</label>
          </li>
          <li>
            <input id="relation7" name="relationship" type="radio"/>
            <label htmlFor="relation7">TUWUU</label>
          </li>
          <li>
            <input id="relation8" name="relationship"  type="radio"/>
            <label htmlFor="relation8">LIKES</label>
          </li>
          <li>
            <input id="relation9" name="relationship"  type="radio"/>
            <label htmlFor="relation9">RETWEETS</label>
          </li>
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
 useEffect(() => {
    console.log('Current zIndex:', zIndex);
  }, [zIndex]);
  return (
    <div
      ref={windowRef}
      className="window"
      style={{ top: pos.top, left: pos.left, position: 'absolute', userSelect: 'none', zIndex: zIndex }}
    >
      <div className="window_cover" />
    {children({ onBarMouseDown: onMouseDown })}
    </div>
  );
}
function Windows({ id, type, loadscreenState, isSideBarMenuOpen, action, selectedContent, selectedSubContent, formIResponse,formIIResponse,formToolResponse,batchFilesSearchResults,searchResultsVisible,searchPlaceholder,batchFilesCollection, batchFilesDataframeInfoI, batchFilesDataframeInfoII, onClose, onMove, zIndex, onFocus }) {
  console.log("SelectedContent:", selectedContent, typeof selectedContent);
  if (type === "source") {
    console.log(`Window [${id}] selectedContent:`, selectedContent);
      return (
        <DraggableWindow initialPos={{ top: 0, left: 0}} zIndex={zIndex}>
        {(dragProps) => (

          <div
            id={`window_${type}_${id}`}
            style={{ zIndex }}
            className="window"
            onMouseDown={() => onFocus && onFocus()}>
            <div className="window_cover" />
            <div className="windows_loadscreen" 
              style={{
                display: loadscreenState ? 'Block':'none'
              }}>
              <div className="loadscreen">Loading ...</div>
            </div>          
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
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Live source</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
            </div>
            <div id="source_window_source_container" className='source_container'>
              <div id="source_window_source_content" className='content_container'>
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
                {selectedContent === null && (
                  <div className="placeholder">
                    <IframeEmbed id="source_placeholder"/>
                  </div>
                )}
                {selectedContent === "batch_input" && (
                  <div className="live_source_options_container">
                    <div className="" style={{fontSize:'2.5vh',borderBottom:'1px solid #ccc', padding:'2vh',textAlign:'left',paddingLeft:'0vw',margin:'1.5vw',marginBottom:0}}>Pick a source input</div>
                    <div className="live_source_option" style={{position:'absolute',top:'calc( 3vh - 3vw)',left:'0vw'}} onClick={() => action(id,"live_source_options_passive","update")}>
                      <span className="live_source_option_icon">
                        <Icons id="window_live_source_option" type="batch_input" condition="True"/> 
                      </span>
                      <span className="live_source_option_details">
                        <label>Batch input</label>
                        <p>Connect to a Broker/API and fetch for datas as a batch query.</p>
                      </span>
                    </div>
                    <div className="batch_connection_form_container">
                      {selectedSubContent === "batch_input_form_pageI" && (
                        <div>
                          <div id="batch_input_form_response" className="form_response_container"
                            style={{
                              color: formIResponse === "Connecting..." ? '#999' : formIResponse === "Connection established!" ? '#090' : formIResponse === "Connection failed!" ? '#900' : '',
                              backgroundColor: formIResponse === "Connecting..." ? 'rgba(0,0,0,0.1)' : formIResponse === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : formIResponse === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  formIResponse === "Connecting..." ? "loadingx" :
                                  formIResponse === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{formIResponse || "Not connected."}</span>
                          </div>
                          <form onSubmit={(e) => { e.preventDefault(); 
                            if (formIResponse === "Connection established!") {
                              // Disconnect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              action(id, "batch_input_form", "disconnect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                              });
                            }
                            else if (formIResponse === "Disconnecting failed!") {
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
                                formIResponse === "Connecting..." ? 'True' : 
                                formIResponse === "Connection established!" ? 'True': ''
                              }
                              style={{
                                color: formIResponse === "Connection established!" ? '#AAA' : '',
                                backgroundColor: formIResponse === "Connection established!" ? '#EEE' : '',
                                borderColor: formIResponse === "Connection established!" ? '#DDD' : ''
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
                                formIResponse === "Connecting..." ? 'True' : 
                                formIResponse === "Connection established!" ? 'True': ''
                              }
                              style={{
                                color: formIResponse === "Connection established!" ? '#AAA' : '',
                                backgroundColor: formIResponse === "Connection established!" ? '#EEE' : '',
                                borderColor: formIResponse === "Connection established!" ? '#DDD' : ''
                              }} defaultValue="localhost:9870"/>
                            </fieldset>                        
                            <button type="submit"><span><Icons id="window_live_source_option" type="connect" condition="True"/></span>
                              <span>
                                {
                                  formIResponse === null
                                    ? "Connect":
                                  formIResponse === "Not connected."
                                    ? "Connect":
                                  formIResponse === "Disconnecting..."
                                    ? "Disconnecting...":
                                  formIResponse === "Disconnected!"
                                    ? "Connect":
                                  formIResponse === "Connecting..."
                                    ? "Connecting...": 
                                  formIResponse === "Connection established!"
                                    ? "Disconnect":
                                  formIResponse === "Connection failed!"
                                    ? "Connect":
                                  formIResponse === "Connection failed! No storage found."
                                    ? "Connect":
                                  formIResponse === "Disconnecting failed!"
                                    ? "Retry"
                                    : ""
                                }
                              </span>
                             </button>
                          </form>
                          <form onSubmit={(e) => { e.preventDefault();
                            if (formIResponse === "Connection established!" && formToolResponse !== "Connected!") {
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
                                formIResponse === "Connection established!" ? '':
                                formIResponse !== "Connection established!" && formToolResponse === "Connected!" ? '': 'False'
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
                              color: formIResponse === "Connecting..." ? '#999' : formIResponse === "Connection established!" ? '#090' : formIResponse === "Connection failed!" ? '#900' : '',
                              backgroundColor: formIResponse === "Connecting..." ? 'rgba(0,0,0,0.1)' : formIResponse === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : formIResponse === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  formIResponse === "Connecting..." ? "loadingx" :
                                  formIResponse === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{formIResponse || "Not connected."}</span>
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
                                      display: searchResultsVisible ? "block" : "none",
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
                              color: formIResponse === "Connecting..." ? '#999' : formIResponse === "Connection established!" ? '#090' : formIResponse === "Connection failed!" ? '#900' : '',
                              backgroundColor: formIResponse === "Connecting..." ? 'rgba(0,0,0,0.1)' : formIResponse === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : formIResponse === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  formIResponse === "Connecting..." ? "loadingx" :
                                  formIResponse === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{formIResponse || "Not connected."}</span>
                          </div>
                          <form className="batch_files_dataframe_form" onSubmit={(e) => { e.preventDefault()}}>
                            <fieldset className="batch_files_dataframe_form_fieldset">
                              <legend><b>Dataframe</b> Infomation</legend>
                              <div className="batch_files_dataframe_infos">
                               <div id="batch_files_dataframe_infos_left_container" className="dataframe_infos_left_container">
                                {batchFilesDataframeInfoI && (
                                  batchFilesDataframeInfoI.map((info, index) => (
                                      <table id="batch_files_dataframe_infos_left_table" cellPadding='0'>
                                        <tbody>
                                        <tr>
                                          <td>Source files</td>
                                          <td>
                                            <ul>
                                              <li></li>
                                            </ul>
                                          </td>
                                        </tr>
                                        <tr>
                                          <td>Total rows</td>
                                          <td>{info.num_rows}</td>
                                        </tr>
                                        <tr>
                                          <td>Total columns</td>
                                          <td>{info.num_cols}</td>
                                        </tr>
                                      </tbody>
                                    </table>
                                  ))
                                )}
                               </div>
                               <div id="batch_files_dataframe_infos_right_container" className="dataframe_infos_right_container">
                                {batchFilesDataframeInfoII}
                               </div>
                              </div>
                            </fieldset>
                            <fieldset className="batch_files_dataframe_filter">
                              <legend><b> Filters </b></legend>
                              <div className="batch_files_dataframe_filter_container">
                                <div className="batch_files_dataframe_filter_inputs">
                                  <div className="filters_partition">
                                    <select>
                                      <option>option1</option>
                                    </select>
                                    <input type='text'/>
                                  </div>
                                  <div className="filters_partition">
                                    <select>
                                      <option>option1</option>
                                    </select>
                                    <input type='text'/>
                                  </div>
                                  <div className="filters_partition">
                                    <select>
                                      <option>option1</option>
                                    </select>
                                    <input type='text'/>
                                  </div>
                                </div>
                                <div className="batch_files_dataframe_filter_query">
                                  <div className="filters_partition">
                                    <label>Query area</label>
                                    <textarea placeholder="Type a query to filter with."></textarea>
                                  </div>
                                </div>
                              </div>
                              <div className="batch_files_dataframe_filter_menu">
                                <span className="batch_files_dataframe_filter_menu_rows">125 Data rows</span>
                                <span className="batch_files_dataframe_filter_menu_add_btn">
                                  <button>Apply filter</button>
                                </span>
                              </div>
                            </fieldset>
                          </form>
                        </div>
                      )}
                    </div>
                    <div className="batch_connection_form_pager_container">
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageII"){
                            action(id, "batch_input_form_swap", "page_I",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII"){
                            action(id, "batch_input_form_swap", "page_II",null);
                          }
                          else{
                            return null;
                            //action(id, "batch_input_form_swap", "page_I",null);
                          }
                        }}
                        disabled={
                          selectedSubContent !== "batch_input_form_pageI" ? '': 'True' 
                        }>Back</button>
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageI"){
                            action(id, "batch_input_form_swap", "page_II",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageII"){
                            action(id, "batch_input_form_swap", "page_III",null);
                          }
                          else{
                            console.log("here",selectedSubContent)
                            return null;
                            //action(id, "batch_input_form_swap", "page_I",null);
                          }
                        }}
                        disabled={
                          selectedSubContent === "batch_input_form_pageI" && 
                          formIResponse === "Connection established!" ||
                          selectedSubContent === "batch_input_form_pageII" && 
                          batchFilesCollection.length> 0  ? '': 'True' 
                        }>Next</button>
                    </div>
                  </div>
                )}
              </div>
            </div>
            <div id="source_window_source_properties" className="properties_container">
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
            <div className='window_footer'>
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
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'link_source_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'link_source_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action(id,"link_source_options","test1")}>
                  <Icons id="window_side_bar" type="link" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'link_source_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("side_bar_menu_list","link_source_options","test2")}>      
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Link to source</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'save_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'save_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action(id,"save_options","test1")}>
                  <Icons id="window_side_bar" type="save" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'save_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action(id,"save_options","test2")}>      
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Save Graph</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'snap_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'snap_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action(id,"snap_options","test1")}>
                  <Icons id="window_side_bar" type="capture" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'snap_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("side_bar_menu_list","snap_options","test2")}>      
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Take a snap</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'print_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'print_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action(id,"print_options","test1")}>
                  <Icons id="window_side_bar" type="print" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'print_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action("side_bar_menu_list","print_options","test2")}>      
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Print Graph</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'expand_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'expand_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action(id,"expand_options","test1")}>
                  <Icons id="window_side_bar" type="expand" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'expand_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action(id,"expand_options","test2")}>      
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Expand view</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'reset_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'reset_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action(id,"reset_options","test1")}>
                  <Icons id="window_side_bar" type="reset" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'reset_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action(id,"reset_options","test2")}>      
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Reset Graph</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>
              <label className="side_bar_menu"
                style={{
                  backgroundColor: isSideBarMenuOpen === 'export_options' ? '#333' : 'rgba(0,0,0,0)',borderRadius: isSideBarMenuOpen === 'export_options' ? 'none' : '5px 0 0 5px'}}>
                <i className="sbicon" onClick={() => action(id,"export_options","test1")}>
                  <Icons id="window_side_bar" type="export" condition="True"/>
                </i>
                <div className="side_bar_btn_options"
                  style={{
                    display: isSideBarMenuOpen === 'export_options' ? 'block' : 'none'}}>
                  <ul>
                    <label>
                      <li onClick={() => action(id,"export_options","test2")}>      
                        {/*<i>
                          <Icons id="window_side_bar" type="live_source" condition="True"/>
                        </i> */}             
                        <span>Export JSON</span>
                      </li>
                    </label>
                  </ul>
                </div>
              </label>                
            </div>
            <div className='graph_container'>
              <IframeEmbed id="graph"/>
            </div>
            <div className='properties_container'>
                <WindowVerticalSplitPanels
                  initialTopHeight={'80%'}
                  minTopHeight={'20%'}
                  maxTopHeight={'100%'}
                />
           </div>
           <div className='window_footer'>To zoom on the graph 1. click on the graph 2. toggle capslock button. &nbsp; Right click on the graph to save as Image.<span><b>Window Id : </b><i>{id}</i></span></div>
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
    // openWindows('source', 'upload');
    // openWindows('graph', 'upload');
    //openWindows('chart', 'upload');
  }, []);
  useEffect(() => {
    console.log("Current windows:", windows);
  }, [windows]);  
  return (
    <main id='main'>
    </main>
  );
}
function Root() {
  const [windows, setWindows] = useState([]);
  const [isToggleMenuOpen, setIsToggleMenuOpen] = useState(false);
  const [loadscreenState, setloadscreenState] = useState(false);
  const [loadscreenText, setloadscreenText] = useState('');
  const [isSideBarMenuOpen, setIsSideBarMenuOpen] = useState(false);
  const [selectedContent, setSelectedContent] = useState(null); // content to show inside the windows
  const [selectedSubContent, setSelectedSubContent] = useState(null); // content to show inside the windows
  const [batchFilesSearchResults, setBatchFilesSearchResults] = useState({ results: [], message: "" });
  const [searchResultsVisible, setSearchResultsVisible] = useState(null);
  const [searchPlaceholder, setSearchPlaceholder] = useState('');
  const [batchFilesDataframeInfoI, setBatchFilesDataframeInfoI] = useState('');
  const [batchFilesDataframeInfoII, setBatchFilesDataframeInfoII] = useState(null);
  const debounceRef = useRef(null);
  const [zIndexCounter, setZIndexCounter] = useState(1); // zIndex counter
  const windowIdRef = useRef(0); // Added

  useEffect(() => {
    console.log("Windows state updated:", windows);
  }, [windows]);
//-------- Storage search result container visiblity
  useEffect(() => {
    console.log("batchFilesSearchResults changed:", batchFilesSearchResults);
    if (batchFilesSearchResults.results) {
      console.log("Setting searchResultsVisible to true");
      setSearchResultsVisible(true);
      console.log("on")
    } else {
      console.log("Setting searchResultsVisible to false");
      setSearchResultsVisible(false);
    }
  }, [batchFilesSearchResults]);
  useEffect(() => {
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
          console.log("off",searchResultsVisible)
        }
      } else {
        console.log('Container not found');
      }
    };
    //document.addEventListener("click", handleClickOutside);
    return () => {
      document.removeEventListener("click", handleClickOutside);
    };
  }, []);
  useEffect(() => {
    console.log("searchResultsVisible changed:", searchResultsVisible);
  }, [searchResultsVisible]);
//-------------------------------------------------------------------------------------          
  
  const generateWindowId = () => {
    windowIdRef.current += 1;
    return windowIdRef.current.toString();
  };
  const handleCreateWindow = (type) => {
    const id = generateWindowId();
    setWindows(prev => [...prev, { id, type, zIndex: zIndexCounter, selectedContent: null, selectedSubContent: null, formData: {}, formIResponse: null, formToolResponse: null, batchFilesSearchResults:null}]);
    setZIndexCounter(prev => prev + 1);
  };
  const Loadscreen = ({ loadingText }) => {
    baseText= loadingText;
    const [dotCount, setDotCount] = useState(0);
    useEffect(() => {
      const interval = setInterval(() => {
        setDotCount(prev => (prev + 1) % 4); // cycle through 0,1,2,3 dots
      }, 500); // change every 500ms

      return () => clearInterval(interval);
    }, []);
    setsetloadscreenState(true)
  };
  const openWindows = (type, source) => {
    setWindows(prev => {
      const id = generateWindowId();
      const maxZ = Math.max(0, ...prev.map(w => w.zIndex));
      return [...prev, { id, type, source, zIndex: maxZ + 1, selectedContent: null, selectedSubContent: null, formData: {}, formIResponse: null, formToolResponse: null, batchFilesSearchResults:null}]
    });
  };
  const handleOpenWindow = (id, type) => {
    setWindows(prev => {
      if (prev.find(w => w.id === id)) return prev;
      return [...prev, { id, type, zIndex: zIndexCounter }];
    });
    setZIndexCounter(prev => prev + 1);
  };
  const handleCloseWindow = (id) => {
    setWindows(prev => prev.filter(w => w.id !== id));
  };
  const handleMoveWindow = (id, newPos) => {
    setWindows(prev =>
      prev.map(w => (w.id === id ? { ...w, position: newPos } : w))
    );
  };
  // --- Windows actions ---
  const handleWindowActions = (id, menuId, action, payload) => {
    setIsSideBarMenuOpen(prev => prev === menuId ? null : menuId); // Toggle open/close

    setWindows(prev =>
      prev.map(w => {
        if (w.id !== id) return w;

        console.log("Updating window:", id, "for menu:", menuId, "action:", action);

        let loadscreenState = w.loadscreenState;
        let newContent = w.selectedContent;
        let newSubContent = w.selectedSubContent;
        let newBatchSearchResult = w.batchFilesSearchResults;
        let newBatchFilesCollection = w.batchFilesCollection || []; // Ensure selectedFiles is initialized

        //Source window
        if (menuId === "live_source_options" && action === "update") {
          newContent = "live_source_options"; // This will show live source UI
        }
        if (menuId === "live_source_options_passive" && action === "update") {
          newContent = "live_source_options";
        }
        if (menuId === "batch_input" && action === "update") {
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageI";
          //console.log("here", newSubContent); // Debugging
        }
        if (menuId === "batch_input_form" && action === "connect" && payload) {
          // Async logic (outside setWindows)
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, formIResponse: "Connecting..." } : w
            )
          );
          fetch("http://localhost:5000/connect_to_source", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          })
            .then((res) => res.json())
            .then((data) => {
              console.log("message:",data.message)
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, formIResponse: data.message } : w
                )
              );
            })
            .catch((err) => {
              console.error(err);
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, formIResponse: "Connection failed!" } : w
                )
              );
            });        
        }
        if (menuId === "batch_input_form" && action === "disconnect" && payload) {
          // Async logic (outside setWindows)
          console.log("formData:", payload)
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, formIResponse: "Disconnecting..." } : w
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
                  w.id === id ? { ...w, formIResponse: data.message } : w
                )
              );
            })
            .catch((err) => {
              console.error(err);
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, formIResponse: "Disconnecting failed!" } : w
                )
              );
            });        
        }
        if (menuId === "tool_integration_form" && action === "connect" && payload) {
          console.log("toodata:", payload);
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
          console.log("toodata:", payload);
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
          //newContent = "batch_input";
          newSubContent = "batch_input_form_pageI";
          //console.log("here", newSubContent); // Debugging
        }
        if (menuId === "batch_input_form_swap" && action === "page_II") {
          console.log("called")
          //newContent = "batch_input";
          newSubContent = "batch_input_form_pageII";
          //console.log("here", newSubContent); // Debugging
        }
        if (menuId === "batch_files_search_input" && action === "search") {
          // Clear previous debounce timeout
          if (debounceRef.current) {
            clearTimeout(debounceRef.current);
          }

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
          setWindows(prev =>
            prev.map(w =>
             w.id === id ? { ...w, loadscreenState: true } : w
            )
          );
          alert("requesting dataframe")
          //Requesting dataframe creation
          // Inside your event handler, mark the function async:
          const handleEvent = async () => {
            if (menuId === "batch_input_form_swap" && action === "page_III") {
              // your code...
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, loadscreenState: true } : w
                )
              );
              alert("requesting dataframe");

              const payload = {
                id: "create_DF",
                value: newBatchFilesCollection,
              };

              const handleFetchResponse = async () => {
                const response = await fetch("http://localhost:5000/live_batch_files", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify(payload),
                });
                const data = await response.json();
                if (data.message === "success!") {
                  alert(data.results);
                  const resultData = data.results;
                  const htmlContent = (
                    <table id="batch_files_dataframe_infos_left_table" cellPadding='0'>
                      <tbody>
                        <tr>
                          <td>Source files1</td>
                          <td>
                            <ul>
                              <li>batch_connection_form_container .batch_files_dataframe_form .batch_files_dataframe_form_fieldset </li>
                              <li>batch_connection_form_container .batch_files_dataframe_form .batch_files_dataframe_form_fieldset </li>
                              <li>batch_connection_form_container .batch_files_dataframe_form .batch_files_dataframe_form_fieldset </li>
                            </ul>
                          </td>
                        </tr>
                        <tr>
                          <td>Total rows1</td>
                          <td>{resultData.num_rows}</td>
                        </tr>
                        <tr>
                          <td>Total columns1</td>
                          <td>{resultData.num_columns}</td>
                        </tr>
                      </tbody>
                    </table>
                  );
                  setBatchFilesDataframeInfoI(htmlContent);
                  newSubContent = "batch_input_form_pageIII";
                  console.log("updatedresponse:",batchFilesDataframeInfoI)
                } else {
                  alert("requestID8234692 failed!");
                  newSubContent = "batch_input_form_pageII";
                }
                setWindows(prev =>
                  prev.map(w =>
                    w.id === id ? { ...w, loadscreenState: false,batchFilesDataframeInfoI:batchFilesDataframeInfoI, selectedContent: newContent, selectedSubContent: newSubContent } : w
                  )
                );
              };

              await handleFetchResponse();
            }
          };
          handleEvent();

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
          loadscreenState: loadscreenState
        };
      })
    );
  };

  // --- Bring window to front ---
  const bringToFront = (id) => {
    setWindows(prev => {
      const maxZ = Math.max(...prev.map(w => w.zIndex));
      return prev.map(w =>
        w.id === id ? { ...w, zIndex: maxZ + 1 } : w
      );
    });
  };
  // --- Navigation Menu Actions ---
  const handleNavAction = (action) => {
    //alert("nav_menu")
    // handle specific nav actions here
  };
  // --- Toggle Menu ---
  const handleToggleMenu = (id) => {
    setIsToggleMenuOpen(prev => !prev);
    if(id=="toggle_menu_new_source_window"){
      openWindows("source","");
    }
    else if(id=="toggle_menu_new_graph_window"){
      openWindows("graph","");
    }
    else if(id=="toggle_menu_new_chart_window"){
      openWindows("chart","");
    }
    else if(id=="toggle_menu_new_tabel_window"){
      openWindows("table","");
    }
    else{
      return null;
    }
  };
  return (
    <div style={{ position: 'relative', minHeight: '100vh', overflow: 'hidden' }}>
      <NetworkBackground />
      <div style={{ position: 'absolute',top:0,bottom:0,left:0,right:0,zIndex: 1 }}> 
        <NavBar onNavAction={handleNavAction}/>
        <ToggleMenu onToggle={handleToggleMenu} isToggleMenuOpen={isToggleMenuOpen} action={handleToggleMenu}/>
        <Main windows={windows} setWindows={setWindows} openWindows={openWindows}/>      
        <Taskbar />
        {/* Render windows */}
        {windows.map(window => (
          <Windows
            key={window.id}
            id={window.id}
            {...window}
            type={window.type}
            isToggleMenuOpen={isToggleMenuOpen}
            isSideBarMenuOpen={isSideBarMenuOpen}
            loadscreenState={window.loadscreenState}
            loadscreenText={window.loadscreenText}
            action={handleWindowActions}
            selectedContent={window.selectedContent}
            selectedSubContent={window.selectedSubContent}
            formIResponse={window.formIResponse}
            formIIResponse={window.formIIResponse}
            formToolResponse={window.formToolResponse}
            batchFilesSearchResults={window.batchFilesSearchResults}
            searchResultsVisible={window.searchResultsVisible}
            searchPlaceholder={window.searchPlaceholder}
            batchFilesCollection={window.batchFilesCollection}
            batchFilesDataframeInfoI={window.batchFilesDataframeInfoI}
            batchFilesDataframeInfoII={window.batchFilesDataframeInfoII}
            onClose={handleCloseWindow}
            onMove={handleMoveWindow}
            zIndex={window.zIndex}
            onFocus={() => bringToFront(window.id)}
          />
        ))}
      </div>
    </div>
  )
}

export default Root;


