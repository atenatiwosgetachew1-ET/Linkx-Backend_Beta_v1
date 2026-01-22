import React, { useState, useRef, useEffect, createContext, useContext, useCallback } from 'react';
import { io } from 'socket.io-client';

import './main.css'
import NetworkBackground from './networkAnimation.jsx'
//importing the icons function
import Icons from './Icons.jsx'
// importing action functions
// import ToggleMenuActions from './ToggleMenuActions.jsx'
// import NavBarActions from './NavBarActions.jsx'
// import WindowsActions from './WindowsActions.jsx'

//window.clipboardBuffer = [];
const clipboard = [];

window.addEventListener("message", e => {
  if (e.data.type === "clipboard_get") {
    e.source.postMessage(
      { type: "clipboard_data", payload: clipboard },
      e.origin
    );
  }

  if (e.data.type === "clipboard_set") {
    clipboard.length = 0;
    clipboard.push(...e.data.payload);
  }
});


function ToggleMenu({ onToggle, isToggleMenuOpen, toggleAction, isMaximized, windows, orientation}){
  return(
  <div id='toggle_menu' style={{width: isToggleMenuOpen ? '15vw' : '0vw', zIndex: isToggleMenuOpen ? '99999':'', visibility: orientation === "windows" || windows.length == 0 || isToggleMenuOpen ? 'visible':'hidden'}}>
      <div id="toggle_main_list">
        <div className="animated_logo">
          <span onClick={onToggle}>
            <i>
              <a></a>
            </i>
          </span>
          <label>Linkx | <i>Web Analyzer</i></label>
        </div>
        <div className="animated_windows_taskbar" onClick={() => toggleAction("windows_taskbar")}>
          <span>&#10070;</span>
        </div>
      </div>
      <div id="toggle_side_list">
        <div className="toggle_side_list_container">
          <div className="toogle_side_list_items" 
            style={{
              display: isToggleMenuOpen ? 'block' : 'none'}}>
              <ul>
                <div className="toogle_side_list_menu_container">  
                  <li onClick={() => toggleAction("toggle_menu_new_source_window")}>    
                    {/*<i>
                      <Icons id="toggle_menu" type="source_window" condition="True"/>
                    </i> */}  
                    <span>&#10011; &nbsp;Source window</span>
                  </li>
                  <li onClick={() => toggleAction("toggle_menu_new_graph_window")}>    
                    {/*<i>
                      <Icons id="toggle_menu" type="garph_window" condition="True"/>
                    </i>  */}            
                    <span>&#10011; &nbsp;Graph window</span>
                  </li>
                  {/*<li onClick={() => toggleAction("toggle_menu_new_chart_window")}>    
                    <i>
                      <Icons id="toggle_menu" type="chart_window" condition="True"/>
                    </i>              
                    <span>&#10011; &nbsp;Chart window</span>
                  </li>
                  <li onClick={() => toggleAction("toggle_menu_new_table_window")}>    
                    <i>
                      <Icons id="window_side_bar" type="tabular_window" condition="True"/>
                    </i>             
                    <span>&#10011; &nbsp;Tabular window</span>
                  </li>*/}
                </div>
                <div className="toogle_side_list_options_container">                    
                  <li onClick={() => toggleAction("configurations")}>    
                    {/*<i>
                      <Icons id="window_side_bar" type="tabular_window" condition="True"/>
                    </i> */}             
                    <span>&#9881; &nbsp;Configurations</span>
                  </li>
                  <li>    
                    {/*<i>
                      <Icons id="window_side_bar" type="tabular_window" condition="True"/>
                    </i> */}             
                    <span>&#9715; &nbsp;Orientation</span>
                    <div className="toggle_btn_conatiner" onClick={() => toggleAction("toggle_menu_orientation")}>
                      <span className={`${orientation==="tabs" ? '' : 'active'}`}>Float</span><span className={`${orientation==="tabs" ? 'active' : ''}`}>Tabs</span>
                    </div>
                  </li>
                  <li>    
                    {/*<i>
                      <Icons id="window_side_bar" type="tabular_window" condition="True"/>
                    </i> */}             
                    <span>&#9732; &nbsp;Mood</span>
                    <div className="toggle_btn_conatiner">
                      <span className="active">Day</span><span>Night</span>
                    </div>
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
function Taskbar({ windows, isTaskBarOpen, activeWindowId, focusWindow, toggleAction, isCtrlHeld}) {
  return (
    <div id="windows_taskbar_container" style={{ display: isTaskBarOpen || isCtrlHeld ? 'block' : 'none' }} onClick={() => toggleAction("windows_taskbar")}>       
      <div className="windows_taskbar_lists">
        <div className="windows_taskbar_menu">
          {/*<span onClick={() => toggleAction("windows_taskbar")}>x</span>*/}
          <label>Task bar</label>
        </div>
        {windows.map(w => (
          <span
            key={w.id}
            style={{backgroundImage: activeWindowId === w.id ? 'url(/thumbnails/windows_thumbnail_active.png)' : 'url(/thumbnails/windows_thumbnail_passive.png)'}}
            onMouseEnter={() => focusWindow(w.id)}>
            <label>{`${w.type.charAt(0).toUpperCase()}${w.type.slice(1)} Window`}</label>
            <b>{` ${w.id}`}</b>
          </span>
        ))}
      </div>
    </div>
  );
}
function Configurations({sessionId,actions,loadscreenState,setloadscreenState,toggleAction,configurations,isConfigurationsOpen}) {
  const [remote, setRemote] = useState(false);
  const [automation, setAutomation] = useState(false);
  const [parsedConfig, setParsedConfig] = useState(null);

  useEffect(() => {
    console.log("configurations_window:",configurations)
    if (configurations) {
      // configurations may be a {value: string} object or already parsed
      let cfg;
      if (configurations.value) {
        cfg = JSON.parse(configurations.value.replace(/'/g, '"'));
      } else {
        cfg = configurations;
      }
      setParsedConfig(cfg);      
      setRemote(cfg.remote === "true" || cfg.remote === true);
      setAutomation(cfg.automation === "true" || cfg.automation === true);
      setloadscreenState(false);
    }
  }, [configurations]);



  return (
    <div
      id="configurations_container"
      style={{ display: isConfigurationsOpen ? "block" : "none" }}
    >
      <div className="configurations_options_container">
        <div className="configurations_options_container_bar">
          <span onClick={() => toggleAction("configurations")}>x</span>
          <label>Configurations</label>
        </div>

        <div className="configurations_options">
          <div
            id="configurations_loadscreen"
            className="windows_loadscreen"
            style={{ display: loadscreenState ? "block" : "none" }}
          >
            <Loadscreen loadingText="Loading" />
          </div>

          <form
            onSubmit={(e) => {
              e.preventDefault();
              const formData = new FormData(e.target);
              actions("save", formData);
            }}
          >
            {/* ───────── Left Panel ───────── */}
            <div className="configurations_options_panel">
              {/* Broker Configuration */}
              <fieldset>
                <legend>Broker configuration</legend>

                <label>IP address</label>
                <select
                  id="config_kafka_addresses"
                  name="active_kafka_adress"
                  value={parsedConfig?.active_kafka_adress || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                >
                  <option disabled>Custom</option>
                  {parsedConfig?.kafka_addresses?.map((addr, idx) => (
                    <option key={idx} value={addr}>{addr}</option>
                  ))}
                </select>

                <input
                  type="text"
                  name="kafka_custom_address"
                  className="input_text"
                  placeholder="Kafka / API address"
                  value={parsedConfig?.kafka_custom_address || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />
              </fieldset>

              {/* Storage Configuration */}
              <fieldset>
                <legend>Storage configuration</legend>

                <label>IP address</label>
                <select
                  name="active_storage_address"
                  value={parsedConfig?.active_storage_address || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                >
                  <option disabled>Custom</option>
                  {parsedConfig?.storage_addresses?.map((addr, idx) => (
                    <option key={idx} value={addr}>{addr}</option>
                  ))}
                </select>

                <input
                  type="text"
                  name="storage_custom_address"
                  className="input_text"
                  placeholder="HDFS address"
                  value={parsedConfig?.storage_custom_address || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>Ports</label>
                <table>
                  <tbody>
                    <tr>
                      <td>
                        Hadoop RPC
                        <input
                          type="text"
                          className="subinput"
                          name="hadoop_rcp_port"
                          placeholder="Hadoop port"
                          value={parsedConfig?.hadoop_rcp_port || ""}
                          onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                        />
                      </td>
                      <td>
                        Hadoop Web
                        <input
                          type="text"
                          className="subinput"
                          name="hadoop_web_port"
                          placeholder="UI port"
                          value={parsedConfig?.hadoop_web_port || ""}
                          onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                        />
                      </td>
                    </tr>
                    <tr>
                      <td>
                        Spark port
                        <input
                          type="text"
                          className="subinput"
                          name="spark_port"
                          placeholder="Spark port"
                          value={parsedConfig?.spark_port || ""}
                          onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                        />
                      </td>
                      <td>
                        Hive port
                        <input
                          type="text"
                          className="subinput"
                          name="hive_port"
                          placeholder="Hive port"
                          value={parsedConfig?.hive_port || ""}
                          onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                        />
                      </td>
                    </tr>
                    <tr>
                      <td>
                        API port
                        <input
                          type="text"
                          className="subinput"
                          name="api_port"
                          placeholder="API port"
                          value={parsedConfig?.api_port || ""}
                          onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                        />
                      </td>
                    </tr>
                  </tbody>
                </table>

                <label>API search Endpoint</label>
                <input
                  type="text"
                  name="search_api_endpoint"
                  className="input_text"
                  placeholder="API search Endpoint"
                  value={parsedConfig?.search_api_endpoint || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>Database</label>
                <select
                  name="active_storage_database"
                  value={parsedConfig?.active_storage_database || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                >
                  <option disabled>Custom</option>
                  {parsedConfig?.storage_databases?.map((db, idx) => (
                    <option key={idx} value={db}>{db}</option>
                  ))}
                </select>

                <input
                  type="text"
                  name="storage_database_custom"
                  className="input_text"
                  placeholder="Database name"
                  value={parsedConfig?.storage_database_custom || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>Storage path</label>
                <textarea
                  name="storage_path"
                  className="input_textarea"
                  placeholder="Storage base path"
                  value={parsedConfig?.storage_path || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>Tables (Separate with comma)</label>
                <textarea
                  name="storage_tables"
                  className="input_textarea"
                  placeholder="Tables lists (Separated with comma)"
                  value={Array.isArray(parsedConfig?.storage_tables) ? parsedConfig.storage_tables.join(", ") : ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />
              </fieldset>

              {/* Notes */}
              <fieldset>
                <legend>Note</legend>
                <label>* Any modifications apply only to next instances.</label>
                <label>* Removing a rule will delete it permanently.</label>
                <label>* Saving is required to add new rules.</label>
                <label>* Remember to backup.</label>
              </fieldset>
            </div>

            {/* ───────── Right Panel ───────── */}
            <div className="configurations_options_panel">
              {/* Tool Configuration */}
              <fieldset>
                <legend>Tool configuration</legend>

                <label>Tool</label>
                <select
                  name="active_tool"
                  className="input_text"
                  value={parsedConfig?.active_tool || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                >
                  <option disabled>Custom</option>
                  {parsedConfig?.tools?.map((tool, idx) => (
                    <option key={idx} value={tool}>{tool}</option>
                  ))}
                </select>

                <input
                  type="text"
                  name="active_tool_url"
                  className="input_text"
                  placeholder="Url address"
                  value={parsedConfig?.active_tool_url || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>Ports</label>
                <table>
                  <tbody>
                    <tr>
                      <td>
                        Protocol Port
                        <input
                          type="text"
                          className="subinput"
                          name="tool_protocol_port"
                          placeholder="Tool port"
                          value={parsedConfig?.tool_protocol_port || ""}
                          onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                        />
                      </td>
                      <td>
                        Web UI Port
                        <input
                          type="text"
                          className="subinput"
                          name="tool_web_port"
                          placeholder="UI port"
                          value={parsedConfig?.tool_web_port || ""}
                          onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                        />
                      </td>
                    </tr>
                  </tbody>
                </table>

                <label>Credentials</label>
                <label>Username and Password</label>
                <input
                  type="text"
                  name="active_tool_username"
                  className="input_text"
                  placeholder="Username"
                  value={parsedConfig?.active_tool_username || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />
                <input
                  type="text"
                  name="active_tool_password"
                  className="input_text"
                  placeholder="Password"
                  value={parsedConfig?.active_tool_password || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>Database</label>
                <select
                  name="active_tool_database"
                  className="input_text"
                  value={parsedConfig?.active_tool_database || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                >
                  <option disabled>Custom</option>
                  {parsedConfig?.active_tool_database && (
                    <option value={parsedConfig.active_tool_database}>{parsedConfig.active_tool_database}</option>
                  )}
                </select>

                <input
                  type="text"
                  name="custom_tool_database"
                  className="input_text"
                  placeholder="Database name"
                  value={parsedConfig?.custom_tool_database || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>Tables (Separate with comma)</label>
                <textarea
                  name="active_tool_tables"
                  className="input_textarea"
                  placeholder="Tables lists (Separated with comma)"
                  value={Array.isArray(parsedConfig?.active_tool_tables) ? parsedConfig.active_tool_tables.join(", ") : ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />
              </fieldset>

              {/* Analysis Rules */}
              <fieldset>
                <legend>Analysis Rules</legend>
                <label>Active rule</label>
                <select
                  name="active_rule"
                  className="input_text"
                  value={parsedConfig?.active_rule?.[0] || ""}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                >
                  {parsedConfig?.rule_names?.map((rule, idx) => (
                    <option key={idx} value={rule}>{rule}</option>
                  ))}
                </select>

                <input
                  type="button"
                  className="critical_btns"
                  value="Remove"
                  onClick={(e) => actions("remove", { name: e.target.name, value: e.target.value })}
                />

                <label>Upload rules (JSON)</label>
                <input
                  id="rule_to_upload"
                  type="file"
                  className="input_file"
                  accept=".json"
                  name="rule_file"
                />

                <input
                  type="text"
                  name="rule_name"
                  className="input_text"
                  placeholder="Rule name"
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.value })}
                />

                <label>
                  Get latest sample <a href="public/temp_rules/Linkx_Rules_Template.zip" download> Template</a>
                </label>
              </fieldset>

              {/* Miscellaneous */}
              <fieldset>
                <legend>Miscellaneous</legend>
                <input
                  type="checkbox"
                  id="Automated_across_service"
                  className="input_checkbox"
                  name="automation"
                  checked={!!parsedConfig?.automation}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.checked })}
                />
                <label htmlFor="Automated_across_service" className="sublabel">Automate across services</label>

                <input
                  type="checkbox"
                  id="remote_requests"
                  className="input_checkbox"
                  name="remote"
                  checked={!!parsedConfig?.remote}
                  onChange={(e) => actions("change", { name: e.target.name, value: e.target.checked })}
                />
                <label htmlFor="remote_requests" className="sublabel">Remote requests</label>
              </fieldset>

              {/* Buttons */}
              <fieldset>
                <span className="action_btns" onClick={() => actions("load_default")} title="Reset">⟳ Reset</span>
                <a
                  className="action_btns"
                  href={`public/temp_config/${sessionId}_temp_config.JSON`}
                  download
                  title="Download"
                >⭳ Export</a>
                <button className="action_btns" type="submit" title="Save Configurations">🖫 Save</button>

                <input
                  type="file"
                  id="import_config_file"
                  name="import_config_file"
                  style={{ display: "none" }}
                  onChange={() => actions("upload")}
                  accept=".json"
                />
                <span
                  className="action_btns"
                  onClick={() => document.getElementById("import_config_file").click()}
                  title="Upload Configuration file"
                >&#128448;  Import</span>
              </fieldset>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
function WindowVerticalSplitPanels({id, type, sourceId, initialTopHeight, minTopHeight, maxTopHeight, graphStatus, activeGraph, graphAction, iframeRef, iframeFilters, iframeSettings, selectedPropertyTab, nodeProperties, filterPropertyKeys, filterResults}) {
  const containerRef = useRef(null);
  const [containerHeight, setContainerHeight] = useState(0);
  const [topHeightPx, setTopHeightPx] = useState(0);
  const isDragging = useRef(false);
  const minHeightPx = useRef(0);
  const maxHeightPx = useRef(0);

  const settings = iframeSettings[id];
  console.log("settings:",settings[5],"id:",id)

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
  useEffect(() => {
    if (!filterPropertyKeys) {
      graphAction(id, "properties_tab", "filter_keys", {
        iframe: iframeRef,
        filter: "all_property_keys",
        state: ""
      });
    }
  }, []);   // run only once
  useEffect(() => {
    if (!selectedPropertyTab) {
      graphAction(id, "properties_tab", "switch_tab", "graph_filters");
    }
  }, [id, selectedPropertyTab, graphAction]);


  //alert(filterPropertyKeys)
  const tabGroupName = `${type}_window_${id}_ppt_tab_radio`;

  // End result
  return (
    <div ref={containerRef} className="reference_container">
      {/* Top Panel */}
      <div className="top_panel" style={{height: topHeightPx}}>
        <div className="ppt_tabs_container">
          <input id={`${type}_window_${id}_filters_tab_radio`} name={tabGroupName} type="radio" checked={selectedPropertyTab === "graph_filters"} onChange={(e) => {graphAction(id, "properties_tab", "switch_tab", "graph_filters");}}/>
          <label htmlFor={`${type}_window_${id}_filters_tab_radio`} className="ppt_tabs" title="Graph filters">
            <i>
              <Icons id="properties_container" type="filter" condition="True"/>
            </i>
            <span>Filters</span>
          </label>
          <input id={`${type}_window_${id}_infos_tab_radio`}  name={tabGroupName} type="radio" checked={selectedPropertyTab === "graph_infos"} onChange={(e) => {graphAction(id, "properties_tab", "switch_tab", "graph_infos");}}/>
          <label htmlFor={`${type}_window_${id}_infos_tab_radio`} className="ppt_tabs" title="Nodes informations">
            <i>
              <Icons id="properties_container" type="info" condition="True"/>
            </i>
            <span>Infos</span>
          </label>
          <input id={`${type}_window_${id}_settings_tab_radio`} name={tabGroupName} type="radio" checked={selectedPropertyTab === "graph_settings"} onChange={(e) => {graphAction(id, "properties_tab", "switch_tab", "graph_settings");}}/>
          <label htmlFor={`${type}_window_${id}_settings_tab_radio`} className="ppt_tabs" title="Graph settings">
            <i>
              <Icons id="properties_container" type="settings" condition="True"/>
            </i>
            <span>Settings</span>
          </label>        
        </div>
        <div className="ppt_tabs_body_container">
          {selectedPropertyTab === "graph_filters" &&(
          <div id={`${type}_window_${id}_${iframeRef}_graph_filters`} className="graph_filters_container">
            <form className="filter_form"
                  onSubmit={(e) => {e.preventDefault();
                    const keyword = document.getElementById(
                      `${type}_window_${id}_${iframeRef}_graph_filters_input`
                    ).value;
                    const linkedOption = document.getElementById(
                      `${type}_window_${id}_${iframeRef}_graph_filters_option_input`
                    ).checked;
                    const keys = Array.from(
                      document.getElementsByName(`${type}_window_${id}_${iframeRef}_graph_filters_key`)
                    )
                      .filter(el => el.checked)
                      .map(el => el.value);
                    graphAction(id, "properties_tab", "filter_search", {
                      iframe: iframeRef,
                      keyword,
                      option: linkedOption,
                      keys,
                      settings: settings
                    });
                  }}
                >
              <label className="filter_form_header_label">Search <i><b>Note :</b> Make sure an attribute name is selected for more specific results.</i></label>
              <div className="filter_form_search_container">
                <input id={`${type}_window_${id}_${iframeRef}_graph_filters_input`} type="text" placeholder="Type here to seach"/>
                <button title="Search"><Icons id="properties_container" type="search" condition="True"/></button>
                <div className="filter_form_search_options_container">
                  <input id={`${type}_window_${id}_${iframeRef}_graph_filters_option_input`} type="checkbox"/>
                  <label htmlFor={`${type}_window_${id}_${iframeRef}_graph_filters_option_input`}>Include linked nodes</label>
                </div>
                  <div className="filterPropertyKeys">       
                  {filterPropertyKeys && (
                    <div>
                      {filterPropertyKeys.map((key, index) => (
                        <span key={index}>
                          <input id={`${type}_window_${id}_${iframeRef}_graph_filters_attribute_checkbox_${index}`} type="checkbox" value={key} name={`${type}_window_${id}_${iframeRef}_graph_filters_key`} />
                          <label htmlFor={`${type}_window_${id}_${iframeRef}_graph_filters_attribute_checkbox_${index}`}>{key}</label>
                        </span>
                      ))}
                    </div>
                  )}
                  </div>
                  <div className="filter_results">
                    <h4>Search Results</h4>
                    <span>
                      Nodes found <b>{filterResults ? filterResults.nodes.length : 0}</b>
                    </span>
                    <span>
                      Edges found <b>{filterResults ? filterResults.edges.length : 0}</b>
                    </span>
                  </div>                                                 
              </div>
            </form>
          </div>
          )}
          {selectedPropertyTab === "graph_infos" && (
            <div
              id={`${type}_window_${id}_${iframeRef}_graph_infos`}
              className="graph_infos_container"
            >
              <form className="infos_form">
                <label className="infos_form_header_label">Informations <i><b>Note :</b> Make sure to check the 'edit Informations' checkbox in Settings inorder to modify node properties.</i></label>
                {nodeProperties ? (
                  <div id="graph_infos" className="graph_options_infos">
                    <div>
                      <span><label>Key</label></span>
                      <span>Value</span>
                    </div>
                    {Object.entries(nodeProperties).map(([key, value]) => (
                      <div key={key}>
                        <span title={String(key)}><label>{key}</label></span>
                        <span title={String(value)}>{String(value)}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                   <p>No Information to show.</p>
                )}
            </form>
            </div>
          )}
          {selectedPropertyTab === "graph_settings" &&(
          <div id={`${type}_window_${id}_${iframeRef}_graph_settings`} className="graph_settings_container">
            <form className="settings_form" onSubmit={(e) => { e.preventDefault();}}>
              <label className="setting_form_header_label">Settings <i><b>Note :</b> Analysis time settings are not stored.</i></label>
              <div className="settings_form_div_firstChild">
                <label>Limit Nodes</label>
                {/* Key Selector */}
                {filterPropertyKeys && (
                  <select
                    className="select_option"
                    value={settings[0] || ""}
                    onChange={(e) => {
                      const payload = { key: e.target.value, sort: String(settings[1]) || "asc", amount: parseInt(settings[2]) || 25 };
                      graphAction(id, "properties_tab", "settings", {
                        iframe: iframeRef,
                        settings: "limit_nodes_key",
                        state: payload,
                      });
                    }}
                  >
                    {filterPropertyKeys.map((key) => (
                      <option key={key} value={key}>
                        {key}
                      </option>
                    ))}
                  </select>
                )}
                {/* Sort Selector */}
                <select
                  className="select_option"
                  style={{ width: "4vw" }}
                  value={settings[1] || "asc"}
                  onChange={(e) => {
                    const payload = { key: String(settings[0]) || "", sort: e.target.value, amount: parseInt(settings[2]) || 25 };
                    graphAction(id, "properties_tab", "settings", {
                      iframe: iframeRef,
                      settings: "limit_nodes_sort",
                      state: payload,
                    });
                  }}
                >
                  <option value="asc">Asc</option>
                  <option value="desc">Desc</option>
                </select>
                {/* Amount Input */}
                <input
                  className="input_text"
                  id="graph_settings_limit"
                  type="number"
                  min="1"
                  max="300"
                  placeholder="Limit"
                  value={settings[2] || 25}                
                  onChange={(e) => {
                    const payload = { key: String(settings[0]) || "", sort: String(settings[1]) || "asc", amount: e.target.value };
                    graphAction(id, "properties_tab", "settings", {
                      iframe: iframeRef,
                      settings: "limit_nodes_amount",
                      state: payload,
                    });
                  }}
                />
              </div>
              <div className="settings_form_div">
                <label className="input_labels">Label nodes with</label>
                {filterPropertyKeys && (
                  <select
                    className="select_option"
                    value={settings[11]?.key || ""}
                    onChange={(e) => {
                      graphAction(id, "properties_tab", "settings", {
                        iframe: iframeRef,
                        settings: "label_nodes_with",
                        state: { labelkey: e.target.value, filterKey: settings[0],filterSort: settings[1], limitAmount: settings[2]},
                      });
                    }}
                  >
                    {filterPropertyKeys.map((key) => (
                      <option key={key} value={key}>
                        {key}
                      </option>
                    ))}
                  </select>
                )}
              </div>
              <div className="settings_form_div">                
                <label className="input_labels">Weight Edges</label>                
                <select className="select_option" value={settings[3] ? (settings[3]):("")} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "weight_edges",
                    state: e.target.value,
                  })}}>
                  <option value="">Off</option>
                  <option value="true">On</option>
                </select>  
              </div>
              <div className="settings_form_div">                
                <label className="input_labels">Show Titles</label>
                <select className="select_option" value={settings[4] ? (settings[4]):("")} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "show_title",
                    state: e.target.value,
                  })}}>
                  <option value="">Off</option>
                  <option value="true">On</option>
                </select>
              </div>
              <div className="settings_form_div">                
                <label className="input_labels">Show Labels</label>                
                <select className="select_option" value={settings[5] ? (settings[5]):("")} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "show_label",
                    state: e.target.value,
                  })}}>
                  <option value="">Off</option>
                  <option value="true">On</option>
                </select>
              </div>
              <div className="settings_form_div">                
                <label className="input_labels">Edit Informations</label>                
                <select disabled className="select_option" value={settings[6] ? (settings[6]):("")} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "edit_infos",
                    state: e.target.value,
                  })}}>
                  <option value="">Off</option>
                  <option value="true">On</option>
                </select>
              </div>
              <div className="settings_form_div">                
                <label className="input_labels">Graph Physics</label>                
                <select className="select_option" value={settings[7] ? (settings[7]): ("")} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "graph_physics",
                    state: e.target.value,
                  })}}>
                  <option value="">Off</option>
                  <option value="true">On</option>
                </select>                
              </div>
              <div className="settings_form_div">
                <label className="input_labels">Layout type</label>
                <select className="select_option" value={settings[8] ? (settings[8]):("default")} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "layout_type",
                    state: e.target.value,
                  })}}>
                  <option value="default">Default</option>
                  <option value="hierarchical">hierarchical</option>
                </select>                
              </div>
              <div className="settings_form_div">
                <label className="input_labels">Layout direction</label>
                <select className="select_option" value={settings[8] === "hierarchical" && settings[9] ? settings[9]:"UD"} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "layout_direction",
                    state: e.target.value,
                  })}}
                  disabled={settings[8] !== "hierarchical"}>
                  <option value="UD">Up-Down</option>
                  <option value="LR">Left-Right</option>
                </select>                
              </div>
              <div className="settings_form_div">
                <label className="input_labels">Sort method</label>
                <select className="select_option" value={settings[8] === "hierarchical" && settings[10] ? settings[10]:"directed"} onChange={(e) => {graphAction(id, "properties_tab", "settings", 
                  {
                    iframe: iframeRef,
                    settings: "sort_method",
                    state: e.target.value,
                  })}}
                  disabled={settings[8] !== "hierarchical"}>
                  <option value="directed">Directed</option>
                  <option value="hubsize">Hub-Size</option>
                </select>                
              </div>             
            </form>
          </div>
          )}
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
            <input id="allrelationships" name="relationship" type="radio" />
            <label htmlFor="allrelationships">*</label>
          </li>
          {graphStatus && graphStatus.map((rel, index) => (
            <li key={`${rel.type}-${index}`}>
              <input
                id={`window_${id}_${rel.type}_relationship_${index}`}
                name={`window_${id}_relationship`}
                type="radio"
                onChange={() =>
                  graphAction(id, "get_graph", "relationship", {
                    graphId: id,
                    sourceId: sourceId,
                    relationship: rel.type,
                    iframe: iframeRef,
                  })
                }
              />
              <label
                htmlFor={`window_${id}_${rel.type}_relationship_${index}`}
                style={{ backgroundColor: rel.bgcolor, color: rel.textcolor }}
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
function IframeEmbed({wId,id,fileName,title,activeGraph,graphAction,iframeRef,BASE_URL}) {
  if (id === "source_placeholder"){
    return (
      <div className="iframe_graph">
        <iframe
          ref={iframeRef}
          src={`${BASE_URL}/temp_placeholders/source_placeholder.html`}
          width="100%"
          height="98%"
          style={{ border: 'none' }}
          title="Network Graph"
        />          
      </div>
    );
  }
  if (id == "graph_placeholder"){//graphs_basic
    return (
      <div className="iframe_graph">
        <iframe
          ref={iframeRef}
          src={`${BASE_URL}/temp_placeholders/graph_placeholder.html`}
          width="100%"
          height="98%"
          style={{ border: 'none' }}
          title={`${title}`}
        /> 
        <span className="iframe_options_layer">
          <i onClick={(e) => {graphAction(wId, "iframe_options", "settings", 
            {
              iframe: iframeRef,
              settings: "fit_graph",
            })}}>
            <Icons id="window_graph_option" type="fieldview" condition="True" />
          </i>
        </span>        
      </div>
    );
  }
  if (id == "chart_placeholder"){//charts_basic
    return (
      <div className="iframe_graph">
        <iframe
          ref={iframeRef}
          src={`${BASE_URL}/temp_placeholders/charts_basic.html`}
          width="100%"
          height="98%"
          style={{ border: 'none' }}
          title={`${title}`}
        />        
      </div>
    );
  }
  else{
    return (
      <div className="iframe_graph">
        <iframe
          ref={iframeRef}
          src={`${BASE_URL}/temp_placeholders/${activeGraph}.html`}
          width="100%"
          height="98%"
          style={{ border: 'none' }}
          title={`${title}`}
        />     
      </div>
    );
  }
}
function DraggableWindow({ children, initialPos = { top: 0, left: 0 }, orientation, onDragStart, onFocus, zIndex, covered }) {
  const [pos, setPos] = useState(initialPos);
  const [isDragging, setIsDragging] = useState(false); // Add this line
  const windowRef = useRef(null);
  const dragging = useRef(false);
  const offset = useRef({ x: 0, y: 0 });
  const onMouseDown = (e) => {
    dragging.current = true;
    setIsDragging(true); // set dragging state

    if (windowRef.current) {
      windowRef.current.classList.add("dragging");
    }

    offset.current = {
      x: e.clientX - pos.left,
      y: e.clientY - pos.top,
    };

    document.body.style.userSelect = "none";
    windowRef.current.style.cursor = "grabbing";

    if (onFocus) onFocus();
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
    setIsDragging(false); // reset dragging state
    if (windowRef.current) {
      windowRef.current.classList.remove("dragging");
      windowRef.current.style.cursor = "grab";
    }
    document.body.style.userSelect = "auto";
    if (onDragStart) onDragStart(false);
  };

  useEffect(() => {
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
      document.body.style.userSelect = "auto";
    };
  }, []);
{console.log("isDragging:", isDragging)}
  return (    
    <div
      ref={windowRef}
      className={`window ${orientation === "windows" ? "" : "tab_mode"}`}
      style={{
        ...(orientation === "windows" && {
          top: pos.top,
          left: pos.left,
          position: "absolute",
          zIndex: zIndex
        }),
        userSelect: 'none',
      }}
    >
    {children({ onBarMouseDown: onMouseDown,isDragging:isDragging })}
    </div>
  );
}
function Windows({ id, type, isMaximized, isDragging, sessionId, loadscreenText, loadscreenState, isSideBarMenuOpen, orientation, configurations, windowAction, graphAction, chartAction, selectedContent, selectedSubContent, selectedNodes, selectedEdges,windowResponseI,windowResponseII,formToolResponse,batchFilesSearchHybrid,batchFilesSearchHybridQuery,searchText,batchFilesSearchLimit,batchFilesSearchResults,batchFilesSearchMoreFiles,searchResultsVisible,searchPlaceholder,batchFilesCollection, batchFilesDataframeInfoI, batchFilesDataframeInfoII, batchFilesDataframeActionValue, batchFilesDataframeSourceValue, batchFilesDataframeTargetValue, batchFilesDataframeRelationshipValue, batchFilesDataframeRuleValue, sourceSessionLog, sourceStreams , sourceStreamListener, fileInputRef, textareaRefs, onClose, onMove, zIndex, onFocus, covered, graphLink, graphLinkId, graphLinkSource, graphStatus, activeGraph, chartLink, chartLinkId, activechart, iframeRef, iframeFilters, iframeSettings, selectedPropertyTab, filterPropertyKeys, filterResults, nodeProperties, BASE_URL }) {
  if (type === "source") {
    return (
      <DraggableWindow initialPos={{ top: 0, left: 0}} zIndex={zIndex} orientation={orientation}>
        {(dragProps) => (
          <div id={`window_${type}_${id}`} style={{ zIndex }} 
            className={
              orientation === "tabs"
                ? `window tab_mode ${covered ? '' : 'focused'} ${dragProps.isDragging ? 'dragging' : ''}`
                : `window ${covered ? '' : 'focused'} ${dragProps.isDragging ? 'dragging' : ''}`
            }
            onMouseDown={() => onFocus(id)}>
            <div id={`window_loadscreen_${type}_${id}`} className="windows_loadscreen" style={{ display: loadscreenState ? "block" : "none" }}>
              <Loadscreen loadingText={loadscreenText} />
            </div>
            {(covered || dragProps.isDragging) && <div className="window_cover" />}  
            <div id={`window_bar_${type}_${id}`} className="window_bar"
              onMouseDown={isMaximized ? undefined : dragProps.onBarMouseDown} onDoubleClick={() => windowAction(id,"window_change_view", "",iframeRef)}>
              <div className="window_bar_title_container">Source Window<input placeholder="Add custom title" type="text"/></div>
              <div className="window_bar_btns_container">
                <span onClick={() => onClose(id)}>x</span>
                <span onClick={() => windowAction(id,"window_change_view", "",iframeRef)}>                 
                  {isMaximized ? <Icons id="window_bar" type="maximize" condition="True" /> : <Icons id="window_bar" type="maximize" condition="True" />}
                </span>
                <span>-</span>
              </div>
            </div>
            {/* Sidebar */}
            <div id={`window_side_bar_${type}_${id}`} className="side_bar">
              {/* New Source */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `new_source_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `new_source_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`new_source_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `new_source_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `new_source_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="new" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `new_source_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(id, 'live_source_options', `new_source_options_${type}_${id}`, 'update')}>
                      <span>Direct Source</span>
                    </li>
                    <li onClick={() => windowAction(id, 'upload_source_options', `new_source_options_${type}_${id}`, 'update')}>
                      <span>File Upload</span>
                    </li>
                    <li onClick={() => windowAction(id, 'load_source_options', `new_source_options_${type}_${id}`, 'update')}>
                      <span>Load Session</span>
                    </li>
                  </ul>
                </div>
              </div> 
            </div>
            <div id={`window_content_${type}_${id}`}  className='content_container'>
                {selectedContent === null && (
                  <div className="placeholder">
                    <IframeEmbed wId={id} id="source_placeholder" fileName="source_placeholder" activeGraph={activeGraph} graphAction={graphAction} BASE_URL={BASE_URL}/>
                  </div>
                )}
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
                    <div className="live_source_option" onClick={() => windowAction(id,"batch_input","update")}>
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
                      <input id="upload_source_option_input" multiple accept=".csv,.json,.parquet,.xlsx" type="file" ref={fileInputRef} onChange={(e) => windowAction(id, "upload_source_files", "upload", { files: e.target.files})}/>
                      <button onClick={() => fileInputRef.current.click()}>Choose files</button>
                    </div>
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
                              windowAction(id, "batch_input_form", "disconnect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                                session_id:id                                
                              });
                            }
                            else if (windowResponseI === "Disconnecting failed!") {
                              // Disconnect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              windowAction(id, "batch_input_form", "disconnect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                                session_id:id,
                              });
                            }
                            else {
                              // Connect logic
                              const brokerAddress = document.getElementById("source_input_address_text").value;
                              const hdfsAddress = document.getElementById("source_storage_address_text").value;
                              console.log("session id2",sessionId)
                              windowAction(id, "batch_input_form", "connect", {
                                broker: brokerAddress,
                                hdfs: hdfsAddress,
                                session_id:id                                
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
                              <input id="source_input_address_text" placeholder="Enter Broker/API Address" className="textinput" type="text"
                              disabled={
                                windowResponseI === "Connecting..." ? 'True' : 
                                windowResponseI === "Connection established!" ? 'True': ''
                              }
                              style={{
                                color: windowResponseI === "Connection established!" ? '#AAA' : '',
                                backgroundColor: windowResponseI === "Connection established!" ? '#EEE' : '',
                                borderColor: windowResponseI === "Connection established!" ? '#DDD' : ''
                              }}/>
                            </fieldset>
                            <fieldset>
                              <legend><b>HDFS</b> Connection</legend>
                              <div className="box_inputs_container">
                                <input id="hadoop_address_radio" className="radioinput" type="radio" name="source_storage_address_type" defaultChecked />
                                <label htmlFor="hadoop_address_radio">Hadoop Cluster</label>
                              </div>                            
                                <input id="source_storage_address_text" placeholder="Enter HDFS Address" defaultValue={configurations.active_storage_address} className="textinput" type="text" required
                                disabled={
                                  windowResponseI === "Connecting..." ? 'True' : 
                                  windowResponseI === "Connection established!" ? 'True': ''
                                }
                                style={{
                                  color: windowResponseI === "Connection established!" ? '#AAA' : '',
                                  backgroundColor: windowResponseI === "Connection established!" ? '#EEE' : '',
                                  borderColor: windowResponseI === "Connection established!" ? '#DDD' : ''
                                }}/>                            
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
                              windowAction(id, "tool_integration_form", "connect", {
                                tool_name: 'neo4j',
                                url: toolUrl,
                                username: toolUsername,
                                password: toolPassword,
                                database: toolDatabase,
                                source_id:id                                                                
                              });
                            }
                            else{
                              windowAction(id, "tool_integration_form", "disconnect", {
                                tool_name: 'neo4j',
                                source_id:id
                              });
                            }
                            }}>
                            <fieldset>
                              <legend><b>Tool/Database</b> Integration</legend>
                              <div className="box_inputs_container">
                                <input id="tool_neo4j_radio" className="radioinput" type="radio" name="analysis_tool_type" defaultChecked />
                                <label htmlFor="tool_neo4j_radio">Neo4j</label>
                              </div>
                              <input id="tool_url" placeholder="Url" className="textinput" defaultValue={configurations.active_tool_protocol} type="text"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_username" placeholder="Username" defaultValue={configurations.active_tool_username} className="textinput" type="text"
                                disabled={
                                formToolResponse === "Connected!" ? 'False': ''
                              }/>
                              <input id="tool_password" placeholder="Password" defaultValue={configurations.active_tool_password} className="textinput" type="password"
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
                              <legend><b>Search</b> from storage</legend>
                              <div className="batch_files_search_form" onSubmit={(e) => { e.preventDefault()}}>
                                <div id="batch_files_search_container" className="batch_files_search_container">
                                  <input id="batch_files_search_input" className="batch_files_search_text_input" type="text" placeholder="Type here to seach" required/>
                                  <input id="batch_files_search_date" className="batch_files_search_date_input" type="date"/>
                                  <button id="batch_files_search_button" title="Search" onClick={() => windowAction(id,"batch_files_search_input","search",[document.getElementById("batch_files_search_input").value,document.getElementById("batch_files_search_date").value,batchFilesSearchHybrid])}>
                                   <Icons id="window_live_source_option" type="search" condition="True"/>
                                  </button>
                                  <button title="Search"><Icons id="window_live_source_option" type="inbox-files" condition="True"/></button>
                                </div>
                                <div id="batch_files_search_options_container" className="batch_files_search_options_container">
                                  <input id="batch_files_search_files" title="Raw files search" defaultChecked type="radio" name="useSearch" onClick={() =>windowAction(id,"batch_files_search_useSearch","files","")}/>
                                  <label htmlFor="batch_files_search_files" title="Raw files search">Files</label>
                                  <input id="batch_files_search_es" title="Elastic keyword search" type="radio" name="useSearch" onClick={() =>windowAction(id,"batch_files_search_useSearch","hybrid","")}/>
                                  <label htmlFor="batch_files_search_es" title="Elastic keyword search">Hybrid (Elastic + Hive search)</label>
                                </div>
                                <div id="batch_files_search_result_container"
                                    className="batch_files_search_result_container"
                                    style={{
                                      '--searching-text': `'${searchPlaceholder}'`,
                                      display: searchResultsVisible ? "block" : "none"
                                    }}
                                  >
                                  <ul
                                    key={batchFilesSearchHybrid ? "hive-list" : "nonhive-list"}
                                    className="batch_files_search_results"
                                  >
                                    {searchText ? (
                                      // show empty UL to trigger CSS :empty::before
                                      null
                                    ) : (
                                      <>
                                        {Array.isArray(batchFilesSearchResults) &&
                                        batchFilesSearchResults.length > 0 ? (                                          
                                          batchFilesSearchHybrid ? (
                                            // =============================
                                            // ===== Hybrid RESULTS LIST =====
                                            // =============================
                                            <>
                                              {batchFilesSearchResults.map((file, index) => {
                                                const name = file?.name || "";
                                                const size = file?.size || "";
                                                console.log("batchFilesSearchResults:",batchFilesSearchResults)
                                                return (
                                                  <li
                                                    key={`hive-${name}-${index}`}
                                                    style={{
                                                      backgroundColor: batchFilesCollection.some(
                                                        (selectedFile) => selectedFile.name === name
                                                      )
                                                        ? "#EEE"
                                                        : "",
                                                      color: batchFilesCollection.some(
                                                        (selectedFile) => selectedFile.name === name
                                                      )
                                                        ? "#999"
                                                        : "",
                                                    }}
                                                    onClick={() => {
                                                      windowAction(id, "batch_files_select_file", "toggle_select", {
                                                          name,
                                                          size: file.size,
                                                        });
                                                      }}                                                                                                        
                                                  >
                                                    <span title={name}>{name}</span>
                                                    <span>{file.size} Rows</span>                                                    
                                                  </li>
                                                );
                                              })}
                                            </>
                                          ) : (
                                            // =============================
                                            // === Files RESULTS LIST ===
                                            // =============================
                                            <>
                                              {batchFilesSearchResults.map((file, index) => {
                                                const name = file?.name || "";
                                                const size = file?.size || "";
                                                return (
                                                  <li
                                                    key={`nonhive-${name}-${index}`}
                                                    style={{
                                                      backgroundColor: batchFilesCollection.some(
                                                        (selectedFile) => selectedFile.name === name && selectedFile.size === size
                                                      )
                                                        ? "#EEE"
                                                        : "",
                                                      color: batchFilesCollection.some(
                                                        (selectedFile) => selectedFile.name === name && selectedFile.size === size
                                                      )
                                                        ? "#999"
                                                        : "",
                                                    }}
                                                    onClick={() => {
                                                      windowAction(id, "batch_files_select_file", "toggle_select", {
                                                        name,
                                                        size: file.size,
                                                        date: file.date,                                                        
                                                        path: file.path,
                                                      });
                                                    }}
                                                  >
                                                    <span title={name}>{name}</span>
                                                    <span>{file.date}</span>
                                                    <span>{file.size} Kb</span>
                                                  </li>
                                                );
                                              })}

                                              {/* ===========================
                                                  ===== LOAD MORE BUTTON =====
                                                  =========================== */}
                                              {batchFilesSearchMoreFiles && (
                                                <li
                                                  style={{
                                                    backgroundColor:
                                                      searchPlaceholder === "Load more" ? "#FFF" : "",
                                                    color:
                                                      searchPlaceholder === "No more files" ? "#999" : "",
                                                  }}
                                                  className="batch_files_search_results_load_more"
                                                  onClick={() =>
                                                    windowAction(
                                                      id,
                                                      "batch_files_search_input",
                                                      "load_more",
                                                      [
                                                        document.getElementById("batch_files_search_input")
                                                          .value,
                                                        batchFilesSearchHybrid,
                                                        batchFilesSearchHybridQuery,
                                                      ]
                                                    )
                                                  }
                                                >
                                                  {searchPlaceholder}
                                                </li>
                                              )}
                                            </>
                                          )
                                        ) : (
                                          // No results
                                          <li style={{ backgroundColor: "#EEE", color: "#AAA" }}>
                                            No results Found!
                                          </li>
                                        )}
                                      </>
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
                                    <th>Size / Rows</th>
                                    <th></th>
                                  </tr>
                                </thead>
                                <tbody>
                                    {batchFilesCollection.length > 0 ? (
                                      console.log("hey:",batchFilesCollection),
                                        batchFilesCollection.map((file, index) => (
                                          <tr key={index}>
                                            <td>{index + 1}</td>
                                            <td>{file.name || file.response}</td>
                                            <td>{file.date || '*'}</td>
                                            <td>{file.size || '*'}</td>
                                            <td
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                const name=file.name || file.response;
                                                const date=file.last || null;
                                                const size=file.size || null;

                                                const payload = {
                                                  name: name,
                                                  date: date,
                                                  size: size,
                                                };
                                                console.log("batchFilesCollection:",batchFilesCollection)
                                                windowAction(id, "batch_files_select_file", "toggle_select", payload);
                                              }}
                                          >                                            
                                            <Icons
                                              id="window_live_source_option"
                                              type="minus"
                                              condition="True"
                                              onClick={() => handleRemoveFile(index)} // Logic to remove the file
                                            />
                                          </td>
                                        </tr>
                                      ))
                                    ) : null}
                                  {batchFilesCollection.length === 0 && (
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
                              color: windowResponseI === null ? '#999' : windowResponseI === "Connecting..." ? '#999' : windowResponseI === "..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : windowResponseI === "Dataset uploaded!" ? '#090' : '',
                              backgroundColor: windowResponseI === null ? 'rgba(0,0,0,0.1)' :windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : windowResponseI === "Dataset uploaded!" ? 'rgba(0, 255, 0, 0.2)' :''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === null ? "loadingx" :
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "..." ? "loadingx" :
                                  windowResponseI === "Connection established!" ? "correctx" :
                                  windowResponseI === "Dataset uploaded!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "..."}</span>
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
                                    <select id={`batch_files_dataframe_action_select_${type}_${id}`} value={batchFilesDataframeActionValue ? batchFilesDataframeActionValue:''} disabled={batchFilesDataframeInfoI[0] ? false:true} onChange={(e) => windowAction(id,"batch_files_actions_select","change",e.target.value)} className="actions_select_options">
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
                                    <select id={`batch_files_dataframe_source_select_${type}_${id}`} value={batchFilesDataframeSourceValue ? batchFilesDataframeSourceValue:''} disabled={batchFilesDataframeInfoI[2]  && batchFilesDataframeActionValue === "Source / Target Relationship" ? false:true} onChange={(e) => windowAction(id,"batch_files_source_select","change",e.target.value)} style={{float:'left',position:'relative',width:'32.5%',borderRight:'0.1vh dashed #EEE',marginRight:'0.1vw'}}>
                                      <option value="" disabled>Select Source</option>
                                      {batchFilesDataframeInfoI[2] ? (
                                        batchFilesDataframeInfoI[2].map((item, index) => (
                                            <option key={index} value={item}>{item}</option>
                                          ))
                                        ) : (
                                          <option>No columns</option>
                                      )}
                                    </select>
                                    <select id={`batch_files_dataframe_target_select_${type}_${id}`} value={batchFilesDataframeTargetValue ? batchFilesDataframeTargetValue:''} disabled={batchFilesDataframeInfoI[2] && batchFilesDataframeActionValue === "Source / Target Relationship" ? false:true} onChange={(e) => windowAction(id,"batch_files_target_select","change",e.target.value)} style={{float:'left',position:'relative',width:'32%',borderRight:'0.1vh dashed #EEE',marginRight:'0.1vw'}}>
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
                                    <input placeholder="HAS_RELATIONSHIP" value={batchFilesDataframeRelationshipValue ? batchFilesDataframeRelationshipValue:''} disabled={batchFilesDataframeActionValue === "Source / Target Relationship" && batchFilesDataframeSourceValue && batchFilesDataframeTargetValue ? false:true} onChange={(e) => windowAction(id,"batch_files_relationship_select","change",e.target.value)}
                                     type='text'/>
                                  </div>
                                  <div className="actions_partition">
                                    <label>Rule to apply</label>                                    
                                    <select id={`batch_files_dataframe_rule_select_${type}_${id}`} value={batchFilesDataframeRuleValue ? batchFilesDataframeRuleValue:''} disabled={batchFilesDataframeInfoI[5] && batchFilesDataframeActionValue === "Link Analysis" ? false:true} onChange={(e) => windowAction(id,"batch_files_rule_select","change",e.target.value)}>
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
                                    <select id={`batch_files_dataframe_filter_selectI_${type}_${id}`} disabled={batchFilesDataframeInfoI[2] ? false:true} onChange={(e) => windowAction(id,"batch_files_target_select","change",e.target.value)} defaultValue="">
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
                                    <select id={`batch_files_dataframe_filter_selectII_${type}_${id}`} disabled={batchFilesDataframeInfoI[2] ? false:true} onChange={(e) => windowAction(id,"batch_files_target_select","change",e.target.value)} defaultValue="">
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
                              color: windowResponseI === null ? '#999' : windowResponseI === "Session running..." ? '#33C' : windowResponseI === "Connecting..." ? '#999' : windowResponseI === "..." ? '#999' : windowResponseI === "Connection established!" ? '#090' : windowResponseI === "Connection failed!" ? '#900' : '',
                              backgroundColor:  windowResponseI === null ? '#999' : windowResponseI === "Session running..." ? 'rgba(0, 0, 255, 0.2)' : windowResponseI === "Connecting..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "..." ? 'rgba(0,0,0,0.1)' : windowResponseI === "Connection established!" ? 'rgba(0, 255, 0, 0.2)' : windowResponseI === "Connection failed!" ? 'rgba(255, 0, 0, 0.2)' : ''
                            }}>
                            <span>
                              <Icons
                                id="window_live_source_option"
                                type={
                                  windowResponseI === null ? "loadingx" :
                                  windowResponseI === "Connecting..." ? "loadingx" :
                                  windowResponseI === "..." ? "loadingx" :
                                  windowResponseI === "Session running..." ? "streamx" :
                                  windowResponseI === "Connection established!" ? "correctx" : "errorx"
                                }
                                condition="True"
                              />
                            </span>
                            <span>{windowResponseI || "..."}</span>
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
                              <textarea ref={el => (textareaRefs.current[id] = el)} className="batch_files_dataframe_filter_log_textarea" readOnly value={sourceSessionLog || ''}></textarea>
                            </fieldset>
                          </form>
                        </div>
                      )}
                    </div>
                    <div className="batch_connection_form_pager_container">
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageII"){
                            windowAction(id, "batch_input_form_swap_passive", "page_I",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII" && windowResponseI === "Connection established!"){
                            windowAction(id, "batch_input_form_swap_passive", "page_II",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII" && windowResponseI === "Dataset uploaded!"){
                            windowAction(id, "batch_input_form_swap_passive", "page_I",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV"){
                            windowAction(id, "batch_input_form_swap_passive", "page_III",null);
                          }
                          else{
                            return null;
                          }
                        }}
                        disabled={
                          selectedSubContent !== "batch_input_form_pageI" && selectedSubContent !== "batch_input_form_pageIV" ||
                          selectedSubContent === "batch_input_form_pageIV" && !sourceStreamListener ? '': 'True' 
                        }>
                        {"Back"}    
                      </button>
                      <button onClick={() => {
                          if (selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Connection established!"){
                            windowAction(id, "batch_input_form_swap", "page_II",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageI" && windowResponseI === "Dataset uploaded!"){
                            windowAction(id, "batch_input_form_swap", "page_III",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageII"){
                            windowAction(id, "batch_input_form_swap", "page_III",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIII"){
                            windowAction(id, "batch_input_form_swap", "page_IV",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV" && sourceStreamListener){
                            windowAction(id, "batch_input_stream_terminate", "page_IV",null);
                          }
                          else if (selectedSubContent === "batch_input_form_pageIV" && !sourceStreamListener){
                            windowAction(id, "batch_input_form_swap", "page_IV",null);
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
                          selectedSubContent === "batch_input_form_pageIV" && sourceStreamListener? '': 'True' 
                        }>
                        {selectedSubContent === "batch_input_form_pageIII" || selectedSubContent === "batch_input_form_pageIV" && !sourceStreamListener ? "Stream Graph":
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
                  <IframeEmbed wId={id} id="source_placeholder" fileName="source_placeholder" activeGraph={activeGraph} graphAction={graphAction} BASE_URL={BASE_URL}/>
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
      <DraggableWindow initialPos={{ top: 0, left: 0}} zIndex={zIndex} orientation={orientation}>
        {(dragProps) => (
          <div id={`window_${type}_${id}`} style={{ zIndex }} 
            className={
              orientation === "tabs"
                ? `window tab_mode ${covered ? '' : 'focused'} ${dragProps.isDragging ? 'dragging' : ''}`
                : `window ${covered ? '' : 'focused'} ${dragProps.isDragging ? 'dragging' : ''}`
            }
            onMouseDown={() => onFocus(id)}>
            <div id={`window_loadscreen_${type}_${id}`} className="windows_loadscreen" style={{ display: loadscreenState ? "block" : "none" }}>
              <Loadscreen loadingText={loadscreenText} />
            </div>
            {(covered || dragProps.isDragging) && <div className="window_cover" />}  
            <div id={`window_bar_${type}_${id}`} className="window_bar"
              onMouseDown={isMaximized ? undefined : dragProps.onBarMouseDown} onDoubleClick={() => windowAction(id,"window_change_view", "",iframeRef)}>
              <div className="window_bar_title_container">Graph Window<input placeholder="Add custom title" type="text"/></div>
              <div className="window_bar_btns_container">
                <span onClick={() => onClose(id)}>x</span>
                <span onClick={() => windowAction(id,"window_change_view", "",iframeRef)}>                 
                  {isMaximized ? <Icons id="window_bar" type="maximize" condition="True" /> : <Icons id="window_bar" type="maximize" condition="True" />}
                </span>
                <span>-</span>
              </div>
            </div>
            <div id={`window_side_bar_${type}_${id}`} className='side_bar'>
              {/* New Graph */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `new_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `new_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`new_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `new_graph_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `new_graph_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="newGraph" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `new_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(id,"new_graph",`new_graph_options_${type}_${id}`, iframeRef)}>
                      <div className="window_side_bar_menu_list_I">
                        <span>Empty Graph</span>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div>             
              {/* Link Graph Options */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `link_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `link_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`link_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `link_graph_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `link_graph_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="link" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `link_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <div className="window_side_bar_menu_list_I">
                        <form
                          onSubmit={(e) => {
                            e.preventDefault();
                            if (graphLink) {
                              // unlink logic
                              windowAction(id, "graph_link_form", "unlink", {
                                sourceId: graphLinkId,
                                graphId: id
                              });
                            } else {
                              const newGraphLinkId = document.getElementById(`graph_link_id_input_${id}`).value;
                              windowAction(id, "graph_link_form", "link", {
                                sourceId: newGraphLinkId,
                                graphId: id,
                                iframe: iframeRef
                              });
                            }
                          }}
                         > 
                        <input
                          id={`graph_link_id_input_${id}`}
                          type="text"
                          placeholder={graphLink ? graphLinkId : 'Enter window ID'}
                          disabled={Boolean(graphLink)}                          
                        />
                        <button type="submit">
                          {graphLink ? 'Unlink' : 'Link'}
                        </button>
                      </form>
                      </div>                                        
                  </ul>
                </div>
              </div> 
              {/* Upload Graph */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `upload_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `upload_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`upload_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `upload_graph_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `upload_graph_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="upload" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `upload_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(`upload_graph_options_${type}_${id}`, "upload")}>
                      <div className="window_side_bar_menu_list_I">
                        <span onClick={() => {
                            const input = document.createElement("input");
                            input.type = "file";
                            input.accept = ".json,.html";

                            input.onchange = (e) => {
                              alert("Any unsaved progress will be replaced!");
                              const file = e.target.files[0];
                              if (file) {
                                windowAction(id, "load_graph_url", file, iframeRef);
                              }
                            };

                            input.click();
                        }}>Upload Graph</span>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div>
              {/* Save Graph */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `save_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `save_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`save_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `save_graph_options_${type}_${id}` || false === false ? 'sbicon_disabled' : 'sbicon'}`} 
                  onClick={false !== false ? () => windowAction("side_bar_menu_list", `save_graph_options_${type}_${id}`, "") : null}>
                  <Icons id="window_side_bar" type="save" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `save_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(`save_graph_options_${type}_${id}`, "update")}>
                      <div className="window_side_bar_menu_list_I">
                        <span>Save Graph</span>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div>
              {/* Snap Graph */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `snap_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `snap_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`snap_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `snap_graph_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `snap_graph_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="capture" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `snap_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(id,"graph_snapshot", "",iframeRef)}>
                      <div className="window_side_bar_menu_list_I">
                        <span>Take a snap</span>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div> 
              {/* Print Graph */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `print_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `print_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`print_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `print_graph_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `print_graph_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="print" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `print_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(id,"graph_print", "",iframeRef)}>
                      <div className="window_side_bar_menu_list_I">
                        <span>Print Graph</span>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div> 
              {/* Reset Graph */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `reset_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `reset_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`reset_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `reset_graph_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `reset_graph_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="reset" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `reset_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(id,"reset_graph",`reset_graph_options_${type}_${id}`, iframeRef)}>
                      <div className="window_side_bar_menu_list_I">
                        <span>Reset Graph</span>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div> 
              {/* Export JSON */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `export_graph_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `export_graph_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`export_graph_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `export_graph_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `export_graph_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="export" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `export_graph_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li onClick={() => windowAction(`export_graph_options_${type}_${id}`, "update")}>
                      <div className="window_side_bar_menu_list_I">
                        <span onClick={() => {windowAction(id, "export_graph", "html", iframeRef);}}>Export HTML</span>
                      </div>                      
                    </li>
                    <li onClick={() => windowAction(`export_graph_options_${type}_${id}`, "update")}>
                      <div className="window_side_bar_menu_list_I">
                        <span onClick={() => {windowAction(id, "export_graph", "json", iframeRef);}}>Export JSON</span>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div> 
            </div>
            <div id={`window_content_${type}_${id}`} className="content_container">
            {/* Graph iframe: show placeholder or actual graph */}            
            {(selectedContent === "graph_content" || selectedContent === null) && 
            (
              <div className="placeholder">
                <IframeEmbed
                  wId={id} 
                  id={activeGraph || "graph_placeholder"}
                  fileName={activeGraph || "graph_placeholder"}
                  activeGraph={activeGraph}
                  graphAction={graphAction} 
                  iframeRef={iframeRef}
                  BASE_URL={BASE_URL}
                />
              </div>
            )}
          </div>
          <div id={`window_properties_${type}_${id}`} className="properties_container">
            {/* Settings panel: only show when a graph iframe is present */}
            {(selectedContent === "graph_content" || selectedContent === null) && 
            (
              <WindowVerticalSplitPanels
                id={id}
                type={type}
                sourceId={graphLinkSource}
                initialTopHeight="90%"
                minTopHeight="20%"
                maxTopHeight="100%"
                graphStatus={graphStatus}
                activeGraph={activeGraph}
                graphAction={graphAction}
                iframeRef={iframeRef}  // same ref as iframe
                iframeFilters={iframeFilters}
                iframeSettings={iframeSettings}
                selectedPropertyTab={selectedPropertyTab}
                nodeProperties={nodeProperties}
                filterPropertyKeys={filterPropertyKeys}
                filterResults={filterResults}
              />
              )}
            </div>
            <div id={`window_footer_${type}_${id}`}  className='window_footer'>
              <div className='window_footer'>
                <span>
                  <b>Window Id : </b>
                  <i>{id}</i>
                </span>
              </div>
            </div>
          "</div>
        )}
      </DraggableWindow>
    )
  }
  if (type === "chart"){ //Graph window
    return (
      <DraggableWindow initialPos={{ top: 0, left: 0}} zIndex={zIndex} orientation={orientation}>
        {(dragProps) => (
          <div id={`window_${type}_${id}`} style={{ zIndex }} 
            className={
              orientation === "tabs"
                ? `window tab_mode ${covered ? '' : 'focused'} ${dragProps.isDragging ? 'dragging' : ''}`
                : `window ${covered ? '' : 'focused'} ${dragProps.isDragging ? 'dragging' : ''}`
            }
            onMouseDown={() => onFocus(id)}>
            <div id={`window_loadscreen_${type}_${id}`} className="windows_loadscreen" style={{ display: loadscreenState ? "block" : "none" }}>
              <Loadscreen loadingText={loadscreenText} />
            </div>
            {(covered || dragProps.isDragging) && <div className="window_cover" />}  
            <div id={`window_bar_${type}_${id}`} className="window_bar"
              onMouseDown={isMaximized ? undefined : dragProps.onBarMouseDown} onDoubleClick={() => windowAction(id,"window_change_view", "",iframeRef)}>
              <div className="window_bar_title_container">Chart Window<input placeholder="Add custom title" type="text"/></div>
              <div className="window_bar_btns_container">
                <span onClick={() => onClose(id)}>x</span>
                <span onClick={() => windowAction(id,"window_change_view", "",iframeRef)}>                 
                  {isMaximized ? <Icons id="window_bar" type="maximize" condition="True" /> : <Icons id="window_bar" type="maximize" condition="True" />}
                </span>
                <span>-</span>
              </div>
            </div>
            <div id={`window_side_bar_${type}_${id}`} className='side_bar'>
              {/* Add chart Options */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `new_chart_options_${type}_${id}` && chartLink !== false ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `new_chart_options_${type}_${id}` && chartLink !== false 
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}
                >
                <i
                  className={`${
                  isSideBarMenuOpen === `new_chart_options_${type}_${id}` || chartLink === false ? 'sbicon_toggled' : 'sbicon'
                  }`}
                  onClick={chartLink !== false ? () =>
                    windowAction('side_bar_menu_list', `new_chart_options_${type}_${id}`, '') : null
                  }>
                  <Icons id="window_side_bar" type="newChart" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `new_chart_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li>
                      <div className="window_side_bar_menu_list_I">
                        <span>Global-Level Charts</span>
                        <ul className="window_side_bar_menu_list_II">
                        <li onClick={() => windowAction(id, "create_chart", "Assortativity", {iframe:iframeRef})}>Assortativity</li>
                        <li onClick={() => windowAction(id, "create_chart", "Reciprocity", {iframe:iframeRef})}>Reciprocity</li>
                        <li onClick={() => windowAction(id, "create_chart", "Global Metrics", {iframe:iframeRef})}>Global Metrics</li>
                        <li onClick={() => windowAction(id, "create_chart", "Cohesion Metrics", {iframe:iframeRef})}>Cohesion Metrics</li>
                        <li onClick={() => windowAction(id, "create_chart", "Temporal Metrics", {iframe:iframeRef})}>Temporal Metrics</li>                        
                      </ul>
                      </div>                      
                    </li>
                    <li>
                      <div className="window_side_bar_menu_list_I">
                        <span>Node-Level Charts</span>
                        <ul className="window_side_bar_menu_list_II">
                        <li onClick={() => windowAction(id, "create_chart", "Degree Centrality", {iframe:iframeRef})}>Degree Centrality</li>
                        <li onClick={() => windowAction(id, "create_chart", "In / Out Degree", {iframe:iframeRef})}>In / Out Degree</li>
                        <li onClick={() => windowAction(id, "create_chart", "Betweenness Centrality", {iframe:iframeRef})}>Betweenness Centrality</li>
                        <li onClick={() => windowAction(id, "create_chart", "Closeness Centrality", {iframe:iframeRef})}>Closeness Centrality</li>
                        <li onClick={() => windowAction(id, "create_chart", "Eigenvector Centrality", {iframe:iframeRef})}>Eigenvector Centrality</li>
                        <li onClick={() => windowAction(id, "create_chart", "Katz Centrality", {iframe:iframeRef})}>Katz Centrality</li>
                        <li onClick={() => windowAction(id, "create_chart", "PageRank", {iframe:iframeRef})}>PageRank</li>
                        <li onClick={() => windowAction(id, "create_chart", "Clustering Coefficient", {iframe:iframeRef})}>Clustering Coefficient</li>
                        <li onClick={() => windowAction(id, "create_chart", "Local Eccentricity", {iframe:iframeRef})}>Local Eccentricity</li>
                        <li onClick={() => windowAction(id, "create_chart", "HITS (Authority / Hub Scores)", {iframe:iframeRef})}>HITS (Authority / Hub Scores)</li>
                        <li onClick={() => windowAction(id, "create_chart", "Constraint (Structural Holes)", {iframe:iframeRef})}>Constraint (Structural Holes)</li>
                        <li onClick={() => windowAction(id, "create_chart", "Ego Network Size / Density", {iframe:iframeRef})}>Ego Network Size / Density</li>
                      </ul>
                      </div>                      
                    </li>
                    <li>
                      <div className="window_side_bar_menu_list_I">
                        <span>Edge-Level Charts</span>
                        <ul className="window_side_bar_menu_list_II">
                        <li onClick={() => windowAction(id, "create_chart", "Edge Betweenness", {iframe:iframeRef})}>Edge Betweenness</li>
                        <li onClick={() => windowAction(id, "create_chart", "Edge Weight", {iframe:iframeRef})}>Edge Weight</li>
                        <li onClick={() => windowAction(id, "create_chart", "Edge Embeddedness", {iframe:iframeRef})}>Edge Embeddedness</li>
                        <li onClick={() => windowAction(id, "create_chart", "Edge Similarity (Jaccard, Cosine)", {iframe:iframeRef})}>Edge Similarity (Jaccard, Cosine)</li>                        
                      </ul>
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div>
              {/* Link chart Options */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `link_chart_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `link_chart_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i id={`link_chart_options_${type}_${id}`} className={`${
                  isSideBarMenuOpen === `link_chart_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`} onClick={() => windowAction("side_bar_menu_list", `link_chart_options_${type}_${id}`, "")}>
                  <Icons id="window_side_bar" type="link" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `link_chart_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <div className="window_side_bar_menu_list_I">
                        <form
                          onSubmit={(e) => {
                            e.preventDefault();
                            if (chartLink) {
                              // unlink logic
                              windowAction(id, "chart_link_form", "unlink", {
                                graphId: chartLinkId,
                                chartId: id
                              });
                            } else {
                              const newChartLinkId = document.getElementById(`chart_link_id_input_${id}`).value;
                              windowAction(id, "chart_link_form", "link", {
                                graphId: newChartLinkId,
                                chartId: id
                              });
                            }
                          }}
                        >
                          <input
                            id={`chart_link_id_input_${id}`}
                            type="text"
                            placeholder={chartLink ? chartLinkId : 'Enter window ID'}
                            disabled={Boolean(chartLink)}
                          />
                          <button type="submit">
                            {chartLink ? 'Unlink' : 'Link'}
                          </button>
                        </form>
                      </div>                                        
                  </ul>
                </div>
              </div> 
              {/* Snap chart */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `snap_chart_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `snap_chart_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i
                  className={`${
                  isSideBarMenuOpen === `snap_chart_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`}
                  onClick={() =>
                    windowAction('side_bar_menu_list', `snap_chart_options_${type}_${id}`, '')
                  }>
                  <Icons id="window_side_bar" type="capture" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `snap_chart_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li>
                      <div className="window_side_bar_menu_list_I">
                        <span onClick={() => windowAction(id,"chart_snapshot", "",iframeRef)}>Take snaps</span>                        
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div>
              {/* Print chart */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `print_chart_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `print_chart_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i
                  className={`${
                  isSideBarMenuOpen === `print_chart_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`}
                  onClick={() =>
                    windowAction('side_bar_menu_list', `print_chart_options_${type}_${id}`, '')
                  }>
                  <Icons id="window_side_bar" type="print" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `print_chart_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li>
                      <div className="window_side_bar_menu_list_I">
                        <span onClick={() => windowAction(id,"chart_print", "",iframeRef)}>Print Charts</span>                        
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div>
              {/* Reset Chart */}
              <div
                className={`side_bar_menu ${
                  isSideBarMenuOpen === `reset_chart_options_${type}_${id}` ? 'show' : ''
                }`}
                style={{
                  backgroundColor:
                    isSideBarMenuOpen === `reset_chart_options_${type}_${id}`
                      ? ''
                      : 'rgba(0,0,0,0)',
                  borderRadius: '5px',
                }}>
                <i
                  className={`${
                  isSideBarMenuOpen === `reset_chart_options_${type}_${id}` ? 'sbicon_toggled' : 'sbicon'
                }`}
                  onClick={() =>
                    windowAction('side_bar_menu_list', `reset_chart_options_${type}_${id}`, '')
                  }>
                  <Icons id="window_side_bar" type="reset" condition="True" />
                </i>
                <div
                  className={`side_bar_btn_options ${
                    isSideBarMenuOpen === `reset_chart_options_${type}_${id}` ? 'show' : ''
                  }`}>
                  <ul>
                    <li>
                      <div className="window_side_bar_menu_list_I">
                        <span onClick={() => windowAction(id,"chart_reset", "",iframeRef)}>Reset Charts</span>                        
                      </div>                      
                    </li>
                  </ul>
                </div>
              </div>
            </div>
            <div id={`window_content_${type}_${id}`} className="content_container">
            {/* Graph iframe: show placeholder or actual graph */}            
            {(selectedContent === "chart_content" || selectedContent === null) && 
            (
              <div className="placeholder">
                <IframeEmbed
                  wId={id} 
                  id={activechart || "chart_placeholder"}
                  fileName={activechart || "chart_placeholder"}
                  activeGraph={activechart}
                  chartaction={chartAction} 
                  iframeRef={iframeRef}
                  BASE_URL={BASE_URL}
                />
              </div>
            )}
          </div>
          <div id={`window_properties_${type}_${id}`} className="properties_container">
            {/* Settings panel: only show when a chart iframe is present */}
            {(selectedContent === "chart_content" || selectedContent === null) && 
            (
              <div></div>
              )}
            </div>
            <div id={`window_footer_${type}_${id}`}  className='window_footer'>
              <div className='window_footer'>
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
}
function Main({setSessionId, API_URL,debounceRef,setConfigurations, configurations,windows, setWindows, openWindows }) {
  const hasRunRef = useRef(false);
  const iframeRef = useRef(null);
  useEffect(() => {
    if (hasRunRef.current) return;
    hasRunRef.current = true;
    console.log("Running initial openWindows calls");
    //openWindows('source', '',iframeRef);
    //openWindows('graph', '',iframeRef);

    // openWindows('chart', '',iframeRef);
  }, []);
  return (
    <main id='main'>\
    <NetworkBackground/>
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
}
function Root() {
  const [windows, setWindows] = useState([]);
  const [orientation, setOrientation] = useState("tabs"); // "windows" | "tabs"
  const [activeWindowId, setActiveWindowId] = useState(null);
  const [activeTabId, setActiveTabId] = useState(null);
  const windowIdRef = useRef(null);    // stores currently selected/active window
  const windowsRef = useRef([]);       // always mirrors latest windows[]
  const [isMaximized, setIsMaximized] = useState(false);
  const [isMinimized, setIsMinimized] = useState(false);
  const [zIndexCounter, setZIndexCounter] = useState(1); // zIndex counter
  const [sessionId, setSessionId] = useState(null);
  const [isToggleMenuOpen, setIsToggleMenuOpen] = useState(false);
  const [isTaskBarOpen, setIsTaskBarOpen] = useState(false);
  const [configurations, setConfigurations] = useState({});
  const [isConfigurationsOpen, setIsConfigurationsOpen] = useState(false);
  const [loadscreenState, setloadscreenState] = useState(false);
  const [loadscreenText, setloadscreenText] = useState('');
  const [isSideBarMenuOpen, setIsSideBarMenuOpen] = useState(false);
  const [selectedContent, setSelectedContent] = useState(null); // content to show inside the windows
  const [selectedSubContent, setSelectedSubContent] = useState(null); // content to show inside the windows
  const [batchFilesSearchHybrid, setBatchFilesSearchHybrid] = useState(false);
  const [batchFilesSearchHiveQuery, setbatchFilesSearchHiveQuery] = useState(false);
  const [searchText, setSearchText] = useState(false);
  const searchButtonRef = useRef(null)
  const [batchFilesSearchOffset, setBatchFilesSearchOffset] = useState(0);
  const [batchFilesSearchLimit, setBatchFilesSearchLimit] = useState(50);
  const [batchFilesSearchResults, setBatchFilesSearchResults] = useState([]);
  const [searchResultsVisible, setSearchResultsVisible] = useState(null);    
  const [batchFilesSearchMoreFiles, setBatchFilesSearchMoreFiles] = useState(true);
  const [searchPlaceholder, setSearchPlaceholder] = useState('');
  const [batchFilesDataframeInfoI, setBatchFilesDataframeInfoI] = useState([]);
  const [batchFilesDataframeInfoII, setBatchFilesDataframeInfoII] = useState([]);
  const [batchFilesDataframeActionValue, setBatchFilesDataframeActionValue] = useState(null);
  const [batchFilesDataframeRelationshipValue, setBatchFilesDataframeRelationshipValue] = useState(null);
  const [batchFilesDataframeSourceValue, setBatchFilesDataframeSourceValue] = useState(null);
  const [batchFilesDataframeTargetValue, setBatchFilesDataframeTargetValue] = useState(null);  
  const [batchFilesDataframeRuleValue, setBatchFilesDataframeRuleValue] = useState(null);
  const [sourceStreams, setSourceStreams] = useState({});  
  const [sourceStreamListener, setSourceStreamListener] = useState(false);
  const [sourceSessionLog, setSourceSessionLog] = useState('');
  const [sourceSessionLogFile, setSourceSessionLogFile] = useState({});     
  const fileInputRef = useRef(null);  
  const sourceRef = useRef(null);
  const socketRef = useRef(null);
  const logRef = useRef(''); // for accumulating logs    
  const textareaRefs = useRef({});
  const debounceRef = useRef(null);
  const [isDragging, setIsDragging] = useState(false);
  const [graphLinkstate, setGraphLinkState] = useState(false);
  const [graphStatusListener, setGraphStatusListener] = useState(false);
  const [graphStatus, setGraphStatus] = useState({});
  const [graphLinkSource, setGraphLinkSource] = useState(null);   
  const [activeGraph, setActiveGraph] = useState(''); 
  const iframeRefs = useRef({}); //to communicate across the iframe boundary  
  const [iframeSettings, setIframeSettings] = useState({}); // object instead of array
  const [isCtrlHeld, setIsCtrlHeld] = useState(false);  
  const API_URL = import.meta.env.VITE_API_URL
  const BASE_URL = import.meta.env.VITE_BASE_URL
  //const BASE_URL = "http://localhost:5173"
  //const API_URL = "http://localhost:5000";

  // useEffect(() => {  // Sync windowsRef on every update
  //   console.log("orientation:",orientation)
  // }, [orientation]);
  // ---------------------------------------------------------------------------- Basic useEffects ---
  // ---------------------------------------------------------------------------- Main session initializer ---
  useEffect(() => {
    const oldSession = localStorage.getItem('session'); //Already stored session
    debounceRef.current = setTimeout(() => {
        const payload = { id: "init", existing_session:oldSession};
        fetch(`${API_URL}/init`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        })
          .then(res => res.json())
          .then(data => {
            if (data.message === "success!") {
              const session = data.results;
              try{
                const configs= data.configurations;
                // const jsonString = configs.replace(/'/g, '"');
                // const parsedConfig = JSON.parse(jsonString);
                console.log("init_config:",configs)
                setConfigurations(configs)
                setSessionId(session);
              } 
              catch (error) {
                console.error('Error parsing configurations:', error);
              }
              if(session != oldSession){
                localStorage.setItem('session', session);
                setSessionId(session);
              }
            } else {
              alert("Could not initialize!");
            }
          })
          .catch(console.error);
      }, 300);
  }, []);
  // ---------------------------------------------------------------------------- sockets ---
  console.log("API_URL:",API_URL)

  useEffect(() => {  // Sync windowsRef on every update
    windowsRef.current = windows;
  }, [windows]);
  useEffect(() => {
    socketRef.current = io(API_URL);
    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
      }
    };
  }, []);
  // ------------------------------------------------------------------ logger ---
  useEffect(() => {
    if (!sourceSessionLogFile) return;

    logRef.current = '';
    const socket = socketRef.current;
    if (!socket) return;

    const handleLogs = (data) => {
      const id = String(data.session_id);
      if (data.error) return;

      logRef.current += '\n' + data.data;
      setWindows(prev =>
        prev.map(w =>
          w.id === id
            ? { ...w, sourceSessionLog: logRef.current }
            : w
        )
      );
      // ⬇️ autoscroll AFTER React paints
      requestAnimationFrame(() => {
        const textarea = textareaRefs.current[id];
        if (textarea) {
          textarea.scrollTop = textarea.scrollHeight;
        }
      });
    };

    socket.on("stream_logs", handleLogs);
    if (filename: sourceSessionLogFile.logFile){
      socket.emit("log_stream_plug", {filename: sourceSessionLogFile.logFile,session_id: sourceSessionLogFile.session_id});  
    }
    
    return () => {
      socket.emit("log_stream_unplug", {filename: sourceSessionLogFile.logFile,session_id: sourceSessionLogFile.session_id});
      socket.off("stream_logs", handleLogs);
    };
  }, [sourceSessionLogFile]);

// ------------------------------------------------------- graph status socket ---
  useEffect(() => {
    const socket = socketRef.current;
    if (!socket || !sourceStreamListener) return;

    let lastHash = null;
    let sessionId = sourceStreams
    const latestStreamingId = Object.entries(sourceStreams).filter(([_, isStreaming]) => isStreaming).at(-1)?.[0];
    const handleGraphStatus = (payload) => {
      const { type, data, error, session_id } = payload;
      sessionId = session_id
      if (error) {
        console.error("Graph status error:", error);
        return;
      }
      console.log("recieved status socket log:",payload)
      // Always get up-to-date target windows
      const targetWindows = windowsRef.current.filter(
        w => w.graphLinkSource === String(session_id)
      );

      if (targetWindows.length === 0) { //Target windows are the children of this source window (linked)
        console.warn("No target windows found for session:", session_id);
      }

      if (type === "metadata") {
        // Update local graph status
        //the global GraphStatus is dictionary
        setGraphStatus(prev => ({
          ...prev,
          [session_id]: {           // dynamic key
            ...prev[session_id], // Keep the relationships
            status: data
          }
        }));

        // Send payload to all target iframes
        targetWindows.forEach(w => {
          const iframe = iframeRefs.current[w.id];
          if (iframe?.current?.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: "informations", payload: data },
              "*"
            );
          } else {
            console.warn("Iframe ref not avaiLabel for window:", w.id);
          }
        });

        return;
      }

      if (type === "relationships") {
        // Optional: prevent unnecessary updates using a simple hash
        console.log("recieved_rlns",sessionId,data)
        const hash = JSON.stringify(data.map(r => [r.id, r.type, r.textcolor, r.bgcolor]));
        if (hash === lastHash) return;
        lastHash = hash;
        //the global GraphStatus is dictionary
        setGraphStatus(prev => ({
          ...prev,
          [session_id]: {           // dynamic key
            ...prev[session_id], 
            relationships: data // Keep the status
          }
        }));
        // Batch update windows in a single setWindows call
        //the private GraphStatus is simple (doesent consist all the values)
        setWindows(prev =>
          prev.map(w =>
            targetWindows.find(tw => tw.id === w.id)
              ? { ...w, graphStatus: data }
              : w
          )
        );
      }
    };

    socket.emit("graph_status_subscribe", { session_id: latestStreamingId });
    socket.on("status", handleGraphStatus);

    return () => {
      socket.emit("graph_status_unsubscribe", { session_id: latestStreamingId });
      socket.off("status", handleGraphStatus);
    };
  }, [sourceStreamListener]); // depend only on the source/session
  // ---------------------------------------------------------------------------- layout orientation ---
  useEffect(() => {
    if (orientation === "tabs" && windows.length > 0) {
      if (!activeWindowId || !windows.some(w => w.id === activeWindowId)) {
        setActiveWindowId(windows[0].id);
      }
    }
  }, [orientation, windows]);
  // ---------------------------------------------------------------------------- ctl button ---
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Control') {
        setIsCtrlHeld(true);
      }
    };

    const handleKeyUp = (e) => {
      if (e.key === 'Control') {
        setIsCtrlHeld(false);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    window.addEventListener('keyup', handleKeyUp);

    // Cleanup
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
      window.removeEventListener('keyup', handleKeyUp);
    };
  }, []);
  //------------------------------------------------------------------------------ Storage search result container visiblity
  useEffect(() => {
  }, [batchFilesSearchHybrid]);
  useEffect(() => {
  }, [batchFilesSearchHiveQuery]);
  useEffect(() => {
    if (searchResultsVisible) {
      const handleClickOutside = (event) => {
        if (
          searchButtonRef.current &&
          resultContainerRef.current &&
          !searchButtonRef.current.contains(event.target) &&
          !resultContainerRef.current.contains(event.target)
        ) {
          console.log('Click outside container', event.target);
          resultContainerRef.current.style.display = 'none';
          setSearchResultsVisible(false);
        } else {
          console.log('Click inside container', event.target);
        }
      };

      document.addEventListener('click', handleClickOutside);

      // Cleanup function
      return () => {
        document.removeEventListener('click', handleClickOutside);
      };
    }
  }, [searchResultsVisible]);

  //-------------------------------------------------------------------------------- Debounce
  useEffect(() => {
    // Cleanup function to clear the debounce timer when component unmounts or dependencies change
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, []);

  //-------------------------------------------------------------------------------- node properties listner
  useEffect(() => {
    const handleIframeMessage = (event) => {
      if (event.data?.type === "nodeProperties") {
        const properties = event.data.payload; // unpack the payload
        const mostTopGraphWindow=windowIdRef.current;
        //alert(mostTopGraphWindow,properties)
        setWindows(prev =>
            prev.map(w => w.type === "graph" && w.id === mostTopGraphWindow ? { ...w, nodeProperties: properties } : w)
        );
      }
      if (event.data?.type === "all_property_keys_response") {
        const { id, keys } = event.data.payload; // unpack the payload
        //console.log(`Got keys from iframe ${id}:`, keys); // ["label", "group", ...]
        // Update your state with just the keys
        // Update the specific window that matches this iframe id
        setWindows(prev =>
          prev.map(w => w.type === "graph" && w.id === id ? { ...w, filterPropertyKeys: keys } : w)
        );
      }
      if (event.data?.type === "graph_filter_results") {
        const { id, results } = event.data.payload; // unpack the payload
        setWindows(prev =>
          prev.map(w => w.type === "graph" && w.id === id ? { ...w, filterResults: results } : w)
        );
      }
      if (event.data?.type === "network_components") {
        console.log(5)
        const { id, nodes, edges } = event.data.payload; // unpack the payload
        //Store components
        handleChartActions(id,"storeNetwork","components",event.data.payload);//id the chart window id
        console.log(5.5,id)
      }
      if (event.data?.type === "entity_selection") {
        console.log(22)
        //Note: this id is not a chart window id but the sourse graph window id 
        const { id, selectedNodes, selectedEdges } = event.data.payload; // unpack the payload
        //set selected entities for alllinked chart window to the top graph window
        handleChartActions(id,"updateNetwork","selection",event.data.payload);
        console.log(23)
      }
      //1 Listen what is selected from the graph window
      //2 update the selected node and edges in each window that are chart type and linkd to that specific graph window
      //3 listen fro the nodes selection change and update the graphs that are already created
    };

      window.addEventListener("message", handleIframeMessage);
      return () => window.removeEventListener("message", handleIframeMessage);
  }, []);

  // ---------------------------------------------------------------------------- Windows management ---
  const handleFocusWindow = (id) => {
    console.log("focused_on:", id, "type:", typeof id);
    setWindows((prev) => {
      const maxZ = Math.max(...prev.map((w) => w.zIndex), 0);
      return prev.map((w) =>
        w.id === id
          ? { ...w, zIndex: maxZ + 1, covered: false }
          : { ...w, covered: true}
      );
    });
    windowIdRef.current = id;
    setActiveWindowId(id)
  };
  const generateWindowId = () => {
    if (typeof windowIdRef.current !== "number") { //Happens for source windows (since it hase '_' between the window id and session id)
      const firstChar = String(windowIdRef.current)[0];   // get first character
      const last_id = parseInt(firstChar, 10);
      if (last_id){
        windowIdRef.current = last_id+1  
      }else{
        windowIdRef.current = 1
      }      
      return windowIdRef.current;
    }
    windowIdRef.current += 1;
    return windowIdRef.current;
  };
  const handleCreateWindows = (sessionId, type, iframeRef) => {
    const id = generateWindowId();
    // if (windowIdRef[id]){
    //   const id = generateWindowId();
    // }
    if (type === "source") {
      const windowsId = id+"_"+sessionId;
      debounceRef.current = setTimeout(() => {
          const payload = { id: "source_window", session_id: sessionId, window_id:id};
          fetch(`${API_URL}/init_source`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          })
            .then(res => res.json())
            .then(data => {
              if (data.message === "success!") {
                setActiveWindowId(windowsId);
                setWindows(prev => {
                  const maxZ = prev.length
                    ? Math.max(...prev.map(w => w.zIndex))
                    : 0;
                  return [
                    ...prev,
                    {
                      id: windowsId,
                      type,
                      zIndex: maxZ + 1,
                      sessionId: sessionId,
                      selectedContent: null,
                      selectedSubContent: null,
                      formData: {},
                      windowResponseI: null,
                      formToolResponse: null,
                      batchFilesSearchResults: null,
                    },
                  ];
                });
                console.log("windowsId:",windowsId)
                handleFocusWindow(windowsId)
                setZIndexCounter(prev => prev + 1);
              } else {
                alert("Could not initialize source window!");
              }
            })
            .catch(console.error);
        }, 300);
    } else if (type === "graph") {
      iframeRefs.current[id] = React.createRef();
      setActiveWindowId(id);
      setIframeSettings(prev => ({
        ...prev,
        [id]: ["", "", 25, "", "", true, "", true, "default", "UD", "directed"],
      }));

      setWindows(prev => {
        const maxZ = prev.length ? Math.max(...prev.map(w => w.zIndex)) : 0;
        return [
          ...prev,
          {
            id,
            type,
            zIndex: maxZ + 1,
            sessionId,
            graphLinkSource: null,
            selectedContent: null,
            selectedSubContent: null,
            graphLink: null,
            graphStatus: null,
            activeGraph: null,
            iframeSettings,
            covered: false,
          },
        ];
      });
      handleFocusWindow(id)
      setZIndexCounter(prev => prev + 1);
    } else if (type === "chart") {
      iframeRefs.current[id] = React.createRef();
      setActiveWindowId(id);
      setWindows(prev => {
        const maxZ = prev.length ? Math.max(...prev.map(w => w.zIndex)) : 0;
        return [
          ...prev,
          {
            id,
            type,
            zIndex: maxZ + 1,
            sessionId,
            selectedContent: null,
            selectedSubContent: null,
            chartLink: false,
            activechart: null,
            covered: false,
          },
        ];
      });
      handleFocusWindow(id)
      setZIndexCounter(prev => prev + 1);
    } else if (type === "parent") {
      setWindows(prev => {
        const maxZ = prev.length ? Math.max(...prev.map(w => w.zIndex)) : 0;
        return [
          ...prev,
          {
            id,
            type,
            zIndex: maxZ + 1,
            sessionId,
            selectedContent: null,
            selectedSubContent: null,
            chartLink: false,
            activechart: null,
            covered: false,
          },
        ];
      });
      handleFocusWindow(id)
      setZIndexCounter(prev => prev + 1);
    }    
  };
  const handleOpenWindows = (type, link, iframeRef) => {
    const sessionId=localStorage.getItem('session'); //Already stored session
    if (type==="source"){
      if (link===""){
        //If theres no link just creates the window
        handleCreateWindows(sessionId,type);
      }
      else{
        return;
      }
    }
    if (type==="graph"){
      if (link===""){
        //If theres no link just creates the window
        handleCreateWindows(sessionId,type,iframeRef);
      }
      else{
        return;
      }
    }
    if (type==="chart"){
      if (link===""){
        //If theres no link just creates the window
        handleCreateWindows(sessionId,type,iframeRef);
      }
      else{
        return;
      }
    }
    if (type==="parent"){
      handleCreateWindows(type);
    }
  };
    // --- Bring window to front ---
  const handleCloseWindow = (id) => {
    setWindows(prev => {
      const newWindows = prev.filter(w => w.id !== id);
      // Save next window ID OUTSIDE of setWindows
      const nextId = newWindows.length
        ? newWindows[newWindows.length - 1].id
        : null;
      // Update state only
      setTimeout(() => {
        if (nextId !== null) handleFocusWindow(nextId);
        else setOrientation("windows");
      }, 0);
      return newWindows;
    });
  };
  const handleMoveWindow = (id, newPos) => {
    setWindows(prev =>
      prev.map(w => (w.id === id ? { ...w, position: newPos } : w))
    );
  };
  // ---------------------------------------------------------------------------- Windows functions ---
  // --- Basic Graph actions ---
  const updateIframeSettings = (windowId, key, value) => {
    setIframeSettings(prev => ({
      ...prev,
      [windowId]: {
        ...prev[windowId],
        [key]: value
      }
    }));    
  }
  const handleChartActions = (id, menuId, action, payload) => {
    console.log(6,id, menuId, action, payload)
    setWindows(prev =>
    prev.map(w => {
      if (w.id !== id) return w;
      let loadscreenState = w.loadscreenState;
      let newContent = w.selectedContent;
      let newSubContent = w.selectedSubContent;

      if(menuId === "new_instance"){
        alert("new:",id)
      }
      if(menuId === "getNetwork" && action === "components"){
        console.log(11,payload)
        const iframe = payload;
        if (iframe?.current?.contentWindow) {
          iframe.current.contentWindow.postMessage(
            { action: "network_components", payload: id}, // Requesting for the nodes and egdes of that specific graph (request is sent to the graph window itself), #id is the graph window id
              "*"
          );
        }
        else{
          alert(0)
        }
      }    
      if (menuId === "storeNetwork" && action === "components") {
        console.log(7);
        let iframe; // define outside
        const { id, nodes, edges } = payload; //id is chart window id
        console.log(id, nodes, edges);

        const targetWindow = windowsRef.current.find(w => w.id === String(id) || w.id === Number(id));
        if (targetWindow && targetWindow.type === "chart") {
          iframe = iframeRefs.current[targetWindow.id]; // assign here
          console.log(8, iframe);
        }

        if (iframe?.current?.contentWindow) {
          console.log(9);
          iframe.current.contentWindow.postMessage(
            { action: "network_components", payload: { nodes, edges } },
            "*"
          );
        } else {
          console.log(10, iframe?.current);
        }
      }    
      if (menuId === "updateNetwork" && action === "selection") {
        console.log(24);
        const {id, nodes: selectedNodes = [], edges: selectedEdges = [] } = payload;
        const targetWindow = windowsRef.current.find(w => w.id === String(id) || w.id === Number(id));
        if (targetWindow && targetWindow.type === "graph") {
          //Identify all the windows linked to the received window id
          const linkedCharts = windowsRef.current.filter(
            w => w.type === "chart" && (w.chartLink === id || w.chartLink === String(id))
          );
          console.log(26,selectedNodes,selectedEdges,payload);
          if(linkedCharts.length>0){
            //loop over them and send updates
            linkedCharts.forEach(chartWindow => {
              const chartIframeRef = iframeRefs.current[chartWindow.id];
              if (chartIframeRef?.current) {
                // Example: send message or trigger chart update
                chartIframeRef.current.contentWindow.postMessage(
                  { action: "updateSelection", payload:{selectedNodes, selectedEdges} },
                  "*"
                );
              }
            });
          }            
        }
        else {
          console.log(25);
        }
      }               
    return { ...w
        }
      })
    )
  }
  const handleGraphActions = (id, menuId, action, payload) => {
    console.log("GraphAction:", id, menuId, action, payload);

    const sendToIframe = (iframe, msgAction, msgPayload) => {
      if (!iframe?.current) {
        alert("Iframe not found!");
        return;
      }

      const send = () => {
        iframe.current.contentWindow?.postMessage(
          { action: msgAction, payload: msgPayload },
          "*"
        );
      };

      const iframeSrc = iframe.current.src;
      if (iframeSrc.includes("graphs_basic")) {
        send();
      } else {
        iframe.current.onload = send;
      }
    };
    // Clear previous debounce if exists
    if (debounceRef.current) clearTimeout(debounceRef.current);

    // Debounce wrapper for actions that need delay
    debounceRef.current = setTimeout(() => {
      setWindows(prev =>
        prev.map(w => {
          if (w.id !== id) return w;

          const iframe = payload?.iframe || null;
          const updates = {};

          // ------------------ Graph generation ------------------
          if (menuId === "get_graph" && action === "relationship") {
            const newPayload = {
              id: action,
              source_id: payload.sourceId,
              relationship: payload.relationship
            };

            updates.loadscreenState = true;
            updates.loadscreenText = "Staging Graph";

            fetch(`${API_URL}/get_graph`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(newPayload)
            })
              .then(res => res.json())
              .then(data => {
                setWindows(prev =>
                  prev.map(win =>
                    win.id === id
                      ? { ...win, loadscreenState: false, activeGraph: data.message === "success!" ? "graphs_basic" : null }
                      : win
                  )
                );

                if (data.message === "success!") {
                  sendToIframe(iframe, "new_graph", {
                    id,
                    nodes: data.results.nodes,
                    edges: data.results.edges
                  });
                } else {
                  alert(data.message);
                }
              })
              .catch(err => {
                console.error(err);
                setWindows(prev =>
                  prev.map(win =>
                    win.id === id
                      ? { ...win, windowResponseI: "Connection failed!" }
                      : win
                  )
                );
                alert(err);
              });
          }

          // ------------------ Property tabs ------------------
          if (menuId === "properties_tab") {
            if (action === "switch_tab") {
              updates.selectedPropertyTab = payload;
            }

            if (action === "settings") {
              const settingsMap = {
                limit_nodes_key: [0, "key"],
                limit_nodes_sort: [1, "sort"],
                limit_nodes_amount: [2, "amount"],                
                weight_edges: [3, null],
                show_title: [4, null],
                show_label: [5, null],
                edit_infos: [6, null],
                graph_physics: [7, null],
                layout_type: [8, null],
                layout_direction: [9, null],
                sort_method: [10, null],
                label_nodes_with: [11, null]
              };

              const [index, key] = settingsMap[payload.settings] || [];
              if (index !== undefined) {
                updateIframeSettings(id, index, key ? payload.state[key] : payload.state);
              }

              sendToIframe(iframe, payload.settings, payload.state);

              // Special case for hierarchical layout
              if (payload.settings === "layout_type" && payload.state === "hierarchical") {
                updateIframeSettings(id, 9, "UD");
                updateIframeSettings(id, 10, "directed");
              }
            }

            if (action === "filter_keys" && payload.filter === "all_property_keys") {
              sendToIframe(iframe, "all_property_keys", { id });
            }

            if (action === "filter_search") {
              const { option, keyword, keys, settings } = payload;
              sendToIframe(iframe, "graph_search", { id, option, keyword, keys, settings });
            }
          }

          // ------------------ Iframe options ------------------
          if (menuId === "iframe_options" && action === "settings") {
            sendToIframe(iframe, "fit_graph", "fit_graph");
          }

          return { ...w, ...updates };
        })
      );
    }, 300); // debounce delay
  };

  // --- Basic Windows actions ---
  const handleWindowActions = (id, menuId, action, payload) => {
    //console.log("id:",id," Menuid:", menuId," action:", action," payload:", payload)
    // --- Handling sidebar menus
    setIsSideBarMenuOpen(prev => prev === menuId ? null : menuId); // Toggle open/close
    // --------------------------
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
        if (menuId === "live_source_options") {
          newContent = "live_source_options"; // This will show live source UI
        }
        if (menuId === "upload_source_options") {
          newContent = "upload_source_options"; // This will show upload source UI
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, fileInputRef:fileInputRef} : w
            )
          );
        }
        if (menuId === "upload_source_files") {
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
            const sessionId = id //That specific source window id
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
              fetch(`${API_URL}/upload_batch_files`, {
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
          fetch(`${API_URL}/connect_to_source`, {
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
          fetch(`${API_URL}/disconnect_source`, {
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
          fetch(`${API_URL}/connect_to_tool`, {
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
          fetch(`${API_URL}/disconnect_tool`, {
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
                    setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, batchFilesCollection: [] } : w
            )
          );  
        }
        if (menuId === "batch_input_form_swap_passive" && action === "page_II") {
          newContent = "batch_input";
          newSubContent = "batch_input_form_pageII";      
        }
        if (menuId === "batch_files_search_useSearch") {
          setBatchFilesSearchResults({ results: [], message: "" });
          if (action === "hybrid"){
            setBatchFilesSearchHybrid(true)            
            setWindows(prev => prev.map(w =>
              w.id === id
                ? { ...w, batchFilesSearchHybrid: true, batchFilesSearchResults: { results: [], message: "" } }
                : w
            ));
          }
          else{
            setBatchFilesSearchHybrid(false)            
            setWindows(prev => prev.map(w =>
              w.id === id
                ? { ...w, batchFilesSearchHybrid: false, batchFilesSearchResults: { results: [], message: "" } }
                : w
            ));            
          }
        }
        if (menuId === "batch_files_search_hive_query") {
          setbatchFilesSearchHybridQuery(prev => !prev);
          setBatchFilesSearchResults({ results: [], message: "" });
            setWindows(prev => prev.map(w =>
              w.id === id
                ? { ...w, batchFilesSearchHybridQuery: !batchFilesSearchHybridQuery, batchFilesSearchResults: { results: [], message: "" } }
                : w
            ));
        }
        if (menuId === "batch_files_search_input") {
            const keyword = payload[0];
            const date = payload[1];
            const hybrid = payload[2];
            const buildPayload = offset => ({
                id: "search",
                value: {
                    keyword,
                    date,
                    hybrid,
                    offset,
                    limit: batchFilesSearchLimit
                },
                session_id: id
            });
            // ============================
            //       FIRST SEARCH
            // ============================
            if (action === "search" && keyword ) {

                clearTimeout(debounceRef.current);

                debounceRef.current = setTimeout(() => {

                    setBatchFilesSearchOffset(0);
                    setBatchFilesSearchResults([]);
                    setSearchPlaceholder("Searching...");

                    // UI reset
                    setWindows(prev =>
                        prev.map(w =>
                            w.id === id
                                ? {
                                      ...w,
                                      searchText: true,
                                      searchResultsVisible: true,
                                      searchPlaceholder: "Searching..."
                                  }
                                : w
                        )
                    );

                    fetch(API_URL + "/live_batch_files", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify(buildPayload(0))
                    })
                        .then(res => res.json())
                        .then(data => {
                            const hasMore = data.has_more;
                            console.log("search results:",data.results,hasMore)

                            // update the actual rendered list
                            setBatchFilesSearchResults(data.results);
                            setBatchFilesSearchMoreFiles(hasMore);
                            setBatchFilesSearchOffset(data.results.length);
                            // update UI flags only
                            setWindows(prev =>
                                prev.map(w =>
                                    w.id === id
                                        ? {
                                              ...w,
                                              searchText: false,
                                              batchFilesSearchResults:data.results,
                                              batchFilesSearchMoreFiles: hasMore,
                                              searchResultsVisible: true,
                                              searchPlaceholder: hasMore
                                                  ? "Load more"
                                                  : "No more files"
                                          }
                                        : w
                                )
                            );
                            if(data.message === "Result out of bound!"){
                              alert(data.message)
                            }
                        })
                        .catch(console.error);
                }, 300);
            }

            // ============================
            //         LOAD MORE
            // ============================
            if (action === "load_more") {
                const offset = batchFilesSearchOffset;

                // Only set text/loading UI
                setWindows(prev =>
                    prev.map(w =>
                        w.id === id
                            ? {
                                  ...w,
                                  searchText: true,
                                  searchResultsVisible: true,
                                  searchPlaceholder: "Loading more..."
                              }
                            : w
                    )
                );

                fetch(API_URL + "/live_batch_files", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(buildPayload(offset))
                })
                    .then(res => res.json())
                    .then(data => {
                        console.log("search results:",data)
                        const hasMore =  data.has_more;

                        // append to the REAL list used by UI
                        setBatchFilesSearchResults(prev => [...prev, ...data.results]);

                        // update pagination flags
                        setBatchFilesSearchMoreFiles(hasMore);
                        setBatchFilesSearchOffset(offset + data.results.length);

                        // update UI state (not the actual list)
                        setWindows(prev =>
                            prev.map(w =>
                                w.id === id
                                    ? {
                                          ...w,
                                          searchText: false,
                                          batchFilesSearchResults: [ ...(w.batchFilesSearchResults || []), ...data.results ],
                                          batchFilesSearchMoreFiles: hasMore,
                                          searchPlaceholder: hasMore
                                              ? "Load more"
                                              : "No more files"
                                      }
                                    : w
                            )
                        );
                    })
                    .catch(console.error);
            }
        }
        if (menuId === "batch_files_select_file" && action === "toggle_select") {
          var filename=payload["name"];
          var filesize=payload["size"];
          console.log("newBatchFilesCollection:",newBatchFilesCollection);
          console.log("gotPayload:",payload);
          console.log("gotfilename:",filename);
          console.log("gotfilesize:",filesize);
          const isAlreadySelected = newBatchFilesCollection.some(
            (file) => (file.name === filename && file.size === filesize)
          );
          if (isAlreadySelected) {
              // File is already selected, remove it
              console.log(`deSelecting file: ${filename}`,filesize);
              newBatchFilesCollection = newBatchFilesCollection.filter(
              (file) => !(file.name === filename && file.size === filesize)              );
            } else {
              // File is not selected, add it
              console.log(`Selecting file: ${filename}`,filesize);
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
            let dataSourceKind="hdfs";
            if (windowResponseI == "Dataset uploaded!"){
                dataSourceKind = "files"
            }
            // if (batchFilesSearchHybrid == true){
            //   dataSourceKind = "hive"
            // }
            // if (batchFilesSearchHybridQuery == true){
            //   dataSourceKind = "hive_query"
            // }
            const payload = {
                id: "create_DF",
                type: "array",
                kind:dataSourceKind,
                session_id: id,//Source window id
                value: newBatchFilesCollection, //Explodes at back end
              };
            fetch(`${API_URL}/live_batch_files`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            })
            .then((res) => res.json())
            .then((data) => {    
                var arrayData=Object.values(data.results)
                if (data.message == "success!"){
                  //Changing window content
                  alert("Dataframe created")
                  console.log("arrayData:",arrayData)
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
              alert("requestID823469312 failed!");
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
          debounceRef.current = setTimeout(() => {
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? {
                      ...w,
                      batchFilesDataframeInfoI:w.batchFilesDataframeInfoI,
                      batchFilesDataframeActionValue: payload,
                      loadscreenState: false,
                    }
                  : w
              )
            );
            //setBatchFilesDataframeActionValue(payload);
          }, 300);
        } 
        if (menuId === "batch_files_source_select" && action === "change") {
          debounceRef.current = setTimeout(() => {
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? {
                      ...w,
                      batchFilesDataframeInfoI:w.batchFilesDataframeInfoI,
                      batchFilesDataframeSourceValue: payload,
                      loadscreenState: false,
                    }
                  : w
              )
            );
            //setBatchFilesDataframeSourceValue(payload);
          }, 300);
        }
        if (menuId === "batch_files_target_select" && action === "change") {
          debounceRef.current = setTimeout(() => {
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? {
                      ...w,
                      batchFilesDataframeInfoI:w.batchFilesDataframeInfoI,
                      batchFilesDataframeTargetValue: payload,
                      loadscreenState: false,
                    }
                  : w
              )
            );
            //setBatchFilesDataframeTargetValue(payload);
          }, 300);
        }
        if (menuId === "batch_files_relationship_select" && action === "change") {
          // No debounce or loadscreen needed
          setWindows(prev =>
            prev.map(w =>
              w.id === id
                ? { ...w,                       
                  batchFilesDataframeInfoI:w.batchFilesDataframeInfoI,
                  batchFilesDataframeRelationshipValue: payload }
                : w
            )
          );
          //setBatchFilesDataframeRelationshipValue(payload);
        }
        if (menuId === "batch_files_rule_select" && action === "change") {
          debounceRef.current = setTimeout(() => {
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? {
                      ...w,
                      batchFilesDataframeInfoI:w.batchFilesDataframeInfoI,
                      batchFilesDataframeRuleValue: payload,
                      loadscreenState: false,
                    }
                  : w
              )
            );
            //setBatchFilesDataframeRuleValue(payload);
          }, 300);
        }        
        if (menuId === "batch_input_form_swap" && action === "page_IV") { //This is the component that initalizes the graph streaming
          // Set new timeout for debounce
          console.log("streaming:",batchFilesDataframeInfoI)
          debounceRef.current = setTimeout(() => {
            const newLoadscreenText="Initalizing "
            // Initialize a variable to accumulate logs
            let accumulatedLogs = '';
            setSourceStreams(prev => ({ ...prev, [id]: false }));
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w, windowResponseI:null,sourceSessionLog:null,sourceStreamListener: true,loadscreenState: false, loadscreenText:newLoadscreenText } : w
              )
            );
            //Requesting session start
            // Get the window object from the current state
            const targetWindow = windows.find(w => w.id === id);
            let action, source, target, relationship, tool, rule
            if (!targetWindow) {
              console.warn("Window not found:", id);
            } else {
              action = targetWindow.batchFilesDataframeActionValue;
              source = targetWindow.batchFilesDataframeSourceValue;
              target = targetWindow.batchFilesDataframeTargetValue;
              relationship = targetWindow.batchFilesDataframeRelationshipValue && targetWindow.batchFilesDataframeRelationshipValue !== '' &&  targetWindow.batchFilesDataframeRelationshipValue !== true
              ? targetWindow.batchFilesDataframeRelationshipValue : 'HAS_RELATIONSHIP';            
              tool=targetWindow.batchFilesDataframeInfoI[7]; //Tools values sent from dataframe info
              rule=targetWindow.batchFilesDataframeRuleValue;
              console.log("Action, Source, Target from window:", action, source, target, relationship, tool, rule);
            }            
            //Template stance if failed to start the session (keeps the page from swaping)
            newContent = "batch_input";
            newSubContent = "batch_input_form_pageIII";
            const payload = {
                id: "stream",
                session_id: id,
                value:{"window_id":id,"session_id":id,"tool":tool,"action":action,"source":source,"target":target,"relationship":relationship,"rule":rule}// Add filter request here
              };
            fetch(`${API_URL}/live_batch_files`, {
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
                  console.log("logFile:",logFile);
                  const socket = socketRef.current;
                  if (!socket || !logFile) return;
                  setSourceStreamListener(true);
                  setSourceSessionLogFile({logFile,session_id: id});
                  setSourceStreams(prev => ({ ...prev, [id]: true }));
                  newContent = "batch_input";
                  newSubContent = "batch_input_form_pageIV";
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id
                        ? {
                            ...w,
                            windowResponseI: "Session running...",
                            batchFilesDataframeInfoI:w.batchFilesDataframeInfoI,
                            loadscreenState: false,
                            selectedContent: newContent,
                            selectedSubContent: newSubContent,     
                            sourceStreamListener: true                       
                          }
                        : w
                    )
                  );
                }
                else{
                  alert("failed!1")
                  console.log("err:",data.exception,data.message)
                  setSourceStreams(prev => ({ ...prev, [id]: false}));
                  setSourceStreamListener(true);
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id
                        ? {
                            ...w,
                            batchFilesDataframeInfoI:w.batchFilesDataframeInfoI,
                            windowResponseI: "Dataset uploaded!",
                            loadscreenState: false,
                            sourceStreamListener: false
                          }
                        : w
                    )
                  );
                }
              }
            })
            .catch((err) => {
              console.error("err",err);
              alert("requestID823463492 failed!");
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeInfoI:w.batchFilesDataframeInfoI, loadscreenState: false} : w
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
               w.id === id ? { ...w, windowResponseI:null, sourceSessionLog:null,loadscreenState: true, loadscreenText:newLoadscreenText } : w
              )
            );
            // Unpluging sockets
            const socket = socketRef.current;
            if (socket && socket.connected) {
              socket.emit("log_stream_unplug", {filename: sourceSessionLogFile?.logFile,session_id: id});
              socket.emit("graph_status_unsubscribe", {session_id: id})
              // Optional: stop listening to the client side
              // socket.off("stream_logs");
            }
            //Refresh the options (the previous content options)
            let oldBatchFilesDataframeInfoI=batchFilesDataframeInfoI
            //setBatchFilesDataframeInfoI(null)
            console.log("termination:",batchFilesDataframeInfoI)
            const payload = {
                id: "end_session",
                  session_id: id,
                value:{"window_id":id,"session_id":id,"log_file":sourceSessionLogFile}
              };
            fetch(`${API_URL}/live_batch_files`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            }) 
            .then((res) => res.json())
            .then((data) => { 
              if (data!=null){
                if (data.message==="success"){
                  if (socket) socket.off("stream_logs"); 
                  setSourceStreamListener(false);
                  setSourceSessionLogFile(null);
                  setSourceSessionLog(null);
                  setSourceStreams(prev => ({ ...prev, [id]: false }));
                  //setBatchFilesDataframeInfoI(oldBatchFilesDataframeInfoI)
                  // unsubscribe sockets
                  socketRef.current.emit("graph_status_unsubscribe", { session_id: id });
                  setWindows(prev => prev.map(w =>
                    w.id === id
                    ? { ...w,windowResponseI:null,loadscreenState: false,sourceStreamListener: false,batchFilesDataframeInfoI:w.batchFilesDataframeInfoI, sourceSessionLog: null, setSourceSessionLogFile:null}
                      : w
                  ));
                  //alert("alert_message:",data.message)
                }
                else{
                  setWindows(prev => prev.map(w =>
                    w.id === id
                    ? { ...w,loadscreenState: false}
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
              alert("requestID82346922 failed!");
              setWindows(prev =>
                prev.map(w =>
                  w.id === id ? { ...w, batchFilesDataframeInfoI:w.batchFilesDataframeInfoI, loadscreenState: false} : w
                )
              );
            });
          }, 300); // debounce delay   
        }
        // ------------------------------------------------------------------- Graph window contents handling
        if (menuId === "new_graph") {
          const iframe=payload;
          const targetWindow = windows.find(w => w.id === id);
          if (targetWindow?.graphLink === true) {
            alert("Please unlink the graph first!");            
          }

          else{
            alert("message box: any unsaved progress is lost!")                
            if (iframe?.current && iframe.current.contentWindow) {
              iframe.current.contentWindow.postMessage(
                { action: menuId, payload: "" },
                "*"
              );
              // Store the link in the target window
              setWindows(prev =>
                prev.map(w =>
                  w.id === id
                  ? {
                      ...w,
                      activeGraph: "graphs_basic",
                      selectedContent: "graph_content",
                      graphStatus: null,
                      graphLink: false,
                      graphLinkSource: null,
                      loadscreenState: false,
                      nodeProperties: null,
                      filterPropertyKeys: null,
                      filterResults: null,
                    }
                  : w
                )
              );
            } 
          }
        }
        if (menuId === "graph_link_form" && action === "link") {
          debounceRef.current = setTimeout(() => {
            const newLoadscreenText = "Linking window ";

            // Show loadscreen for the window being linked
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? { ...w, loadscreenState: true, loadscreenText: newLoadscreenText }
                  : w
              )
            );

            const sourceId = payload["sourceId"];
            const iframe = payload["iframe"];
            const newPayload = { id: "link", source_id: sourceId, LinkedTo: id };

            // Send link request to backend
            fetch(`${API_URL}/graph_link`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(newPayload),
            })
              .then(res => res.json())
              .then(data => {
                if (data?.message === "success!") {
                  // Link succeeded
                  setGraphLinkState(true);
                  setGraphLinkSource(sourceId);
                  sourceRef.current = sourceId;
                  setGraphStatusListener(true);
                  alert("Linked to:",sourceId);
                  // -----------------------------
                  // 1️⃣ Determine relationships to send
                  // -----------------------------
                  const targetWindows = windowsRef.current.filter(
                    w => w.graphLinkSource === String(sourceId)
                  );

                  let existingRelationships = null;
                  // clone relationships from the existing siblings
                  if (targetWindows.length > 0) { //Could be 0 even the link is formed with the first child

                    // Use graphStatus from first linked window that has it
                    existingRelationships = targetWindows.find(w => w.graphStatus)?.graphStatus;
                  }
                  console.log("targetWindows.length:",targetWindows.length, targetWindows.id)
                  // Fallback: use global graphStatus
                  // No siblings found
                  if (!existingRelationships) {
                    // relationship that corelates with the linked source (cause this linking could be the first)
                    // graphStatus containes all the session status and relationships
                    const relationships = graphStatus[sourceId]?.relationships || [];
                    console.log("relationships_global:",graphStatus[sourceId],relationships)
                    existingRelationships = relationships;
                  }

                  // -----------------------------
                  // 2️⃣ Send relationships to the new window
                  // -----------------------------
                  if (existingRelationships) {
                    console.log("existingRelationships.length",existingRelationships.length)
                    setWindows(prev =>
                      prev.map(w =>
                        w.id === id
                          ? { ...w, graphStatus: existingRelationships}
                          : w
                      )
                    );
                  }
                  
                  // -----------------------------
                  // 3️⃣ Subscribe socket for streaming (doesnt over lap or duplicates)
                  // -----------------------------
                  const latestStreamingId = Object.entries(sourceStreams)
                    .filter(([_, isStreaming]) => isStreaming)
                    .at(-1)?.[0];
                  if (latestStreamingId) {
                    socketRef.current.emit("graph_status_subscribe", { session_id: latestStreamingId });
                  }
                  console.log("latestStreamingId",latestStreamingId)
                  // Initiate new socket
                  const socket = socketRef.current;
                  if (!socket || !sourceStreamListener){
                    alert ("Cannot initiate socket")
                    return;
                  }
                  socket.emit("graph_status_subscribe", { session_id: latestStreamingId });
                  // -----------------------------
                  // 4️⃣ Update new window state
                  // -----------------------------
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id
                        ? {
                            ...w,
                            sessionId: sourceId,
                            graphLinkSource: sourceId,
                            selectedContent: "graph_content",
                            activeGraph: "graph_info_placeholder",                            
                            graphLink: true,
                            loadscreenState: false
                          }
                        : w
                    )
                  );
                } else {
                  // Linking failed
                  setGraphLinkState(false);
                  setGraphLinkSource(null);
                  setGraphStatusListener(false);
                  setIsSideBarMenuOpen("link_graph_options");
                  alert("No streaming found! " + (data?.message || ""));

                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id
                        ? {
                            ...w,
                            selectedContent: selectedContent,
                            graphStatus: null,
                            graphLink: false,
                            loadscreenState: false
                          }
                        : w
                    )
                  );
                }
              })
              .catch(err => {
                setGraphLinkState(false);
                setGraphLinkSource(null);
                setGraphStatusListener(false);
                setIsSideBarMenuOpen("link_graph_options");
                alert("Linking failed! " + (err.data || err.message || err));

                setWindows(prev =>
                  prev.map(w =>
                    w.id === id
                      ? {
                          ...w,
                          selectedContent: selectedContent,
                          graphStatus: null,
                          graphLink: false,
                          loadscreenState: false
                        }
                      : w
                  )
                );
              });
          }, 300);
        }
        if (menuId === "graph_link_form" && action === "unlink") {
            setGraphLinkState(false)
            setGraphLinkSource(null)
            setGraphStatusListener(false)
            //alert("message box: any unsaved progress is lost!")                
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w, activeGraph:"graph_placeholder",graphStatus:null,graphLinkSource:null,filterPropertyKeys:null,selectedContent:null,graphLink:false,loadscreenState: false} : w
              )
            );
        }
        if (menuId === "load_graph_url") {
          const iframe=payload
          debounceRef.current = setTimeout(() => {
            const file = action;
            if (!file || !(file instanceof File)) {
              alert("Selected file is not valid. Please choose a proper .json or .html file.");
              return;
            }

            // Show the loadscreen immediately
            const newLoadscreenText = "Staging File...";
            setWindows(prev =>
              prev.map(w =>
                w.id === id ? { ...w, loadscreenState: true, loadscreenText: newLoadscreenText } : w
              )
            );

            const ext = file.name.split(".").pop().toLowerCase();
            if (ext !== "json" && ext !== "html") {
              alert("Unsupported file type. Only .json or .html files are allowed.");
              setTimeout(() => {
                setWindows(prev =>
                  prev.map(w =>
                    w.id === id ? { ...w, loadscreenState: false, loadscreenText: "" } : w
                  )
                );
              }, 500);
              return; // <--- missing in original
            }

            const reader = new FileReader();
            reader.onload = (event) => {
              const content = event.target.result;

              const hideLoadscreen = () => {
                setWindows(prev =>
                  prev.map(w =>
                    w.id === id ? { ...w, loadscreenState: false, loadscreenText: "" } : w
                  )
                );
              };

              try {
                if (ext === "json") {
                  const parsed = JSON.parse(content);
                  let graphData, networkOptions;

                  if (parsed.graphData && Array.isArray(parsed.graphData.nodes) && Array.isArray(parsed.graphData.edges)) {
                    graphData = parsed.graphData;
                    networkOptions = parsed.networkOptions || {};
                  } else if (Array.isArray(parsed.nodes) && Array.isArray(parsed.edges)) {
                    graphData = parsed;
                    networkOptions = parsed.networkOptions || {};
                  } else {
                    alert("This JSON file does not contain a valid graph.");
                    hideLoadscreen();
                    return;
                  }

                  if (payload?.current && payload.current.contentWindow) {
                    // reset window state
                    setWindows(prev =>
                      prev.map(w =>
                        w.id === id
                          ? {
                              ...w,
                              activeGraph: "graphs_basic",
                              selectedContent: "graph_content",
                              graphStatus: null,
                              graphLink: false,
                              graphLinkSource: null,
                              loadscreenState: false,
                              nodeProperties: null,
                              filterPropertyKeys: null,
                              filterResults: null,
                            }
                          : w
                      )
                    );

                    if (!iframe?.current) {
                      alert("Iframe not found!");
                      hideLoadscreen();
                      return;
                    }

                    const sendGraphMessage = () => {
                      iframe.current.contentWindow?.postMessage(
                        {
                          action: menuId,
                          payload: { id, file },
                        },
                        "*"
                      );
                    };

                    const iframeSrc = iframe.current.src;
                    if (iframeSrc.includes("graphs_basic")) {
                      sendGraphMessage();
                    } else {
                      iframe.current.onload = () => {
                        console.log("graphs_basic iframe loaded");
                        sendGraphMessage();
                      };
                    }

                    setTimeout(hideLoadscreen, 700);
                  }

                } else if (ext === "html") {
                  // reset window state
                  setWindows(prev =>
                    prev.map(w =>
                      w.id === id
                        ? {
                            ...w,
                            activeGraph: "graphs_basic",
                            selectedContent: "graph_content",
                            graphStatus: null,
                            graphLink: false,
                            graphLinkSource: null,
                            loadscreenState: false,
                            nodeProperties: null,
                            filterPropertyKeys: null,
                            filterResults: null,
                          }
                        : w
                    )
                  );

                  setTimeout(hideLoadscreen, 700);

                  if (!content.includes("new vis.DataSet")) {
                    alert("This HTML file does not contain a valid graph.");
                    hideLoadscreen();
                    return;
                  }

                  if (!iframe?.current) {
                    alert("Iframe not found!");
                    hideLoadscreen();
                    return;
                  }

                  const sendGraphMessage = () => {
                    iframe.current.contentWindow?.postMessage(
                      {
                        action: menuId,
                        payload: { id, file },
                      },
                      "*"
                    );
                  };

                  const iframeSrc = iframe.current.src;
                  if (iframeSrc.includes("graphs_basic")) {
                    sendGraphMessage();
                  } else {
                    iframe.current.onload = () => {
                      console.log("graphs_basic iframe loaded");
                      sendGraphMessage();
                    };
                  }

                  setTimeout(hideLoadscreen, 700);
                }
              } catch (err) {
                alert("Error reading graph file: " + err.message);
                hideLoadscreen();
              }
            };

            reader.onerror = () => {
              alert("Error reading file.");
              setTimeout(() => {
                setWindows(prev =>
                  prev.map(w =>
                    w.id === id ? { ...w, loadscreenState: false, loadscreenText: "" } : w
                  )
                );
              }, 500);
            };

            reader.readAsText(file);
          }, 300);
        }
        if (menuId === "graph_snapshot") {
          const iframe=payload;          
          if (iframe?.current && iframe.current.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: menuId, payload: "" },
              "*"
            );
          }          
        }
        if (menuId === "graph_print") {
          const iframe=payload;          
          if (iframe?.current && iframe.current.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: menuId, payload: "" },
              "*"
            );
          }          
        }
        if (menuId === "window_change_view") {
          const iframe=payload;  
          setIsMaximized(prev => !prev);
          setWindows(prev =>
            prev.map(w =>
              w.id === id
                ? { 
                    ...w,
                    isMaximized: !w.isMaximized // toggle true ↔ false
                  }
                : w
            )
          );                   
        }
        if (menuId === "reset_graph") {
          const iframe=payload;     
          const newSettings=["","",25, "", "", true, "", true, "default", "UD", "directed"]
          setIframeSettings(prev => ({
            ...prev,        // spread existing entries
            [id]: newSettings // update specific id
          }));

          if (iframe?.current && iframe.current.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: menuId, payload: newSettings},
              "*"
            );
          }   
        }
        if (menuId === "export_graph") {
          const iframe=payload;          
          if (iframe?.current && iframe.current.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: menuId, payload: action },
              "*"
            );
          }          
        }
        // ------------------------------------------------------------------- Graph window contents handling
        if (menuId === "create_chart") {
          const { iframe } = payload;
          console.log("here:",iframe)
            if (iframe?.current && iframe.current.contentWindow) {
              iframe.current.contentWindow.postMessage(
                { action: menuId, payload: action },
                "*"
              );
            }
        }      
        if (menuId === "chart_link_form" && action === "link") {
          console.log("chart_linking...")
          const graphId = payload["graphId"];
          const newLoadscreenText = "Linking window ";
          // Show loadscreen for the window being linked
          setWindows(prev =>
            prev.map(w =>
              w.id === id ? { ...w, loadscreenState: true, loadscreenText: newLoadscreenText } : w
            )
          );
          //Finding the window
          const targetWindow = windowsRef.current.find(w => w.id === String(graphId) || w.id === Number(graphId));
          if (targetWindow && targetWindow.type==="graph") {
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? { ...w, 
                    activechart: "charts_basic",
                    loadscreenState: false, 
                    chartLink: graphId }
                  : w
              )
            );
            //iframe of the found window
            const iframeRef = iframeRefs.current[targetWindow.id];
            //Transfring the action to HandelChartActions
            handleChartActions(id, "getNetwork", "components", iframeRef);
            handleChartActions(id, "new_instance")
          } else {
            alert(`No graph window found with id: ${graphId}`);
            setWindows(prev =>
              prev.map(w =>
                w.id === id
                  ? { ...w, loadscreenState: false, chartLink: false }
                  : w
              )
            );
          }
        }
        if (menuId === "chart_link_form" && action === "unlink") {            
            alert("message box: any unsaved progress is lost!")                
            setWindows(prev =>
              prev.map(w =>
               w.id === id ? { ...w, selectedContent:null,chartLink:false,loadscreenState: false} : w
              )
            );
        }
        if (menuId === "chart_snapshot") {
          const iframe=payload;          
          if (iframe?.current && iframe.current.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: menuId, payload: "" },
              "*"
            );
          }          
        }
        if (menuId === "chart_print") {
          const iframe=payload;          
          if (iframe?.current && iframe.current.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: menuId, payload: "" },
              "*"
            );
          }          
        }
        if (menuId === "chart_reset") {
          const iframe=payload;          
          if (iframe?.current && iframe.current.contentWindow) {
            iframe.current.contentWindow.postMessage(
              { action: menuId, payload: "" },
              "*"
            );
          }          
        }
        console.log("batchFilesDataframeInfoI:",batchFilesDataframeInfoI)
        return { ...w, 
          selectedContent: newContent, 
          selectedSubContent: newSubContent,
          batchFilesSearchHybrid: batchFilesSearchHybrid,
          batchFilesSearchHiveQuery: batchFilesSearchHiveQuery,
          batchFilesSearchResults: newBatchSearchResult, 
          searchResultsVisible: searchResultsVisible,
          batchFilesCollection : newBatchFilesCollection,
          searchPlaceholder: searchPlaceholder,
          batchFilesDataframeInfoI: w.batchFilesDataframeInfoI,
          batchFilesDataframeInfoII: batchFilesDataframeInfoII,
          loadscreenState: loadscreenState,
          sourceStreams: sourceStreams,
          textareaRefs: textareaRefs,
          isMaximized:isMaximized
        };
      })
    );
  };
  // --- Navigation Menu Actions ---
  const handleNavAction = (action) => {
    //alert("nav_menu")
    // handle specific nav actions here
  };
    // --- Configuration Actions ---
  const handleConfigurationActions = (id,payload) => {
    if (id === "change"){
      const { name, value } = payload;
      setConfigurations(prev => ({
        ...prev,
        [name]: name === "storage_tables" || name === "active_tool_tables"
          ? value.split(",").map(s => s.trim())  // convert string to array
          : value
      }));
    }
    if (id === "save"){
      debounceRef.current = setTimeout(() => {
        let formData=payload;
        formData.append("id","save")
        formData.append("session_id",sessionId)
        fetch(`${API_URL}/configuration`, {
          method: "POST",
          body: formData,
        })
        .then((res) => res.json())
        .then((data) => {
          if (data.message === "success!") {
            alert("Configuration saved!")
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
      }, 300);
    }
    else if (id === "load_default"){
      const session = localStorage.getItem('session'); //Stored main session
      const newPayload = {"id":"load","session_id":session}
      debounceRef.current = setTimeout(() => {  
        setloadscreenState(true)    
        fetch(`${API_URL}/configuration`, {
          method: "POST",
          body: JSON.stringify(newPayload),
          headers: {"Content-Type": "application/json",},
        })
        .then((res) => res.json())
        .then((data) => {
          if (data.message === "success!") {     
            try{
                  const configs= data.results.data;
                  // const jsonString = configs.replace(/'/g, '"');
                  // const parsedConfig = JSON.parse(jsonString);
                  console.log("defaultConfig:",configs)                 
                  setConfigurations(configs) 
                  setloadscreenState(false)
                } 
                catch (error) {
                  console.error('Error parsing configurations:', error);
                  setloadscreenState(false)        
                }             
          } 
          else {
            alert(data.message)
          }
        })
        .catch((err) => {
          console.error("ConfigErr",err);  
          setloadscreenState(false)        
        });    
      }, 300);
    }
  };
  // --- Toggle Menu Actions ---
  const handleToggleMenu = (id) => {
    if(id=="toggle_menu_new_source_window"){
      handleOpenWindows("source","");
      setIsToggleMenuOpen(false)
    }
    else if(id=="toggle_menu_new_graph_window"){
      handleOpenWindows("graph","");
      setIsToggleMenuOpen(false)      
    }
    else if(id=="toggle_menu_new_chart_window"){
      handleOpenWindows("chart","");
      setIsToggleMenuOpen(false)      
    }
    else if(id=="toggle_menu_new_tabel_window"){
      handleOpenWindows("table","");
      setIsToggleMenuOpen(false)      
    }
    else if(id === "toggle_menu_orientation") {
        setOrientation(prev =>
            prev === "windows" ? "tabs" : "windows"
        );
        setIsToggleMenuOpen(true)
    }
    else if(id === "windows_taskbar") {
      setIsTaskBarOpen(prev => !prev);
    }
    else if(id === "configurations") {
      handleConfigurationActions("load_default")
      setIsConfigurationsOpen(prev => !prev);
    }
    else{
      setIsToggleMenuOpen(prev => !prev);
    }
  };
  // --- Root Return ---
  // ------------------------
  // Rendering
  // ------------------------
  return (
    <div style={{ position: 'relative', minHeight: '100vh', overflow: 'hidden' }}>
      {/*<NetworkBackground />*/}
      <div style={{ position: 'absolute', top: 0, bottom: 0, left: 0, right: 0, zIndex: 1 }}>
        <NavBar onNavAction={handleNavAction} />
        <ToggleMenu
          onToggle={handleToggleMenu}
          isToggleMenuOpen={isToggleMenuOpen}
          toggleAction={handleToggleMenu}
          isMaximized={isMaximized}
          windows={windows}
          orientation={orientation}
        />
        <Taskbar windows={windows} isTaskBarOpen={isTaskBarOpen} activeWindowId={activeWindowId} focusWindow={handleFocusWindow} toggleAction={handleToggleMenu} isCtrlHeld={isCtrlHeld}/>
        <Configurations sessionId={sessionId} actions={handleConfigurationActions} loadscreenState={loadscreenState} setloadscreenState={setloadscreenState} toggleAction={handleToggleMenu} configurations={configurations} isConfigurationsOpen={isConfigurationsOpen}/>
        <Main setSessionId={setSessionId} API_URL={API_URL} debounceRef={debounceRef} setConfigurations={setConfigurations} configurations={configurations} windows={windows} setWindows={setWindows} openWindows={handleOpenWindows} />
        {/* ----------------------
            Windows Container
        ---------------------- */}
          {windows.map(window => (
            <Windows
              {...window}
              key={window.id}
              onFocus={handleFocusWindow}
              // keep all your existing props intact
              id={window.id}
              type={window.type}
              isMaximized={window.isMaximized}
              orientation={orientation}
              configurations={configurations}
              isMinimized={window.isMinimized}
              isDragging={window.isDragging}
              sessionId={window.sessionId}
              isToggleMenuOpen={isToggleMenuOpen}
              isTaskBarOpen={isTaskBarOpen}
              isSideBarMenuOpen={isSideBarMenuOpen}
              loadscreenState={window.loadscreenState}
              loadscreenText={window.loadscreenText}
              windowAction={handleWindowActions}
              graphAction={handleGraphActions}
              chartAction={handleChartActions}
              selectedContent={window.selectedContent}
              selectedSubContent={window.selectedSubContent}
              windowResponseI={window.windowResponseI}
              windowResponseII={window.windowResponseII}
              formToolResponse={window.formToolResponse}
              batchFilesSearchHybrid={window.batchFilesSearchHybrid}
              batchFilesSearchHiveQuery={window.batchFilesSearchHiveQuery}
              batchFilesSearchLimit={window.batchFilesSearchLimit}
              batchFilesSearchResults={window.batchFilesSearchResults}
              batchFilesSearchMoreFiles={window.batchFilesSearchMoreFiles}
              searchResultsVisible={window.searchResultsVisible}
              searchPlaceholder={window.searchPlaceholder}
              batchFilesCollection={window.batchFilesCollection}
              batchFilesDataframeInfoI={window.batchFilesDataframeInfoI}
              batchFilesDataframeInfoII={window.batchFilesDataframeInfoII}
              batchFilesDataframeActionValue={window.batchFilesDataframeActionValue}
              batchFilesDataframeRelationshipValue={window.batchFilesDataframeRelationshipValue}
              sourceSessionLog={window.sourceSessionLog}
              sourceStreams={window.sourceStreams}
              sourceStreamListener={window.sourceStreamListener}
              fileInputRef={window.fileInputRef}
              textareaRefs={window.textareaRefs}
              onClose={handleCloseWindow}
              onMove={orientation === 'windows' ? handleMoveWindow : null}
              zIndex={window.zIndex}
              covered={window.covered}
              graphLink={window.graphLink}
              graphStatus={window.graphStatus}
              activeGraph={window.activeGraph}
              iframeRef={iframeRefs.current[window.id]}
              iframeSettings={iframeSettings}
              selectedPropertyTab={window.selectedPropertyTab}
              filterPropertyKeys={window.filterPropertyKeys}
              filterResults={window.filterResults}
              nodeProperties={window.nodeProperties}
              BASE_URL={BASE_URL}
            />
          ))}      

        {/* ----------------------
            Tabs Bar (only visible in tab mode)
        ---------------------- */}
        {orientation === 'tabs' && windows.length > 0 && (
          <div id="window_parent_tabs" className="window_parent_tabs">
            {/* Window bar with tab titles */}
            <div id="window_parent_bar" className="window_parent_bar">
              <div className="window_parent_bar_toggle_menu">
                <div className="toggle_menu_btn">
                  <span onClick={handleToggleMenu}>
                    <i><a></a></i>
                  </span>
                  <label style={{display : isToggleMenuOpen ? 'none':''}}>Linkx | <i>Web Analyzer</i></label>
                </div>
              </div>
              <div className="window_parent_bar_title_container"></div>
            </div>
            <div className="window_parent_tabs_container">
              {windows.map(w => (
                <div
                  key={w.id}
                  className={`tab_title ${activeWindowId === w.id ? 'active' : ''}`}
                  onClick={() => handleFocusWindow(w.id)}
                >
                  {`${w.type.charAt(0).toUpperCase()}${w.type.slice(1)} Window ${w.id}` || `Window ${w.id}`}
                  <div className="tab_title_close_btn" onClick={() => handleCloseWindow(w.id)}>x</div>
                </div>
              ))}
            </div>          
          </div>
        )}
      </div>
    </div>
  );
}

export default Root;

