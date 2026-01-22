function Icons({id,category,type,condition}){
  if (id === "window_side_bar"){
    if (type === "new"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="New source" style={{marginBottom:'5vh'}}>
              <svg width="100%" height="100%" label="New" title="New">
                <def>
                  <symbol id="icon-add-solid" viewBox="-5 -5 30 30">
                    <path d="M11 9v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zM10 20c-5.523 0-10-4.477-10-10s4.477-10 10-10v0c5.523 0 10 4.477 10 10s-4.477 10-10 10v0z"/>
                  </symbol>
                </def>
                <use xlinkHref="#icon-add-solid"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="New source" style={{marginBottom:'5vh'}}>
              <svg width="100%" height="100%" label="New" title="New">
                <def>
                  <symbol id="icon-add-solid" viewBox="-5 -5 30 30">
                    <path d="M11 9v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zM10 20c-5.523 0-10-4.477-10-10s4.477-10 10-10v0c5.523 0 10 4.477 10 10s-4.477 10-10 10v0z"/>
                  </symbol>
                </def>
                <use xlinkHref="#icon-add-solid"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "newChart"){
      if (condition === "True"){
        return(
          <span className="side_bar_icon_active" title="New source">
            <svg width="100%" height="100%" label="New" title="New">
              <def>
                  <symbol id="icon-add-solid" viewBox="-5 -5 30 30">
                    <path d="M11 9v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zM10 20c-5.523 0-10-4.477-10-10s4.477-10 10-10v0c5.523 0 10 4.477 10 10s-4.477 10-10 10v0z"/>
                  </symbol>
                </def>
                <use xlinkHref="#icon-add-solid"/>
            </svg>
          </span>
        )
      }
      else{
        return(
           <span className="side_bar_icon_active" title="New source">
            <svg width="100%" height="100%" label="New" title="New">
              <def>
                  <symbol id="icon-add-solid" viewBox="-8 -8 36 36">
                    <path d="M11 9v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zM10 20c-5.523 0-10-4.477-10-10s4.477-10 10-10v0c5.523 0 10 4.477 10 10s-4.477 10-10 10v0z"/>
                  </symbol>
                </def>
                <use xlinkHref="#icon-add-solid"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "newGraph"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="Empty Graph" style={{marginBottom:'5vh'}}>
              <svg width="100%" height="100%" label="Save" title="Save">
                <def>
                  <symbol id="icon-new-graph-solid" viewBox="-3 -3.5 30 30">
                    <path d="M13.266 10.734l2.719 1.266-2.719 1.266-1.266 2.719-1.266-2.719-2.719-1.266 2.719-1.266 1.266-2.719zM17.016 9.984l-0.938-2.063-2.063-0.938 2.063-0.938 0.938-2.063 0.938 2.063 2.063 0.938-2.063 0.938zM19.031 9.984h1.969v9q0 0.797-0.586 1.406t-1.383 0.609h-14.016q-0.797 0-1.406-0.609t-0.609-1.406v-13.969q0-0.797 0.609-1.406t1.406-0.609h9v2.016h-9v13.969h14.016v-9z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-new-graph-solid"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Empty Graph" style={{marginBottom:'5vh'}}>
              <svg width="50%" height="50%" label="Save" title="Save">
                <def>
                  <symbol id="icon-new-solid" viewBox="0 0 20 20">
                    <path d="M13.266 10.734l2.719 1.266-2.719 1.266-1.266 2.719-1.266-2.719-2.719-1.266 2.719-1.266 1.266-2.719zM17.016 9.984l-0.938-2.063-2.063-0.938 2.063-0.938 0.938-2.063 0.938 2.063 2.063 0.938-2.063 0.938zM19.031 9.984h1.969v9q0 0.797-0.586 1.406t-1.383 0.609h-14.016q-0.797 0-1.406-0.609t-0.609-1.406v-13.969q0-0.797 0.609-1.406t1.406-0.609h9v2.016h-9v13.969h14.016v-9z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-new-graph-solid"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "upload"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="load">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-upload-disk" viewBox="-8 -7.5 49 49">
                    <path d="M18 8l-4-4h-14v26h32v-22h-14zM16 15l7 7h-5v8h-4v-8h-5l7-7z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-upload-disk"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Save">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-save-disk" viewBox="-13 -13 76 76">
                    <path d="M21 33.5c0 1.38 1.12 2.5 2.5 2.5s2.5-1.12 2.5-2.5-1.12-2.5-2.5-2.5-2.5 1.12-2.5 2.5zM4 0c-2.21 0-4 1.79-4 4v40c0 2.21 1.79 4 4 4h40c2.21 0 4-1.79 4-4l-0-34.080c0-1.34-0.529-2.53-1.38-3.39l-5.080-5.060c-0.899-0.91-2.16-1.47-3.54-1.47h-34zM7.2 5h2.8v7.8c0 1.22 0.98 2.2 2.2 2.2h2.6c1.22 0 2.2-0.98 2.2-2.2l-0-7.8h18.8c1.22 0 2.2 0.98 2.2 2.2v9.6c0 1.22-0.98 2.2-2.2 2.2h-28.6c-1.22 0-2.2-0.98-2.2-2.2v-9.6c0-1.22 0.98-2.2 2.2-2.2zM17 33.5c0-3.59 2.91-6.5 6.5-6.5s6.5 2.91 6.5 6.5-2.91 6.5-6.5 6.5-6.5-2.91-6.5-6.5z"></path>                  </symbol>
                </def>
                <use xlinkHref="#icon-save-disk"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "save"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="Save">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-save-disk" viewBox="-16 -15.5 79 79">
                    <path d="M21 33.5c0 1.38 1.12 2.5 2.5 2.5s2.5-1.12 2.5-2.5-1.12-2.5-2.5-2.5-2.5 1.12-2.5 2.5zM4 0c-2.21 0-4 1.79-4 4v40c0 2.21 1.79 4 4 4h40c2.21 0 4-1.79 4-4l-0-34.080c0-1.34-0.529-2.53-1.38-3.39l-5.080-5.060c-0.899-0.91-2.16-1.47-3.54-1.47h-34zM7.2 5h2.8v7.8c0 1.22 0.98 2.2 2.2 2.2h2.6c1.22 0 2.2-0.98 2.2-2.2l-0-7.8h18.8c1.22 0 2.2 0.98 2.2 2.2v9.6c0 1.22-0.98 2.2-2.2 2.2h-28.6c-1.22 0-2.2-0.98-2.2-2.2v-9.6c0-1.22 0.98-2.2 2.2-2.2zM17 33.5c0-3.59 2.91-6.5 6.5-6.5s6.5 2.91 6.5 6.5-2.91 6.5-6.5 6.5-6.5-2.91-6.5-6.5z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-save-disk"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_active" title="Save">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-save-disk" viewBox="-16 -15.5 79 79">
                    <path fill="#333" d="M21 33.5c0 1.38 1.12 2.5 2.5 2.5s2.5-1.12 2.5-2.5-1.12-2.5-2.5-2.5-2.5 1.12-2.5 2.5zM4 0c-2.21 0-4 1.79-4 4v40c0 2.21 1.79 4 4 4h40c2.21 0 4-1.79 4-4l-0-34.080c0-1.34-0.529-2.53-1.38-3.39l-5.080-5.060c-0.899-0.91-2.16-1.47-3.54-1.47h-34zM7.2 5h2.8v7.8c0 1.22 0.98 2.2 2.2 2.2h2.6c1.22 0 2.2-0.98 2.2-2.2l-0-7.8h18.8c1.22 0 2.2 0.98 2.2 2.2v9.6c0 1.22-0.98 2.2-2.2 2.2h-28.6c-1.22 0-2.2-0.98-2.2-2.2v-9.6c0-1.22 0.98-2.2 2.2-2.2zM17 33.5c0-3.59 2.91-6.5 6.5-6.5s6.5 2.91 6.5 6.5-2.91 6.5-6.5 6.5-6.5-2.91-6.5-6.5z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-save-disk"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "capture"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="Take a snap">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-camera" viewBox="-2.5 -3 30 30">
                    <path d="M9.844 21.75q0.188-0.328 2.016-3.516t2.813-4.828l3.656 6.328q-1.125 0.938-3 1.594t-3.328 0.656q-1.125 0-2.156-0.234zM2.438 15h9.703l-3.703 6.328q-2.156-0.797-3.727-2.461t-2.273-3.867zM4.641 5.25l5.063 8.766h-7.5q-0.188-1.125-0.188-2.016 0-1.594 0.773-3.563t1.852-3.188zM21.797 9.984q0.188 0.891 0.188 2.016 0 1.594-0.773 3.563t-1.852 3.188l-4.781-8.25-0.281-0.516h7.5zM21.563 9h-9.703l3.703-6.328q2.156 0.797 3.727 2.461t2.273 3.867zM9.422 10.5l-0.094 0.094-3.656-6.328q1.125-0.938 3-1.594t3.328-0.656q1.125 0 2.156 0.234z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-camera"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Take a snap">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-camera" viewBox="0 0 24 24">
                    <path d="M9.844 21.75q0.188-0.328 2.016-3.516t2.813-4.828l3.656 6.328q-1.125 0.938-3 1.594t-3.328 0.656q-1.125 0-2.156-0.234zM2.438 15h9.703l-3.703 6.328q-2.156-0.797-3.727-2.461t-2.273-3.867zM4.641 5.25l5.063 8.766h-7.5q-0.188-1.125-0.188-2.016 0-1.594 0.773-3.563t1.852-3.188zM21.797 9.984q0.188 0.891 0.188 2.016 0 1.594-0.773 3.563t-1.852 3.188l-4.781-8.25-0.281-0.516h7.5zM21.563 9h-9.703l3.703-6.328q2.156 0.797 3.727 2.461t2.273 3.867zM9.422 10.5l-0.094 0.094-3.656-6.328q1.125-0.938 3-1.594t3.328-0.656q1.125 0 2.156 0.234z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-camera"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "print"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="Print">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-printer1" viewBox="-8 -8 48 48">
                    <path d="M8 2h16v4h-16v-4z"></path>
                    <path d="M30 8h-28c-1.1 0-2 0.9-2 2v10c0 1.1 0.9 2 2 2h6v8h16v-8h6c1.1 0 2-0.9 2-2v-10c0-1.1-0.9-2-2-2zM4 14c-1.105 0-2-0.895-2-2s0.895-2 2-2 2 0.895 2 2-0.895 2-2 2zM22 28h-12v-10h12v10z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-printer1"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Print">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-printer1" viewBox="-8 -8 48 48">
                    <path d="M8 2h16v4h-16v-4z"></path>
                    <path d="M30 8h-28c-1.1 0-2 0.9-2 2v10c0 1.1 0.9 2 2 2h6v8h16v-8h6c1.1 0 2-0.9 2-2v-10c0-1.1-0.9-2-2-2zM4 14c-1.105 0-2-0.895-2-2s0.895-2 2-2 2 0.895 2 2-0.895 2-2 2zM22 28h-12v-10h12v10z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-printer1"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "expand"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="Expand">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-enlarge2" viewBox="-12 -13 57 57">
                    <path d="M32 0v13l-5-5-6 6-3-3 6-6-5-5zM14 21l-6 6 5 5h-13v-13l5 5 6-6z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-enlarge2"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Expand">
              <svg width="45%" height="45%">
                <def>
                  <symbol id="icon-enlarge2" viewBox="-12 -13 57 57">
                    <path d="M32 0v13l-5-5-6 6-3-3 6-6-5-5zM14 21l-6 6 5 5h-13v-13l5 5 6-6z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-enlarge2"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "reset"){
        if (condition === "True"){
          return(
            <span className="side_bar_icon_active" title="Reset graph">
              <svg width="110%" height="110%">
                <def>
                  <symbol id="icon-refresh1" viewBox="-2 -2.1 27 27">
                    <path d="M10 3v2c-0.003 0-0.006 0-0.009 0-2.761 0-5 2.239-5 5 0 1.383 0.561 2.635 1.469 3.54l0 0-1.41 1.41c-1.267-1.267-2.051-3.017-2.051-4.95 0-3.866 3.134-7 7-7 0 0 0 0 0.001 0h-0zM14.95 5.050c1.267 1.267 2.051 3.017 2.051 4.95 0 3.866-3.134 7-7 7-0 0-0 0-0.001 0h0v-2c0.003 0 0.006 0 0.009 0 2.761 0 5-2.239 5-5 0-1.383-0.561-2.635-1.469-3.54l-0-0 1.41-1.41zM10 20l-4-4 4-4v8zM10 8v-8l4 4-4 4z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-refresh1"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Reset graph">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-refresh1" viewBox="-2 -2.1 27 27">
                    <path d="M10 3v2c-0.003 0-0.006 0-0.009 0-2.761 0-5 2.239-5 5 0 1.383 0.561 2.635 1.469 3.54l0 0-1.41 1.41c-1.267-1.267-2.051-3.017-2.051-4.95 0-3.866 3.134-7 7-7 0 0 0 0 0.001 0h-0zM14.95 5.050c1.267 1.267 2.051 3.017 2.051 4.95 0 3.866-3.134 7-7 7-0 0-0 0-0.001 0h0v-2c0.003 0 0.006 0 0.009 0 2.761 0 5-2.239 5-5 0-1.383-0.561-2.635-1.469-3.54l-0-0 1.41-1.41zM10 20l-4-4 4-4v8zM10 8v-8l4 4-4 4z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-refresh1"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "export"){
        if (condition === "True"){      
          return(
            <span className="side_bar_icon_active" title="Export">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-forward" viewBox="-7 -6 47 47">
                    <path d="M14.039 11.954v-9.954l-14.039 14 14.039 14v-10.042c7.135 0 14.177 1.242 17.961 7.136-0.394-10.741-8.43-13.298-17.961-15.14z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-forward"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Export">
              <svg width="48%" height="48%">
                <def>
                  <symbol id="icon-forward" viewBox="-7 -6 47 47">
                    <path d="M14.039 11.954v-9.954l-14.039 14 14.039 14v-10.042c7.135 0 14.177 1.242 17.961 7.136-0.394-10.741-8.43-13.298-17.961-15.14z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-forward"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "link"){
        if (condition === "True"){      
          return(
            <span className="side_bar_icon_active" title="Link Window" style={{marginBottom:'5vh'}}>
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-link" viewBox="-8 -8 48 48">
                    <path d="M13.757 19.868c-0.416 0-0.832-0.159-1.149-0.476-2.973-2.973-2.973-7.81 0-10.783l6-6c1.44-1.44 3.355-2.233 5.392-2.233s3.951 0.793 5.392 2.233c2.973 2.973 2.973 7.81 0 10.783l-2.743 2.743c-0.635 0.635-1.663 0.635-2.298 0s-0.635-1.663 0-2.298l2.743-2.743c1.706-1.706 1.706-4.481 0-6.187-0.826-0.826-1.925-1.281-3.094-1.281s-2.267 0.455-3.094 1.281l-6 6c-1.706 1.706-1.706 4.481 0 6.187 0.635 0.635 0.635 1.663 0 2.298-0.317 0.317-0.733 0.476-1.149 0.476z"></path>
                    <path d="M8 31.625c-2.037 0-3.952-0.793-5.392-2.233-2.973-2.973-2.973-7.81 0-10.783l2.743-2.743c0.635-0.635 1.664-0.635 2.298 0s0.635 1.663 0 2.298l-2.743 2.743c-1.706 1.706-1.706 4.481 0 6.187 0.826 0.826 1.925 1.281 3.094 1.281s2.267-0.455 3.094-1.281l6-6c1.706-1.706 1.706-4.481 0-6.187-0.635-0.635-0.635-1.663 0-2.298s1.663-0.635 2.298 0c2.973 2.973 2.973 7.81 0 10.783l-6 6c-1.44 1.44-3.355 2.233-5.392 2.233z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-link"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Link Window" style={{marginBottom:'5vh'}}>
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-link" viewBox="-7 -7 46 46">
                    <path d="M13.757 19.868c-0.416 0-0.832-0.159-1.149-0.476-2.973-2.973-2.973-7.81 0-10.783l6-6c1.44-1.44 3.355-2.233 5.392-2.233s3.951 0.793 5.392 2.233c2.973 2.973 2.973 7.81 0 10.783l-2.743 2.743c-0.635 0.635-1.663 0.635-2.298 0s-0.635-1.663 0-2.298l2.743-2.743c1.706-1.706 1.706-4.481 0-6.187-0.826-0.826-1.925-1.281-3.094-1.281s-2.267 0.455-3.094 1.281l-6 6c-1.706 1.706-1.706 4.481 0 6.187 0.635 0.635 0.635 1.663 0 2.298-0.317 0.317-0.733 0.476-1.149 0.476z"></path>
                    <path d="M8 31.625c-2.037 0-3.952-0.793-5.392-2.233-2.973-2.973-2.973-7.81 0-10.783l2.743-2.743c0.635-0.635 1.664-0.635 2.298 0s0.635 1.663 0 2.298l-2.743 2.743c-1.706 1.706-1.706 4.481 0 6.187 0.826 0.826 1.925 1.281 3.094 1.281s2.267-0.455 3.094-1.281l6-6c1.706-1.706 1.706-4.481 0-6.187-0.635-0.635-0.635-1.663 0-2.298s1.663-0.635 2.298 0c2.973 2.973 2.973 7.81 0 10.783l-6 6c-1.44 1.44-3.355 2.233-5.392 2.233z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-link"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "lock"){
        if (condition === "True"){      
          return(
            <span className="side_bar_icon_active" title="Locked">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-lock" viewBox="0 0 32 32">
                    <path d="M16 21.915v0c-0.583-0.206-1-0.762-1-1.415 0-0.828 0.672-1.5 1.5-1.5s1.5 0.672 1.5 1.5c0 0.653-0.417 1.209-1 1.415v2.594c0 0.263-0.224 0.491-0.5 0.491-0.268 0-0.5-0.22-0.5-0.491v-2.594zM15 22.5v1.998c0 0.829 0.666 1.502 1.5 1.502 0.828 0 1.5-0.671 1.5-1.502v-1.998c0.607-0.456 1-1.182 1-2 0-1.381-1.119-2.5-2.5-2.5s-2.5 1.119-2.5 2.5c0 0.818 0.393 1.544 1 2v0 0zM9 14v-3.501c0-4.143 3.358-7.499 7.5-7.499 4.134 0 7.5 3.358 7.5 7.499v3.501c1.659 0.005 3 1.35 3 3.009v4.991c0 4.409-3.581 8-7.999 8h-5.002c-4.413 0-7.999-3.582-7.999-8v-4.991c0-1.67 1.342-3.005 3-3.009v0 0zM10 14h1v-3.491c0-3.048 2.462-5.509 5.5-5.509 3.031 0 5.5 2.466 5.5 5.509v3.491h1v-3.507c0-3.586-2.917-6.493-6.5-6.493-3.59 0-6.5 2.908-6.5 6.493v3.507zM12 14h9v-3.499c0-2.486-2.020-4.501-4.5-4.501-2.485 0-4.5 2.009-4.5 4.501v3.499zM8.997 15c-1.103 0-1.997 0.897-1.997 2.006v4.994c0 3.866 3.137 7 6.994 7h5.012c3.863 0 6.994-3.142 6.994-7v-4.994c0-1.108-0.891-2.006-1.997-2.006h-15.005z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-lock"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Locked" style={{position:"absolute",bottom:"5%"}}>
              <svg width="46%" height="46%">
                <def>
                  <symbol id="icon-lock" viewBox="0 0 32 32">
                    <path d="M16 21.915v0c-0.583-0.206-1-0.762-1-1.415 0-0.828 0.672-1.5 1.5-1.5s1.5 0.672 1.5 1.5c0 0.653-0.417 1.209-1 1.415v2.594c0 0.263-0.224 0.491-0.5 0.491-0.268 0-0.5-0.22-0.5-0.491v-2.594zM15 22.5v1.998c0 0.829 0.666 1.502 1.5 1.502 0.828 0 1.5-0.671 1.5-1.502v-1.998c0.607-0.456 1-1.182 1-2 0-1.381-1.119-2.5-2.5-2.5s-2.5 1.119-2.5 2.5c0 0.818 0.393 1.544 1 2v0 0zM9 14v-3.501c0-4.143 3.358-7.499 7.5-7.499 4.134 0 7.5 3.358 7.5 7.499v3.501c1.659 0.005 3 1.35 3 3.009v4.991c0 4.409-3.581 8-7.999 8h-5.002c-4.413 0-7.999-3.582-7.999-8v-4.991c0-1.67 1.342-3.005 3-3.009v0 0zM10 14h1v-3.491c0-3.048 2.462-5.509 5.5-5.509 3.031 0 5.5 2.466 5.5 5.509v3.491h1v-3.507c0-3.586-2.917-6.493-6.5-6.493-3.59 0-6.5 2.908-6.5 6.493v3.507zM12 14h9v-3.499c0-2.486-2.020-4.501-4.5-4.501-2.485 0-4.5 2.009-4.5 4.501v3.499zM8.997 15c-1.103 0-1.997 0.897-1.997 2.006v4.994c0 3.866 3.137 7 6.994 7h5.012c3.863 0 6.994-3.142 6.994-7v-4.994c0-1.108-0.891-2.006-1.997-2.006h-15.005z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-lock"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "unlock"){
        if (condition === "True"){      
          return(
            <span className="side_bar_icon_active" title="UnLocked" style={{position:"absolute",bottom:"5%"}}>
              <svg width="48%" height="48%">
                <def>
                  <symbol id="icon-unlock" viewBox="0 0 32 32">
                    <path d="M24 9.5v3c0 0.826-0.672 1.5-1.5 1.5-0.834 0-1.5-0.672-1.5-1.5v-4c0-2.485-2.020-4.499-4.5-4.499-2.485 0-4.5 2.010-4.5 4.499v7.001c0 0.169 0.009 0.335 0.027 0.499h11.973c1.659 0.005 3 1.35 3 3.009v4.991c0 4.409-3.581 8-7.999 8h-5.002c-4.413 0-7.999-3.582-7.999-8v-4.991c0-1.67 1.342-3.005 3-3.009v-7.501c0-4.143 3.358-7.499 7.5-7.499 4.134 0 7.5 3.358 7.5 7.499v1.001zM23 8.493c0-3.586-2.917-6.493-6.5-6.493-3.59 0-6.5 2.908-6.5 6.493v7.013c0 0.166 0.006 0.331 0.019 0.493h0.981v-7.491c0-3.048 2.462-5.509 5.5-5.509 3.031 0 5.5 2.466 5.5 5.509v3.993c0 0.275 0.232 0.498 0.5 0.498v0c0.276 0 0.5-0.215 0.5-0.49v-4.016zM16 23.915v0 0c-0.583-0.206-1-0.762-1-1.415 0-0.828 0.672-1.5 1.5-1.5s1.5 0.672 1.5 1.5c0 0.653-0.417 1.209-1 1.415v2.594c0 0.263-0.224 0.491-0.5 0.491-0.268 0-0.5-0.22-0.5-0.491v-2.594zM15 24.5v1.998c0 0.829 0.666 1.502 1.5 1.502 0.828 0 1.5-0.671 1.5-1.502v-1.998c0.607-0.456 1-1.182 1-2 0-1.381-1.119-2.5-2.5-2.5s-2.5 1.119-2.5 2.5c0 0.818 0.393 1.544 1 2v0 0zM8.997 17c-1.103 0-1.997 0.897-1.997 2.006v4.994c0 3.866 3.137 7 6.994 7h5.012c3.863 0 6.994-3.142 6.994-7v-4.994c0-1.108-0.891-2.006-1.997-2.006h-15.005z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-unlock"/>
              </svg>
            </span>
          )
        }
        else{
          return(
            <span className="side_bar_icon_disabled" title="Unlocked" style={{position:"absolute",bottom:"5%"}}>
              <svg width="48%" height="48%">
                <def>
                  <symbol id="icon-unlock" viewBox="0 0 32 32">
                    <path d="M24 9.5v3c0 0.826-0.672 1.5-1.5 1.5-0.834 0-1.5-0.672-1.5-1.5v-4c0-2.485-2.020-4.499-4.5-4.499-2.485 0-4.5 2.010-4.5 4.499v7.001c0 0.169 0.009 0.335 0.027 0.499h11.973c1.659 0.005 3 1.35 3 3.009v4.991c0 4.409-3.581 8-7.999 8h-5.002c-4.413 0-7.999-3.582-7.999-8v-4.991c0-1.67 1.342-3.005 3-3.009v-7.501c0-4.143 3.358-7.499 7.5-7.499 4.134 0 7.5 3.358 7.5 7.499v1.001zM23 8.493c0-3.586-2.917-6.493-6.5-6.493-3.59 0-6.5 2.908-6.5 6.493v7.013c0 0.166 0.006 0.331 0.019 0.493h0.981v-7.491c0-3.048 2.462-5.509 5.5-5.509 3.031 0 5.5 2.466 5.5 5.509v3.993c0 0.275 0.232 0.498 0.5 0.498v0c0.276 0 0.5-0.215 0.5-0.49v-4.016zM16 23.915v0 0c-0.583-0.206-1-0.762-1-1.415 0-0.828 0.672-1.5 1.5-1.5s1.5 0.672 1.5 1.5c0 0.653-0.417 1.209-1 1.415v2.594c0 0.263-0.224 0.491-0.5 0.491-0.268 0-0.5-0.22-0.5-0.491v-2.594zM15 24.5v1.998c0 0.829 0.666 1.502 1.5 1.502 0.828 0 1.5-0.671 1.5-1.502v-1.998c0.607-0.456 1-1.182 1-2 0-1.381-1.119-2.5-2.5-2.5s-2.5 1.119-2.5 2.5c0 0.818 0.393 1.544 1 2v0 0zM8.997 17c-1.103 0-1.997 0.897-1.997 2.006v4.994c0 3.866 3.137 7 6.994 7h5.012c3.863 0 6.994-3.142 6.994-7v-4.994c0-1.108-0.891-2.006-1.997-2.006h-15.005z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-unlock"/>
              </svg>
            </span>
          )
        }
    }
    if (type === "live_source"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-live_source" viewBox="-89 -8 53 53">
                    <path d="M16 0c-8.837 0-16 7.163-16 16s7.163 16 16 16 16-7.163 16-16-7.163-16-16-16zM16 28c-6.627 0-12-5.373-12-12s5.373-12 12-12c6.627 0 12 5.373 12 12s-5.373 12-12 12zM10 16c0-3.314 2.686-6 6-6s6 2.686 6 6c0 3.314-2.686 6-6 6s-6-2.686-6-6z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-live_source"/>
              </svg>
          </span>
        )
      }
    }
    if (type === "upload"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-upload" viewBox="-69 -7 40 40">
                  <path d="M8 12h4v-6h3l-5-5-5 5h3v6zM19.338 13.532c-0.21-0.224-1.611-1.723-2.011-2.114-0.265-0.259-0.644-0.418-1.042-0.418h-1.757l3.064 2.994h-3.544c-0.102 0-0.194 0.052-0.24 0.133l-0.816 1.873h-5.984l-0.816-1.873c-0.046-0.081-0.139-0.133-0.24-0.133h-3.544l3.063-2.994h-1.756c-0.397 0-0.776 0.159-1.042 0.418-0.4 0.392-1.801 1.891-2.011 2.114-0.489 0.521-0.758 0.936-0.63 1.449l0.561 3.074c0.128 0.514 0.691 0.936 1.252 0.936h16.312c0.561 0 1.124-0.422 1.252-0.936l0.561-3.074c0.126-0.513-0.142-0.928-0.632-1.449z"></path>
                </symbol>
                </def>
                <use xlinkHref="#icon-upload"/>
              </svg>
          </span>
        )
      }
    }
    if (type === "load_session"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-load" viewBox="-96.5 -10 57 57">
                    <path d="M6 28h20c3.314 0 6-2.686 6-6h-32c0 3.314 2.686 6 6 6zM26 24h2v2h-2v-2zM30 4h-28l-2 16h32z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-load"/>
              </svg>
          </span>
        )
      }
    }  
  }
  if (id === "properties_container"){
    if (type === "filter"){
      return(
        <span className="" title="Graph filters">
          <svg width="100%" height="100%" fill="#666">
            <def>
              <symbol id="icon-equalizer" viewBox="-33 -17 65 65">
                <path d="M14 4v-0.5c0-0.825-0.675-1.5-1.5-1.5h-5c-0.825 0-1.5 0.675-1.5 1.5v0.5h-6v4h6v0.5c0 0.825 0.675 1.5 1.5 1.5h5c0.825 0 1.5-0.675 1.5-1.5v-0.5h18v-4h-18zM8 8v-4h4v4h-4zM26 13.5c0-0.825-0.675-1.5-1.5-1.5h-5c-0.825 0-1.5 0.675-1.5 1.5v0.5h-18v4h18v0.5c0 0.825 0.675 1.5 1.5 1.5h5c0.825 0 1.5-0.675 1.5-1.5v-0.5h6v-4h-6v-0.5zM20 18v-4h4v4h-4zM14 23.5c0-0.825-0.675-1.5-1.5-1.5h-5c-0.825 0-1.5 0.675-1.5 1.5v0.5h-6v4h6v0.5c0 0.825 0.675 1.5 1.5 1.5h5c0.825 0 1.5-0.675 1.5-1.5v-0.5h18v-4h-18v-0.5zM8 28v-4h4v4h-4z"></path>
              </symbol>
            </def>
            <use xlinkHref="#icon-equalizer"/>
          </svg>
        </span>
      )
    }
    if (type === "info"){
      return(
        <span className="" title="Node Informations">
          <svg width="100%" height="100%" fill="#666">
            <def>
              <symbol id="icon-info" viewBox="-23 -12 55 55">
                <path d="M14 9.5c0-0.825 0.675-1.5 1.5-1.5h1c0.825 0 1.5 0.675 1.5 1.5v1c0 0.825-0.675 1.5-1.5 1.5h-1c-0.825 0-1.5-0.675-1.5-1.5v-1z"></path>
                <path d="M20 24h-8v-2h2v-6h-2v-2h6v8h2z"></path>
                <path d="M16 0c-8.837 0-16 7.163-16 16s7.163 16 16 16 16-7.163 16-16-7.163-16-16-16zM16 29c-7.18 0-13-5.82-13-13s5.82-13 13-13 13 5.82 13 13-5.82 13-13 13z"></path>
              </symbol>
            </def>
            <use xlinkHref="#icon-info"/>
          </svg>
        </span>
      )
    }
    if (type === "settings"){
      return(
        <span className="" title="Node Informations">
          <svg width="100%" height="100%" fill="#666">
            <def>
              <symbol id="icon-settings" viewBox="-23 -12 55 55">
                <path d="M32 17.969v-4l-4.781-1.992c-0.133-0.375-0.273-0.738-0.445-1.094l1.93-4.805-2.829-2.828-4.762 1.961c-0.363-0.176-0.734-0.324-1.117-0.461l-2.027-4.75h-4l-1.977 4.734c-0.398 0.141-0.781 0.289-1.16 0.469l-4.754-1.91-2.828 2.828 1.938 4.711c-0.188 0.387-0.34 0.781-0.485 1.188l-4.703 2.011v4l4.707 1.961c0.145 0.406 0.301 0.801 0.488 1.188l-1.902 4.742 2.828 2.828 4.723-1.945c0.379 0.18 0.766 0.324 1.164 0.461l2.023 4.734h4l1.98-4.758c0.379-0.141 0.754-0.289 1.113-0.461l4.797 1.922 2.828-2.828-1.969-4.773c0.168-0.359 0.305-0.723 0.438-1.094l4.782-2.039zM15.969 22c-3.312 0-6-2.688-6-6s2.688-6 6-6 6 2.688 6 6-2.688 6-6 6z"></path>
              </symbol>
            </def>
            <use xlinkHref="#icon-settings"/>
          </svg>
        </span>
      )
    }
    if (type === "search"){
      return(
        <span className="" title="Search">
          <svg width="50%" height="50%" fill="#666">
            <def>
              <symbol id="icon-search" viewBox="0 0 32 32">
                <path d="M31.008 27.231l-7.58-6.447c-0.784-0.705-1.622-1.029-2.299-0.998 1.789-2.096 2.87-4.815 2.87-7.787 0-6.627-5.373-12-12-12s-12 5.373-12 12 5.373 12 12 12c2.972 0 5.691-1.081 7.787-2.87-0.031 0.677 0.293 1.515 0.998 2.299l6.447 7.58c1.104 1.226 2.907 1.33 4.007 0.23s0.997-2.903-0.23-4.007zM12 20c-4.418 0-8-3.582-8-8s3.582-8 8-8 8 3.582 8 8-3.582 8-8 8z"></path>
              </symbol>
            </def>
            <use xlinkHref="#icon-search"/>
          </svg>
        </span>
      )
    }    
  }
  if (id === "toggle_menu"){
    if (type === "new_window"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="Add Window" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-window-new" viewBox="-6 -6 35 32">
                  <path d="M2 2H22V18H2V2ZM2 5H22M12 9V13M10 11H14" />
                </symbol>
              </def>
              <use xlinkHref="#icon-window-new"/>
            </svg>
          </span>
        )
      }
      else{
        return(
          <span className="side_bar_icon_disabled" title="Link Window" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-window-new" viewBox="0 0 20 20">
                    <path d="M9 10v-2h2v2h2v2h-2v2h-2v-2h-2v-2h2zM0 3c0-1.1 0.9-2 2-2h16c1.105 0 2 0.895 2 2v0 14c0 1.105-0.895 2-2 2v0h-16c-1.105 0-2-0.895-2-2v0-14zM2 5v12h16v-12h-16z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-window-new"/>
            </svg>
          </span>
        )
      }
    }
  }
  if (id === "window_bar"){
    if (type === "maximize"){
      if (condition === "True"){      
        return(
            <svg width="100%" height="100%" fill="#FFF">
              <def>
                <symbol id="icon-window-maximize" viewBox="-12 -13.5 45 45">
                  <path d="M0 3c0-1.1 0.9-2 2-2h16c1.105 0 2 0.895 2 2v0 14c0 1.105-0.895 2-2 2v0h-16c-1.105 0-2-0.895-2-2v0-14zM2 5v12h16v-12h-16z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-window-maximize"/>
            </svg>
        )
      }
      else{
        return(
            <svg width="100%" height="100%" fill="#FFF">
              <def>
                <symbol id="icon-window-maximize" viewBox="-10.5 -11 39 39">
                  <path d="M17 1h-10c-0.552 0-1 0.448-1 1v3h-5c-0.552 0-1 0.448-1 1v10c0 0.552 0.448 1 1 1h10c0.552 0 1-0.448 1-1v-3h5c0.552 0 1-0.448 1-1v-10c0-0.552-0.448-1-1-1zM10 14.5c0 0.276-0.224 0.5-0.5 0.5h-7c-0.276 0-0.5-0.224-0.5-0.5v-7c0-0.276 0.224-0.5 0.5-0.5h7c0.276 0 0.5 0.224 0.5 0.5v7zM16 10.5c0 0.276-0.224 0.5-0.5 0.5h-3.5v-5c0-0.552-0.448-1-1-1h-3v-1.5c0-0.276 0.224-0.5 0.5-0.5h7c0.276 0 0.5 0.224 0.5 0.5v7z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-window-maximize"/>
            </svg>
        )
      }
    }
  }
  if(id==="window_upload_source_option"){
    if (type === "upload_input"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="Upload files" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-upload_input" viewBox="-2.5 -2.5 25 25">
                  <path d="M8 12h4v-6h3l-5-5-5 5h3v6zM19.338 13.532c-0.21-0.224-1.611-1.723-2.011-2.114-0.265-0.259-0.644-0.418-1.042-0.418h-1.757l3.064 2.994h-3.544c-0.102 0-0.194 0.052-0.24 0.133l-0.816 1.873h-5.984l-0.816-1.873c-0.046-0.081-0.139-0.133-0.24-0.133h-3.544l3.063-2.994h-1.756c-0.397 0-0.776 0.159-1.042 0.418-0.4 0.392-1.801 1.891-2.011 2.114-0.489 0.521-0.758 0.936-0.63 1.449l0.561 3.074c0.128 0.514 0.691 0.936 1.252 0.936h16.312c0.561 0 1.124-0.422 1.252-0.936l0.561-3.074c0.126-0.513-0.142-0.928-0.632-1.449z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-upload_input"/>
            </svg>
          </span>
        )
      }
    }
  }
  if(id==="window_live_source_option"){
    if (type === "batch_input"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="Add Window" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-batch_input" viewBox="-3 -3 30 30">
                  <path d="M17.016 8.016h-10.031q-0.797 0-1.383 0.586t-0.586 1.383v10.031q0 0.797 0.586 1.383t1.383 0.586h10.031q0.797 0 1.383-0.586t0.586-1.383v-10.031q0-0.797-0.586-1.383t-1.383-0.586zM12.984 20.484h-1.969v-1.5h1.969v1.5zM12.984 18h-1.969q0-0.563-0.375-1.125t-0.891-1.172-0.891-1.289-0.375-1.43q0-0.938 0.492-1.734t1.266-1.266 1.758-0.469 1.758 0.469 1.266 1.266 0.492 1.734q0 0.75-0.375 1.43t-0.891 1.289-0.891 1.172-0.375 1.125zM18 6.516h-12q0-0.656 0.445-1.078t1.055-0.422h9q0.609 0 1.055 0.422t0.445 1.078v0zM17.016 3.516h-10.031q0-0.656 0.445-1.078t1.055-0.422h7.031q0.609 0 1.055 0.422t0.445 1.078v0z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-batch_input"/>
            </svg>
          </span>
        )
      }
      else{
        return(
          <span className="side_bar_icon_disabled" title="Link Window" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-window-new" viewBox="0 0 20 20">
                    <path d="M9 10v-2h2v2h2v2h-2v2h-2v-2h-2v-2h2zM0 3c0-1.1 0.9-2 2-2h16c1.105 0 2 0.895 2 2v0 14c0 1.105-0.895 2-2 2v0h-16c-1.105 0-2-0.895-2-2v0-14zM2 5v12h16v-12h-16z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-window-new"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "realTime_input"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="Add Window" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-stopwatch" viewBox="-6.5 -5.5 45 45">
                  <path d="M16 6.038v-2.038h4v-2c0-1.105-0.895-2-2-2h-6c-1.105 0-2 0.895-2 2v2h4v2.038c-6.712 0.511-12 6.119-12 12.962 0 7.18 5.82 13 13 13s13-5.82 13-13c0-6.843-5.288-12.451-12-12.962zM22.071 26.071c-1.889 1.889-4.4 2.929-7.071 2.929s-5.182-1.040-7.071-2.929c-1.889-1.889-2.929-4.4-2.929-7.071s1.040-5.182 2.929-7.071c1.814-1.814 4.201-2.844 6.754-2.923l-0.677 9.813c-0.058 0.822 0.389 1.181 0.995 1.181s1.053-0.36 0.995-1.181l-0.677-9.813c2.552 0.079 4.94 1.11 6.754 2.923 1.889 1.889 2.929 4.4 2.929 7.071s-1.040 5.182-2.929 7.071z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-stopwatch"/>
            </svg>
          </span>
        )
      }
      else{
        return(
          <span className="side_bar_icon_disabled" title="Link Window" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-window-new" viewBox="0 0 20 20">
                    <path d="M9 10v-2h2v2h2v2h-2v2h-2v-2h-2v-2h2zM0 3c0-1.1 0.9-2 2-2h16c1.105 0 2 0.895 2 2v0 14c0 1.105-0.895 2-2 2v0h-16c-1.105 0-2-0.895-2-2v0-14zM2 5v12h16v-12h-16z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-window-new"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "errorx"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%" fill="#900">
              <def>
                <symbol id="icon-errorx" viewBox="0 -4 40 40">
                  <path d="M16 0c-8.836 0-16 7.164-16 16s7.164 16 16 16 16-7.164 16-16-7.164-16-16-16zM23.914 21.086l-2.828 2.828-5.086-5.086-5.086 5.086-2.828-2.828 5.086-5.086-5.086-5.086 2.828-2.828 5.086 5.086 5.086-5.086 2.828 2.828-5.086 5.086 5.086 5.086z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-errorx"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "correctx"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%" fill="#090">
              <def>
                <symbol id="icon-correctx" viewBox="0 -4 40 40">
                  <path d="M16 0c-8.836 0-16 7.164-16 16s7.164 16 16 16 16-7.164 16-16-7.164-16-16-16zM13.52 23.383l-7.362-7.363 2.828-2.828 4.533 4.535 9.617-9.617 2.828 2.828-12.444 12.445z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-correctx"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "loadingx"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%" fill="#999">
              <def>
                <symbol id="icon-loadingx" viewBox="0 -4 40 40">
                  <path d="M16 9.472c-1.030 0-1.865-0.835-1.865-1.865v-5.596c0-1.030 0.835-1.865 1.865-1.865s1.865 0.835 1.865 1.865v5.596c0 1.030-0.835 1.865-1.865 1.865z"></path>
                  <path d="M16 31.155c-0.644 0-1.166-0.522-1.166-1.166v-5.596c0-0.644 0.522-1.166 1.166-1.166s1.166 0.522 1.166 1.166v5.596c0 0.644-0.522 1.166-1.166 1.166z"></path>
                  <path d="M11.805 10.48c-0.604 0-1.192-0.314-1.516-0.875l-2.798-4.846c-0.483-0.836-0.196-1.906 0.64-2.389s1.906-0.196 2.389 0.64l2.798 4.846c0.483 0.836 0.196 1.906-0.64 2.389-0.275 0.159-0.576 0.235-0.873 0.235z"></path>
                  <path d="M22.995 29.164c-0.363 0-0.715-0.188-0.91-0.525l-2.798-4.846c-0.29-0.502-0.118-1.143 0.384-1.433s1.143-0.118 1.433 0.384l2.798 4.846c0.29 0.502 0.118 1.143-0.384 1.433-0.165 0.095-0.346 0.141-0.524 0.141z"></path>
                  <path d="M8.729 13.436c-0.277 0-0.557-0.070-0.814-0.219l-4.846-2.798c-0.781-0.451-1.048-1.449-0.597-2.229s1.449-1.048 2.229-0.597l4.846 2.798c0.781 0.451 1.048 1.449 0.597 2.229-0.302 0.524-0.851 0.816-1.415 0.816z"></path>
                  <path d="M28.114 23.927c-0.158 0-0.319-0.040-0.465-0.125l-4.846-2.798c-0.446-0.258-0.599-0.828-0.341-1.274s0.828-0.599 1.274-0.341l4.846 2.798c0.446 0.258 0.599 0.828 0.341 1.274-0.173 0.299-0.486 0.466-0.809 0.466z"></path>
                  <path d="M7.607 17.515h-5.596c-0.837 0-1.516-0.678-1.516-1.515s0.678-1.515 1.516-1.515h5.596c0.837 0 1.516 0.678 1.516 1.515s-0.678 1.515-1.516 1.515z"></path>
                  <path d="M29.989 16.933c-0 0 0 0 0 0h-5.596c-0.515-0-0.933-0.418-0.933-0.933s0.418-0.933 0.933-0.933c0 0 0 0 0 0h5.596c0.515 0 0.933 0.418 0.933 0.933s-0.418 0.933-0.933 0.933z"></path>
                  <path d="M3.886 24.394c-0.483 0-0.954-0.251-1.213-0.7-0.386-0.669-0.157-1.525 0.512-1.911l4.846-2.798c0.669-0.387 1.525-0.157 1.911 0.512s0.157 1.525-0.512 1.911l-4.846 2.798c-0.22 0.127-0.461 0.188-0.698 0.188z"></path>
                  <path d="M23.27 12.736c-0.322 0-0.636-0.167-0.809-0.466-0.258-0.446-0.105-1.016 0.341-1.274l4.846-2.798c0.446-0.257 1.016-0.105 1.274 0.341s0.105 1.016-0.341 1.274l-4.846 2.798c-0.147 0.085-0.307 0.125-0.465 0.125z"></path>
                  <path d="M9.004 29.397c-0.218 0-0.438-0.055-0.64-0.172-0.613-0.354-0.823-1.138-0.469-1.752l2.798-4.846c0.354-0.613 1.138-0.823 1.752-0.469s0.823 1.138 0.469 1.752l-2.798 4.846c-0.237 0.411-0.668 0.641-1.112 0.641z"></path>
                  <path d="M20.196 9.664c-0.158 0-0.319-0.040-0.465-0.125-0.446-0.258-0.599-0.828-0.341-1.274l2.798-4.846c0.258-0.446 0.828-0.599 1.274-0.341s0.599 0.828 0.341 1.274l-2.798 4.846c-0.173 0.299-0.486 0.466-0.809 0.466z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-loadingx"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "warningx"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%" fill="#cf7508">
              <def>
                <symbol id="icon-warningx" viewBox="0 -4 40 40">
                  <path d="M31.576 26.211l-11.999-24c-0.677-1.355-2.061-2.211-3.577-2.211s-2.9 0.856-3.577 2.211l-11.999 24c-0.62 1.24-0.554 2.713 0.175 3.893 0.729 1.178 2.016 1.896 3.402 1.896h23.997c1.387 0 2.674-0.718 3.402-1.896 0.729-1.18 0.795-2.653 0.176-3.893zM17.998 27c0 0.553-0.447 1-1 1h-2c-0.553 0-1-0.447-1-1v-2c0-0.553 0.447-1 1-1h2c0.553 0 1 0.447 1 1v2zM17.998 19c0 0.553-0.447 1-1 1h-2c-0.553 0-1-0.447-1-1v-10c0-0.552 0.447-1 1-1h2c0.553 0 1 0.448 1 1v10z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-warningx"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "streamx"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%" fill="#084acf">
              <def>
                <symbol id="icon-streamx" viewBox="-4 -2 30 30">
                  <path d="M9.984 3.984q0-0.844 0.586-1.406t1.43-0.563 1.43 0.563 0.586 1.406-0.586 1.43-1.43 0.586-1.43-0.586-0.586-1.43zM19.781 18.281l-1.453 1.406q-0.844-0.844-2.391-2.414t-1.922-1.945l1.406-1.406 0.328 0.375zM10.031 15.375q-0.844 0.844-2.414 2.391t-1.945 1.922l-1.453-1.406q0.844-0.844 2.414-2.391t1.945-1.922zM13.922 8.625l4.406-4.406 1.453 1.406-4.406 4.406zM10.031 8.578l-1.406 1.406-0.328-0.281-3.984-4.078 1.406-1.406 0.281 0.328h0.047zM9.984 20.016q0-0.844 0.586-1.43t1.43-0.586 1.43 0.586 0.586 1.43-0.586 1.406-1.43 0.563-1.43-0.563-0.586-1.406zM2.016 12q0-0.844 0.563-1.43t1.406-0.586 1.43 0.586 0.586 1.43-0.586 1.43-1.43 0.586-1.406-0.586-0.563-1.43zM18 12q0-0.844 0.586-1.43t1.43-0.586 1.406 0.586 0.563 1.43-0.563 1.43-1.406 0.586-1.43-0.586-0.586-1.43z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-streamx"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "connect"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="Connect" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-connect" viewBox="0 -7 40 40">
                  <path d="M15 8h3c1.105 0 2.103 0.447 2.828 1.172s1.172 1.723 1.172 2.828-0.447 2.103-1.172 2.828-1.723 1.172-2.828 1.172h-3c-0.552 0-1 0.448-1 1s0.448 1 1 1h3c1.657 0 3.158-0.673 4.243-1.757s1.757-2.586 1.757-4.243-0.673-3.158-1.757-4.243-2.586-1.757-4.243-1.757h-3c-0.552 0-1 0.448-1 1s0.448 1 1 1zM9 16h-3c-1.105 0-2.103-0.447-2.828-1.172s-1.172-1.723-1.172-2.828 0.447-2.103 1.172-2.828 1.723-1.172 2.828-1.172h3c0.552 0 1-0.448 1-1s-0.448-1-1-1h-3c-1.657 0-3.158 0.673-4.243 1.757s-1.757 2.586-1.757 4.243 0.673 3.158 1.757 4.243 2.586 1.757 4.243 1.757h3c0.552 0 1-0.448 1-1s-0.448-1-1-1zM8 13h8c0.552 0 1-0.448 1-1s-0.448-1-1-1h-8c-0.552 0-1 0.448-1 1s0.448 1 1 1z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-connect"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "inbox-files"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="List all" style={{marginBottom:'5vh'}}>
            <svg width="100%" height="100%">
              <def>
                <symbol id="icon-inbox-files" viewBox="0 -7 40 40">
                  <path d="M11 16v1.995c0 1.107 0.896 2.005 1.997 2.005h5.005c1.103 0 1.997-0.894 1.997-2.005v-1.995h6.775l-4.375-7h-13.8l-4.375 7h6.775zM3 16l5-8h15l5 8v9h-25v-9zM10 10l-0.6 1h12.2l-0.6-1h-11zM8.7 12l-0.6 1h14.8l-0.6-1h-13.6zM7.5 14l-0.6 1h17.2l-0.6-1h-16z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-inbox-files"/>
            </svg>
          </span>
        )
      }
    }
    if (type === "search"){
      return(
        <span className="" title="Search">
          <svg width="50%" height="50%" fill="#666">
            <def>
              <symbol id="icon-search" viewBox="0 0 32 32">
                <path d="M31.008 27.231l-7.58-6.447c-0.784-0.705-1.622-1.029-2.299-0.998 1.789-2.096 2.87-4.815 2.87-7.787 0-6.627-5.373-12-12-12s-12 5.373-12 12 5.373 12 12 12c2.972 0 5.691-1.081 7.787-2.87-0.031 0.677 0.293 1.515 0.998 2.299l6.447 7.58c1.104 1.226 2.907 1.33 4.007 0.23s0.997-2.903-0.23-4.007zM12 20c-4.418 0-8-3.582-8-8s3.582-8 8-8 8 3.582 8 8-3.582 8-8 8z"></path>
              </symbol>
            </def>
            <use xlinkHref="#icon-search"/>
          </svg>
        </span>
      )
    }
    if (type === "minus"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="Remove" style={{marginBottom:'5vh'}}>
            <svg width="70%" height="100%" fill="#900">
              <def>
                <symbol id="icon-minus" viewBox="0 -3 32 32">
                    <path d="M16 0c-8.836 0-16 7.164-16 16s7.164 16 16 16 16-7.164 16-16-7.164-16-16-16zM24 18h-16v-4h16v4z"></path>
                </symbol>
              </def>
              <use xlinkHref="#icon-minus"/>
            </svg>
          </span>
        )
      }
    }
  }
  if(id==="window_graph_option"){
    if (type === "fieldview"){
      if (condition === "True"){      
        return(
          <span className="side_bar_icon_active" title="">
              <svg width="100%" height="100%">
                <def>
                  <symbol id="icon-fieldview" viewBox="2 -3 20 30">
                    <path d="M18 12.984q1.641 0 2.813 1.195t1.172 2.836-1.172 2.813-2.813 1.172-2.813-1.172-1.172-2.813 1.172-2.836 2.813-1.195zM12 3q1.641 0 2.813 1.172t1.172 2.813-1.172 2.836-2.813 1.195-2.813-1.195-1.172-2.836 1.172-2.813 2.813-1.172zM6 12.984q1.641 0 2.813 1.195t1.172 2.836-1.172 2.813-2.813 1.172-2.813-1.172-1.172-2.813 1.172-2.836 2.813-1.195z"></path>
                  </symbol>
                </def>
                <use xlinkHref="#icon-fieldview"/>
              </svg>
          </span>
        )
      }
    }
  }  
}

export default Icons;
