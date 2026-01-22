//import { useContext } from 'react';
// import { WindowContext } from './app.jsx';


function WindowsActions(id){
  if (id === "side_bar_menu"){
    //alert("side bar menu")
  }
  if (id === "side_bar_menu_list_new_source"){
    const side_bar_menu=document.getElementById(id);
    side_bar_menu.checked=false;
    const source_container=document.getElementById("source_window_source_container");
    source_container.innerHTML="";
    var new_div= new createElement("div");
    source_container.append(new_div);
  }
}

export default WindowsActions