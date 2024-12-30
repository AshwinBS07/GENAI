import React, { useState, useRef } from "react";
import "./Header.css";
import logo from "./Datanauts_logo.png"

import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faLocationDot,
  faMagnifyingGlass,
} from "@fortawesome/free-solid-svg-icons";

export default function Header() {
  const [isOpen, setIsOpen] = useState(false);
  const [searchText, setSearchText] = useState("");
  const searchRef = useRef(null);

  const toggleLocationDropdown = (e) => {
    e.stopPropagation();
    setIsOpen(!isOpen);
  };

  const handleSearchClick = () => {
    if (searchRef.current) {
      searchRef.current.focus();
    }
  };

  const handleSearchInput = (e) => {
    setSearchText(e.target.textContent);
  };

  const handleOutsideClick = () => {
    setIsOpen(false);
  };

  const handleSearchAction = () => {};

  return (
    <div className="header-container" onClick={handleOutsideClick}>
      <div className="header-items">
        {/* <span>{logo}</span> */}
        {/* <img src="Datanauts_logo.jpg"alt=""/> */}
        <img src={logo} alt="logo"/>
        <div className="Searchbar">
          {/* Search Input */}
          <div
            className="search-input"
            ref={searchRef}
            contentEditable="true"
            suppressContentEditableWarning={true}
            onInput={handleSearchInput}
            onClick={handleSearchClick}
            data-placeholder="Search Here..."
          >
            {!searchText && <span className="placeholder">Search Here...</span>}
          </div>

          {/* Location Dropdown */}
          <div
            className="location-dropdown "
            onClick={toggleLocationDropdown}
            role="button"
            aria-expanded={isOpen}
          >
            <FontAwesomeIcon icon={faLocationDot} />
            <span className="location-arrow">{isOpen ? "▲" : "▼"}</span>
            {isOpen && (
              <ul className="location-dropdown-menu">
                <li>Location 1</li>
                <li>Location 2</li>
                <li>Location 3</li>
              </ul>
            )}
          </div>
          <div className="SearchIcon icons" onClick={handleSearchAction}>
            <FontAwesomeIcon icon={faMagnifyingGlass} />
          </div>
        </div>
        <div className="header-buttons">
          <button className="header-button">Dashboard</button>
          <button className="header-button">About</button>
          <button className="header-button">Privacy Policy</button>
          <button className="log-out">Log Out</button>
        </div>
        {/* <span>Upload Resume</span>
        <span>About</span>
        <span>Privacy Policy</span>
        <button className="log-out">Log Out</button> */}
      </div>
    </div>
  );
}
