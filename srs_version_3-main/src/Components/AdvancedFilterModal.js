import React from "react";
import "./AdvancedFilterModal.css";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faXmarkCircle } from "@fortawesome/free-solid-svg-icons";

const AdvancedFilterModal = ({ isOpen, onClose }) => {
  if (!isOpen) return null;

  return (
    <div className="advanced-filter-modal">
      <div className="modal-content">
        <button className="close-modal" onClick={onClose}>
          <FontAwesomeIcon icon={faXmarkCircle} />
        </button>
        <h3>Advanced Filter</h3>

        <div className="filter-section">
          <div className="Advance-filter-item">
            <h>Experience</h>
            <div className="radio-options">
              <div className="radio-options-btns">
                <input type="radio" id="exp-freshners" name="experience" />
                <label htmlFor="exp-freshners">Freshers</label>
              </div>

              <div className="radio-options-btns">
                <input type="radio" id="exp-1-2-years" name="experience" />
                <label htmlFor="exp-1-2-years">1-2 years</label>
              </div>

              <div className="radio-options-btns">
                <input type="radio" id="exp-2-4-years" name="experience" />
                <label htmlFor="exp-2-4-years">2-4 years</label>
              </div>

              <div className="radio-options-btns">
                <input type="radio" id="exp-4-6-years" name="experience" />
                <label htmlFor="exp-2-4-years">4-8 years</label>
              </div>

              <div className="radio-options-btns">
                <input type="radio" id="exp-8-12-years" name="experience" />
                <label htmlFor="exp-2-4-years">8-12 years</label>
              </div>

              <div className="radio-options-btns">
                <input type="radio" id="exp-15+years" name="experience" />
                <label htmlFor="exp-2-4-years">15+ years</label>
              </div>
            </div>
          </div>

          <div className="Advance-filter-item">
            <h>Job Type</h>
            <div className="checkbox-options">
              <div className="checkbox-options-btns">
                <input type="checkbox" id="full-time" />
                <label htmlFor="full-time">Full-Time</label>
              </div>

              <div className="checkbox-options-btns">
                <input type="checkbox" id="internship" />
                <label htmlFor="part-time">Internship</label>
              </div>

              <div className="checkbox-options-btns">
                <input type="checkbox" id="remote" />
                <label htmlFor="part-time">Remote</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="temporary" />
                <label htmlFor="part-time">Temporary</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="contractbased" />
                <label htmlFor="part-time">Contract Based</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="all" />
                <label htmlFor="part-time">All</label>
              </div>
            </div>
          </div>

          <div className="Advance-filter-item">
            <h>Education</h>
            <div className="checkbox-options">
              <div className="checkbox-options-btns">
                <input type="checkbox" id="high-school" />
                <label htmlFor="high-school">High School</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="intermediate" />
                <label htmlFor="intermediate">Intermediate</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="bachelor-degree" />
                <label htmlFor="bachelor-degree">Bachelor Degree</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="graduation" />
                <label htmlFor="bachelor-degree">Graduation</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="master-degree" />
                <label htmlFor="bachelor-degree">Master Degree</label>
              </div>
              <div className="checkbox-options-btns">
                <input type="checkbox" id="all" />
                <label htmlFor="bachelor-degree">All</label>
              </div>

              {/* Add other checkboxes */}
            </div>
          </div>

          <div className="Advance-filter-item">
            <h>Job Level</h>
            <div className="radio-options">
              <div className="radio-options-btns">
                <input type="radio" id="entry-level" name="job-level" />
                <label htmlFor="entry-level">Entry Level</label>
              </div>
              <div className="radio-options-btns">
                <input type="radio" id="mid-level" name="job-level" />
                <label htmlFor="mid-level">Mid Level</label>
              </div>
              <div className="radio-options-btns">
                <input type="radio" id="expert-level" name="job-level" />
                <label htmlFor="expert-level">Expert Level</label>
              </div>
            </div>
          </div>

          
        </div>
        <button className="advance-find-job-btn">Find Job</button>
      </div>
    </div>
  );
};

export default AdvancedFilterModal;
