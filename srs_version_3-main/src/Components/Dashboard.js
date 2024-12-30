import React, { useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useNavigate } from "react-router-dom";
import {
  faMapMarkerAlt,
  faPhone,
  faEnvelope,
  faBriefcase,
  faLocationDot,
  faEye,
} from "@fortawesome/free-solid-svg-icons";

import {
  faArrowRight,
  faTrashAlt,
  faEllipsisH,
  faUpload,
  faLink,
} from "@fortawesome/free-solid-svg-icons";
import "./Dashboard.css";
import Header from "./Header";
import {
  faGithub,
  faTwitterSquare,
  faLinkedin,
  faInstagram,
} from "@fortawesome/free-brands-svg-icons";

export default function Dashboard() {

  const navigate=useNavigate()
  const [activeMenu, setActiveMenu] = useState("My Board");
  const [showProfileEdit, setShowProfileEdit] = useState(false);
  const [activeTab, setActiveTab] = useState("Personal");
  const [activeResumeMenu, setActiveResumeMenu] = useState(null);
  const [showUploadResumeModal, setShowUploadResumeModal] = useState(false);
  const [resumeName, setResumeName] = useState("");
  const [uploadedFile, setUploadedFile] = useState(null);
  const [showAccountEdit, setShowAccountEdit] = useState(false);
  const [isPrivacyPolicyEnabled, setIsPrivacyPolicyEnabled] = useState(false);
  const [isResumePrivacyEnabled, setIsResumePrivacyEnabled] = useState(false);
  const [showCurrentPassword, setShowCurrentPassword] = useState(false);
  const [showNewPassword, setShowNewPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);

  const [userResumes, setUserResumes] = useState([
    "Resumes1.pdf",
    "resumes2.pdf",
  ]);
  const maxResumes = 3;

  const dashboardData = {
    user: {
      name: "Luis",
      email: "luis@example.com",
    },

    appliedJobsCount: 20,
    savedJobsCount: 10,
    jobAlertsCount: 5,
    menuItems: [
      { name: "My Board", icon: "🏠" },
      { name: "Applied Jobs", icon: "📋" },
      { name: "Job Suggestions", icon: "🎯" },
      { name: "Job Alerts", icon: "🔔", count: "(20)" },
      { name: "Saved Jobs", icon: "💾" },
      { name: "My Profile", icon: "⚙️" },
    ],

    recentApplications: [
      {
        id: 1,
        job: "Software Engineer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Remote",
        location: "New York, USA",
        salary: "$120,000/year",
        status: "Active",
        date: "2024-12-10",
        action: "View Details",
      },
      {
        id: 2,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Inactive",
        date: "2024-12-12",
        action: "View Details",
      },

      {
        id: 3,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },

      {
        id: 4,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Inactive",
        date: "2024-12-12",
        action: "View Details",
      },

      {
        id: 5,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 6,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 7,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 8,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 9,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
      {
        id: 10,
        job: "Frontend Developer",
        companyLogo: "https://via.placeholder.com/50",
        shift: "Hybrid",
        location: "San Francisco, USA",
        salary: "$90,000/year",
        status: "Active",
        date: "2024-12-12",
        action: "View Details",
      },
    ],

    profile: {
      fullName: "John Doe",
      role: "Senior Developer",
      experience: "5-10 years",
      education: "Master's in Computer Science",
      personalWebsite: "https://johndoe.dev",
    },
    resumeData: [
      {
        id: 1,
        title: "Senior Developer Resume",
        fileUrl: "https://example.com/resumes/senior-developer.pdf",
        uploadedDate: "2024-12-10",
      },
      {
        id: 2,
        title: "Frontend Developer Resume",
        fileUrl: "https://example.com/resumes/frontend-developer.pdf",
        uploadedDate: "2024-12-05",
      },
    ],
  };

  const openResumeMenu = (id) => {
    // console.log(`Open menu for resume ID: ${id}`);
    setActiveResumeMenu((prev) => (prev === id ? null : id));
  };

  const handleEditResume = (id) => {
    console.log("Edit resume with ID:", id);
    setActiveResumeMenu(null);
  };

  const handleDltResume = (id) => {
    console.log("Delete resume with ID:", id);
    setActiveResumeMenu(null);
  };
  const handleOpenModal = () => {
    if (userResumes.length >= maxResumes) {
      setShowUploadResumeModal(false);
    } else {
      setShowUploadResumeModal(true);
    }
  };
  const handleCloseModal = () => {
    setShowUploadResumeModal(false);
    setResumeName("");
    setUploadedFile(null);
  };

  const handleUploadResume = () => {
    if (!resumeName || !uploadedFile) {
      alert("Please fill in all fields and upload a file.");
      return;
    }
    // Logic to upload the resume
    // console.log("Uploaded:", { resumeName, uploadedFile });
    handleCloseModal();
  };

  const handlePhotoUpload = (event) => {
    const file = event.target.files[0];
    if (file) {
      // Validate file type and size
      if (!["image/png", "image/jpeg"].includes(file.type)) {
        alert("Please upload a valid PNG or JPEG image.");
        return;
      }
      if (file.size > 5 * 1024 * 1024) {
        alert("File size should not exceed 5MB.");
        return;
      }

      // Process the file (e.g., upload to the server or preview it)
      // console.log('Uploaded file:', file);
    }
  };

  const [socialLinks, setSocialLinks] = useState([
    { platform: "Github", link: "" },
    { platform: "Twitter", link: "" },
    { platform: "LinkedIn", link: "" },
    { platform: "Instagram", link: "" },
  ]);

  // Map for platform icons
  const platformIcons = {
    Github: faGithub,
    Twitter: faTwitterSquare,
    LinkedIn: faLinkedin,
    Instagram: faInstagram,
    default: faLink,
  };

  const handleAddNewLink = () => {
    setSocialLinks([...socialLinks, { platform: "", link: "" }]);
  };

  const handleRemoveLink = (index) => {
    const updatedLinks = socialLinks.filter((_, i) => i !== index);
    setSocialLinks(updatedLinks);
  };

  const handleInputChange = (index, field, value) => {
    const updatedLinks = [...socialLinks];
    updatedLinks[index][field] = value;
    setSocialLinks(updatedLinks);
  };

  const handleApply=(jobId)=>{
    navigate(`/jobdetails/${jobId}`);
  };

  const renderSettingsContent = () => {
    // console.log("active", activeTab);
    switch (activeTab) {
      case "Personal":
        return (
          <div>
            <div className="personal-section">
              <h2>Basic Information</h2>

              <div className="personal-section-content">
                <div className="photo-class">
                  <h>Profile Picture</h>
                  <div className="photo-upload">
                    <FontAwesomeIcon icon={faUpload} />
                    <p>
                      <h>Browse photo</h> or drop here (Max: 5MB, PNG/JPEG only)
                    </p>
                    <input
                      type="file"
                      accept="image/png, image/jpeg"
                      className="photo-input"
                      onChange={handlePhotoUpload}
                    />
                  </div>
                </div>

                <div className="personal-details">
                  <div className="details-row-row-group">
                    <div>
                      <label>Full Name</label>
                      <input
                        type="text"
                        defaultValue={dashboardData.profile.fullName}
                        readOnly={!showProfileEdit}
                      />
                    </div>

                    <div>
                      <label>Role</label>
                      <input
                        type="text"
                        defaultValue={dashboardData.profile.role}
                        readOnly={!showProfileEdit}
                      />
                    </div>
                  </div>
                  <div className="details-row-row-group">
                    <div>
                      <label>Experience</label>
                      <select
                        defaultValue={dashboardData.profile.experience}
                        disabled={!showProfileEdit}
                      >
                        <option>0-2 years</option>
                        <option>3-5 years</option>
                        <option>5-10 years</option>
                        <option>10+ years</option>
                      </select>

                      <label>Education</label>
                      <select
                        defaultValue={dashboardData.profile.education}
                        disabled={!showProfileEdit}
                      >
                        <option>Bachelor's</option>
                        <option>Master's</option>
                        <option>PhD</option>
                      </select>
                    </div>
                  </div>
                  <div>
                    <label>Personal Website</label>
                    <div style={{ position: "relative" }}>
                      <input
                        type="text"
                        defaultValue={dashboardData.profile.personalWebsite}
                        readOnly={!showProfileEdit}
                        className="full-width-input"
                      />
                      <span className="link-icon">🔗</span>
                    </div>
                  </div>
                </div>
              </div>
              <button
                onClick={() => setShowProfileEdit(!showProfileEdit)}
                className="personal-section-buttons"
              >
                {showProfileEdit ? "Save" : "Edit"}
              </button>

              <div className="resume-section">
                <h3>Your CV/Resume</h3>
                <div className="max-resume-info">
                  <small>Max resumes allowed: {maxResumes}</small>
                </div>

                <div className="resume-container">
                  <div
                    className={`upload-box $userResumes.length >= maxResumes ? "disabled" : ""}`}
                    onClick={handleOpenModal}
                  >
                    <div className="upload-content">
                      <span className="upload-icon">+</span>
                      <div>
                        <p className="big-text">Upload CV/Resume</p>
                        <p className="small-text">
                          Browse file or drop here (PDF only)
                        </p>
                      </div>
                    </div>
                  </div>

                  {/* Resume List */}

                  {dashboardData.resumeData.map((resume) => (
                    <div key={resume.id} className="resume-item">
                      <span className="resume-icon">📄</span>
                      <div className="resume-details">
                        <p>{resume.title}</p>
                      </div>
                      <div
                        className="resume-menu"
                        onClick={() => openResumeMenu(resume.id)}
                      >
                        ⋮
                      </div>
                      {activeResumeMenu === resume.id && (
                        <div className="resume-menu-dropdown">
                          <button onClick={() => handleEditResume(resume.id)}>
                            Edit
                          </button>
                          <button onClick={() => handleDltResume(resume.id)}>
                            Delete
                          </button>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
                {showUploadResumeModal && (
                  <div className="modal-overlay">
                    <div className="upload-resume-modal">
                      <div className="modal-header">
                        <h3>Upload Resume</h3>
                        <button
                          onClick={handleCloseModal}
                          className="close-modal-btn"
                        >
                          &times;
                        </button>
                      </div>
                      <div className="modal-body">
                        <label>Resume Name</label>
                        <input
                          type="text"
                          placeholder="Enter Resume Name"
                          value={resumeName}
                          onChange={(e) => setResumeName(e.target.value)}
                        />
                        <label>Upload CV</label>
                        <input
                          type="file"
                          accept=".pdf"
                          onChange={(e) => setUploadedFile(e.target.files[0])}
                        />
                      </div>
                      <div className="modal-footer">
                        <button
                          onClick={handleCloseModal}
                          className="cancel-btn"
                        >
                          Cancel
                        </button>
                        <button
                          onClick={handleUploadResume}
                          className="upload-btn"
                        >
                          Upload
                        </button>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        );

      case "profile":
        return (
          <div className="profile-details">
            <h2>Profile Information</h2>

            {/* Nationality and Date of Birth */}
            <div className="profile-details-row">
              <div>
                <label>Nationality</label>
                <select
                  defaultValue={dashboardData.profile.nationality}
                  disabled={!showProfileEdit}
                >
                  <option>USA</option>
                  <option>Canada</option>
                  {/* Add other nationalities here */}
                </select>
              </div>
              <div>
                <label>Date of Birth</label>
                <input
                  type="date"
                  defaultValue={dashboardData.profile.dob}
                  readOnly={!showProfileEdit}
                />
              </div>
            </div>

            {/* Gender and Martial Status */}
            <div className="profile-details-row">
              <div>
                <label>Gender</label>
                <select
                  defaultValue={dashboardData.profile.gender}
                  disabled={!showProfileEdit}
                >
                  <option>Male</option>
                  <option>Female</option>
                  <option>Other</option>
                </select>
              </div>
              <div>
                <label>Martial Status</label>
                <select
                  defaultValue={dashboardData.profile.maritalStatus}
                  disabled={!showProfileEdit}
                >
                  <option>Single</option>
                  <option>Married</option>
                  <option>Divorced</option>
                </select>
              </div>
            </div>

            {/* Phone and Email */}
            <div className="profile-details-row">
              <div>
                <label>Phone No</label>
                <input
                  type="text"
                  defaultValue={dashboardData.profile.phone}
                  readOnly={!showProfileEdit}
                />
              </div>
              <div>
                <label>Email</label>
                <input
                  type="email"
                  defaultValue={dashboardData.profile.email}
                  readOnly={!showProfileEdit}
                />
              </div>
            </div>

            {/* Current Company and Role */}
            <div className="profile-details-row">
              <div>
                <label>Current Company</label>
                <input
                  type="text"
                  defaultValue={dashboardData.profile.currentCompany}
                  readOnly={!showProfileEdit}
                />
              </div>
              <div>
                <label>Current Role</label>
                <input
                  type="text"
                  defaultValue={dashboardData.profile.currentRole}
                  readOnly={!showProfileEdit}
                />
              </div>
            </div>

            <div className="profile-details-row">
              <div>
                <label>Current Location</label>
                <input
                  type="text"
                  defaultValue={dashboardData.profile.currentCompany}
                  readOnly={!showProfileEdit}
                />
              </div>
              <div>
                <label>Notice period in days</label>
                <input
                  type="Number"
                  defaultValue={dashboardData.profile.currentRole}
                  readOnly={!showProfileEdit}
                />
              </div>
            </div>

            {/* Bio */}
            <div className="profile-bio">
              <label>Bio</label>

              <textarea
                defaultValue={dashboardData.profile.bio}
                readOnly={!showProfileEdit}
                placeholder="Write down your bio..."
              />
            </div>

            {/* Edit/Save Button */}
            <button
              onClick={() => setShowProfileEdit(!showProfileEdit)}
              className="profile-section-buttons"
            >
              {showProfileEdit ? "Save" : "Edit"}
            </button>
          </div>
        );
      case "social":
        return (
          <div className="social-links-section">
            <h2>Social Links</h2>

            {socialLinks.map((social, index) => (
              <div className="social-link-row" key={index}>
                <div className="social-platform">
                  <FontAwesomeIcon
                    icon={
                      platformIcons[social.platform] || platformIcons.default
                    }
                    className="platform-icon"
                  />
                  <select
                    value={social.platform}
                    onChange={(e) =>
                      handleInputChange(index, "platform", e.target.value)
                    }
                  >
                    <option value="">Select Platform</option>
                    <option value="Github">Github</option>
                    <option value="Twitter">Twitter</option>
                    <option value="LinkedIn">LinkedIn</option>
                    <option value="Instagram">Instagram</option>
                  </select>
                </div>
                <div className="social-url">
                  <input
                    type="text"
                    placeholder="Profile link/URL"
                    value={social.link}
                    onChange={(e) =>
                      handleInputChange(index, "link", e.target.value)
                    }
                    className="social-url-input"
                  />

                  <button
                    className="social-remove-btn"
                    onClick={() => handleRemoveLink(index)}
                  >
                    <span style={{ backgroundColor: "white" }}>✖️</span>
                  </button>
                </div>
              </div>
            ))}

            <button className="add-social-link-btn" onClick={handleAddNewLink}>
              + Add New Social Link
            </button>

            <div className="action-buttons">
              <button className="social-save-btn">Save</button>
            </div>
          </div>
        );

      case "account":
        return (
          <div className="account-settings-section">
            <h2>Contact Info</h2>

            {/* Address Section */}
            <div className="account-input-container">
              <label>Address</label>
              <div className="account-input-box">
                <FontAwesomeIcon icon={faMapMarkerAlt} />
                <input
                  type="text"
                  placeholder="Enter your Address"
                  className="account-full-width"
                  // defaultValue={dashboardData.account.address}
                  readOnly={!showAccountEdit}
                />
              </div>
            </div>

            {/* Phone Section */}
            <div className="account-input-container">
              <label>Phone</label>
              <div className="account-input-box">
                <FontAwesomeIcon icon={faPhone} />
                <select
                  className="account-country-dropdown"
                  disabled={!showAccountEdit}
                  // defaultValue={dashboardData.account.phoneCountry}
                >
                  <option value="US">+1 USA</option>
                  <option value="IN">+91 India</option>
                </select>
                <input
                  type="text"
                  placeholder="Enter phone number"
                  className="account-full-width"
                  // defaultValue={dashboardData.account.phone}
                  readOnly={!showAccountEdit}
                />
              </div>
            </div>

            {/* Email Section */}
            <div className="account-input-container">
              <label>Email</label>
              <div className="account-input-box">
                <FontAwesomeIcon icon={faEnvelope} />
                <input
                  type="email"
                  placeholder="Enter your email"
                  className="account-full-width"
                  // defaultValue={dashboardData.account.email}
                  readOnly={!showAccountEdit}
                />
              </div>

              <div className="job-alerts-section">
                {/* <h2>Job Alerts</h2> */}

                <div className="job-alerts-container">
                  {/* Role and Location Section */}
                  <div className="job-alerts-input-container">
                    <div className="job-input-box">
                      <FontAwesomeIcon icon={faBriefcase} />
                      <input
                        type="text"
                        placeholder="Enter your desired role"
                        className="job-input"
                        // defaultValue={dashboardData.account.email}
                        readOnly={!showAccountEdit}
                      />
                    </div>

                    <div className="job-input-box">
                      <FontAwesomeIcon icon={faLocationDot} />
                      <input
                        type="text"
                        placeholder="Enter preferred location"
                        className="job-input"
                        // defaultValue={dashboardData.account.email}
                        readOnly={!showAccountEdit}
                      />
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Edit/Save Button */}
            <button
              onClick={() => setShowAccountEdit(!showAccountEdit)}
              className="account-section-buttons"
            >
              {showAccountEdit ? "Save" : "Edit"}
            </button>

            <div className="notification-settings-container">
              <h2>Notifications</h2>
              <div className="notification-checkbox-row">
                <div className="checkbox-container">
                  <input type="checkbox" id="checkbox1" />
                  <label htmlFor="checkbox1">
                    Notify me when employers shortlist me
                  </label>
                </div>
                <div
                  className="checkbox-container"
                  style={{ marginLeft: "50px" }}
                >
                  <input type="checkbox" id="checkbox2" />
                  <label htmlFor="checkbox2">
                    Notify me when my applied job expires
                  </label>
                </div>
              </div>

              {/* Second Checkbox Row */}
              <div className="notification-checkbox-row">
                <div className="checkbox-container">
                  <input type="checkbox" id="checkbox3" />
                  <label htmlFor="checkbox3">
                    Notify me when I have 3 job alerts
                  </label>
                </div>
                <div
                  className="checkbox-container"
                  style={{ marginLeft: "90px" }}
                >
                  <input type="checkbox" id="checkbox4" />
                  <label htmlFor="checkbox4">
                    Notify me when employers save my profile
                  </label>
                </div>
              </div>

              {/* Third Checkbox Row */}
              <div className="notification-checkbox-row">
                <div className="checkbox-container">
                  <input type="checkbox" id="checkbox5" />
                  <label htmlFor="checkbox5">
                    Notify me when employers reject me
                  </label>
                </div>
              </div>
            </div>

            <div className="privacy-settings-section">
              <h2>Privacy Settings</h2>

              <div className="privacy-settings-container">
                {/* Privacy Policy */}
                <div className="privacy-item">
                  <label>Privacy Policy</label>
                  <div className="Settings-toggle-container">
                    <label className="switch">
                      <input
                        type="checkbox"
                        checked={isPrivacyPolicyEnabled}
                        onChange={() =>
                          setIsPrivacyPolicyEnabled(!isPrivacyPolicyEnabled)
                        }
                      />
                      <span className="slider"></span>
                    </label>
                    <span className="toggle-text">
                      {isPrivacyPolicyEnabled
                        ? "Your Profile Is Public Now"
                        : "Your Profile Is Private "}
                    </span>
                  </div>
                </div>

                {/* Resume Privacy */}
                <div className="privacy-item">
                  <label>Resume Privacy</label>
                  <div className="Settings-toggle-container">
                    <label className="switch">
                      <input
                        type="checkbox"
                        checked={isResumePrivacyEnabled}
                        onChange={() =>
                          setIsResumePrivacyEnabled(!isResumePrivacyEnabled)
                        }
                      />
                      <span className="slider"></span>
                    </label>
                    <span className="toggle-text">
                      {isResumePrivacyEnabled
                        ? "Your Resume Is Visible To Recruiter "
                        : "Your Resume Is Hidden From All"}
                    </span>
                  </div>
                </div>
              </div>
            </div>

            <div className="password-change-section">
              <h2>Change Password</h2>

              <div className="password-change-container">
                {/* Current Password */}
                <div className="password-input-container">
                  <label>Current Password</label>
                  <div className="password-input-box">
                    <input
                      type={showCurrentPassword ? "text" : "password"}
                      placeholder="Enter current password"
                      className="account-full-width"
                    />
                    <span
                      className="password-eye-icon"
                      onClick={() =>
                        setShowCurrentPassword(!showCurrentPassword)
                      }
                    >
                      {showCurrentPassword ? "👁" : "👁‍🗨"}
                    </span>
                  </div>
                </div>

                {/* New Password */}
                <div className="password-input-container">
                  <label>New Password</label>
                  <div className="password-input-box">
                    <input
                      type={showNewPassword ? "text" : "password"}
                      placeholder="Enter new password"
                      className="account-full-width"
                    />
                    <span
                      className="password-eye-icon"
                      onClick={() => setShowNewPassword(!showNewPassword)}
                    >
                      {showNewPassword ? "👁" : "👁‍🗨"}
                    </span>
                  </div>
                </div>

                {/* Confirm Password */}
                <div className="password-input-container">
                  <label>Confirm Password</label>
                  <div className="password-input-box">
                    <input
                      type={showConfirmPassword ? "text" : "password"}
                      placeholder="Re-enter new password"
                      className="account-full-width"
                    />
                    <span
                      className="password-eye-icon"
                      onClick={() =>
                        setShowConfirmPassword(!showConfirmPassword)
                      }
                    >
                      {showConfirmPassword ? "👁" : "👁‍🗨"}
                    </span>
                  </div>
                </div>
              </div>

              {/* Save Button */}
              <button className="account-section-buttons">Save</button>
            </div>

            <div className="dlt-settings-section">
              <h2>Delete Your Account</h2>
              <div className="delete-account-section">
                <p className="delete-account-warning">
                  If you delete your account, you will no longer be able to get
                  information about the matched jobs, following employers, job
                  alerts, shortlisted jobs, and more. You will be abandoned from
                  all the services of <strong>[Your Platform Name]</strong>.
                </p>

                <button className="delete-account-button">
                  × Delete Account
                </button>
              </div>
            </div>
          </div>
        );

      default:
        return <div className="tab-content">Select a setting</div>;
    }
  };

  const renderContent = () => {
    switch (activeMenu) {
      case "My Board":
        return (
          <div className="dashboard-overview">
            <h2>Welcome, {dashboardData.user?.name || "User"}!</h2>
            <div className="overview-cards">
              <div className="card">
                <div className="card-content">
                  <span className="icon">📋</span>
                  <div>
                    <h3>Applied Jobs</h3>
                    <p>{dashboardData.appliedJobsCount}</p>
                  </div>
                </div>
              </div>
              <div className="card">
                <div className="card-content">
                  <span className="icon">💾</span>
                  <div>
                    <h3>Saved Jobs</h3>
                    <p>{dashboardData.savedJobsCount}</p>
                  </div>
                </div>
              </div>
              <div className="card">
                <div className="card-content">
                  <span className="icon">🔔</span>
                  <div>
                    <h3>Job Alerts</h3>
                    <p>{dashboardData.jobAlertsCount}</p>
                  </div>
                </div>
              </div>
            </div>
            <div className="recent-applications">
              <div className="recent-applications-header">
                <h3>Recent Applications</h3>
                <button className="custom-view-all-btn">
                  View All <FontAwesomeIcon icon={faArrowRight} />
                </button>
              </div>
              <table className="custom-table">
                <thead>
                  <tr>
                    <th>Job Details</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData.recentApplications.slice(0, 5).map((app) => (
                    <tr key={app.id}>
                      <td>
                        <div className="customjob-details">
                          <div className="customjob-header">
                            <img
                              src={app.companyLogo}
                              alt="Company Logo"
                              className="customcompany-logo"
                            />
                            <div className="customheaderinside">
                              <h4>{app.job}</h4>
                              <span className="customjob-shift">
                                {app.shift}
                              </span>
                            </div>
                          </div>
                          <div className="customjob-info">
                            <span className="customlocation">
                              <span className="icon">📍</span>
                              {app.location}
                            </span>
                            <span className="customsalary">
                              <span className="icon">💰</span>
                              {app.salary}
                            </span>
                          </div>
                        </div>
                      </td>
                      <td>
                        <span>{app.date}</span>
                      </td>

                      <td>
                        <span
                          className={`customstatus ${app.status.toLowerCase()}`}
                        >
                          {app.status}
                        </span>
                      </td>

                      <td>
                        <button className="customview-details">
                          View Details
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );

      case "Applied Jobs":
        return (
          <div className="applied-jobs">
            <h2>Applied Jobs ({dashboardData.appliedJobsCount})</h2>
            <div className="recent-applications">
              <table className="custom-table">
                <thead>
                  <tr>
                    <th>Job Details</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData.recentApplications.map((app) => (
                    <tr key={app.id}>
                      <td>
                        <div className="customjob-details">
                          <div className="customjob-header">
                            <img
                              src={app.companyLogo}
                              alt="Company Logo"
                              className="customcompany-logo"
                            />
                            <div className="customheaderinside">
                              <h4>{app.job}</h4>
                              <span className="customjob-shift">
                                {app.shift}
                              </span>
                            </div>
                          </div>
                          <div className="customjob-info">
                            <span className="customlocation">
                              <span className="icon">📍</span> {app.location}
                            </span>
                            <span className="customsalary">
                              <span className="icon">💰</span> {app.salary}
                            </span>
                          </div>
                        </div>
                      </td>
                      <td>
                        <span>{app.date}</span>
                      </td>
                      <td>
                        <span
                          className={`customstatus ${app.status.toLowerCase()}`}
                        >
                          {app.status}
                        </span>
                      </td>
                      <td>
                        <button className="customview-details">
                          View Details
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );

      case "Job Suggestions":
        return (
          <div className="applied-jobs">
            <h2>Job Suggestions ({dashboardData.appliedJobsCount})</h2>
            <p className="max-resume-info" style={{ marginTop: "20px" }}>
              Select A Resume for Job Recommendations
            </p>
            <div className="job-sugg-resumelist">
              {dashboardData.resumeData.map((resume) => (
                <div key={resume.id} className="job-resume-item">
                  <span className="resume-icon">📄</span>
                  <div className="resume-details">
                    <p>{resume.title}</p>
                  </div>
                </div>
              ))}
            </div>
            <div className="recent-applications">
              <table className="custom-table">
                <thead>
                  <tr>
                    <th>Job Details</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData.recentApplications.map((app) => (
                    <tr key={app.id}>
                      <td>
                        <div className="customjob-details">
                          <div className="customjob-header">
                            <img
                              src={app.companyLogo}
                              alt="Company Logo"
                              className="customcompany-logo"
                            />
                            <div className="customheaderinside">
                              <h4>{app.job}</h4>
                              <span className="customjob-shift">
                                {app.shift}
                              </span>
                            </div>
                          </div>
                          <div className="customjob-info">
                            <span className="customlocation">
                              <span className="icon">📍</span> {app.location}
                            </span>
                            <span className="customsalary">
                              <span className="icon">💰</span> {app.salary}
                            </span>
                          </div>
                        </div>
                      </td>
                      <td>
                        <span>{app.date}</span>
                      </td>
                      <td>
                        <span
                          className={`customstatus ${app.status.toLowerCase()}`}
                        >
                          {app.status}
                        </span>
                      </td>
                      <td>
                        <button className="customview-details" onClick={()=>handleApply(Dashboard.id)}>
                          Apply Now
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );

      case "Job Alerts":
        return (
          <div className="applied-jobs">
            <h2>Job Alerts ({dashboardData.appliedJobsCount})</h2>
            <div className="recent-applications">
              <table className="custom-table">
                <thead>
                  <tr>
                    <th>Job Details</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData.recentApplications.map((app) => (
                    <tr key={app.id}>
                      <td>
                        <div className="customjob-details">
                          <div className="customjob-header">
                            <img
                              src={app.companyLogo}
                              alt="Company Logo"
                              className="customcompany-logo"
                            />
                            <div className="customheaderinside">
                              <h4>{app.job}</h4>
                              <span className="customjob-shift">
                                {app.shift}
                              </span>
                            </div>
                          </div>
                          <div className="customjob-info">
                            <span className="customlocation">
                              <span className="icon">📍</span> {app.location}
                            </span>
                            <span className="customsalary">
                              <span className="icon">💰</span> {app.salary}
                            </span>
                          </div>
                        </div>
                      </td>
                      <td>
                        <span>{app.date}</span>
                      </td>
                      <td>
                        <span
                          className={`customstatus ${app.status.toLowerCase()}`}
                        >
                          {app.status}
                        </span>
                      </td>
                      <td>
                        <button className="customview-details">
                          Apply Now
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );

      case "Saved Jobs":
        return (
          <div className="applied-jobs">
            <h2>Saved Jobs ({dashboardData.savedJobsCount})</h2>
            <div className="recent-applications">
              <table className="custom-table">
                <thead>
                  <tr>
                    <th>Job Details</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData.recentApplications.map((app) => (
                    <tr key={app.id}>
                      <td>
                        <div className="customjob-details">
                          <div className="customjob-header">
                            <img
                              src={app.companyLogo}
                              alt="Company Logo"
                              className="customcompany-logo"
                            />
                            <div className="customheaderinside">
                              <h4>{app.job}</h4>
                              <span className="customjob-shift">
                                {app.shift}
                              </span>
                            </div>
                          </div>
                          <div className="customjob-info">
                            <span className="customlocation">
                              <span className="icon">📍</span> {app.location}
                            </span>
                            <span className="customsalary">
                              <span className="icon">💰</span> {app.salary}
                            </span>
                          </div>
                        </div>
                      </td>
                      <td>
                        <span>{app.date}</span>
                      </td>
                      <td>
                        <span
                          className={`customstatus ${app.status.toLowerCase()}`}
                        >
                          {app.status}
                        </span>
                      </td>
                      <td>
                        <button className="customview-details">
                          Apply Now
                        </button>
                      </td>

                      <td className="push-right">
                        <div className="action-buttons">
                          <button
                            className="remove-btn"
                            // onClick={() => alert(`Removing job: ${app.job}`)}
                          >
                            <FontAwesomeIcon icon={faTrashAlt} />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );

      case "My Profile":
        return (
          <div className="idontknow">
            <div className="applied-jobs">
              <h2>Settings </h2>
              <div className="settings-container">
                <header className="settings-header">
                  <div
                    className={`settings-tab ${
                      activeTab === "personal" ? "active" : ""
                    }`}
                    onClick={() => setActiveTab("Personal")}
                  >
                    <span className="settings-icon">👤</span> Personal
                  </div>

                  <div
                    className={`settings-tab ${
                      activeTab === "profile" ? "active" : ""
                    }`}
                    onClick={() => setActiveTab("profile")}
                  >
                    <span className="settings-icon">🧑</span> Profile
                  </div>

                  <div
                    className={`settings-tab ${
                      activeTab === "social" ? "active" : ""
                    }`}
                    onClick={() => setActiveTab("social")}
                  >
                    <span className="settings-icon">🌐</span> Social
                  </div>
                  <div
                    className={`settings-tab ${
                      activeTab === "account" ? "active" : ""
                    }`}
                    onClick={() => setActiveTab("account")}
                  >
                    <span className="settings-icon">🔒</span> Account Settings
                  </div>
                </header>

                <main className="settings-content">
                  {renderSettingsContent()}
                </main>
              </div>
            </div>
          </div>
        );

      default:
        return <p>Select an option from the Sidebar</p>;
    }
  };

  return (
    <div>
      <Header />

      <div className="dashboard-container">
        <aside className="sidebar">
          <div className="profile-card">
            <div className="avatar">👤</div>
            <h3>{dashboardData.user?.name || "User Name"}</h3>
            <p>{dashboardData.user?.email || "user@example.com"}</p>
            {/* <button
              className="edit-profile-btn"
              onClick={() => setShowProfileEdit(!showProfileEdit)}
            >
              {showProfileEdit ? "Close Edit" : "Edit Profile"}
            </button> */}
            {showProfileEdit && (
              <div className="edit-profile">
                <p>Edit profile coming soon!</p>
              </div>
            )}
          </div>
          <nav>
            {(dashboardData.menuItems || []).map((item, index) => (
              <div
                key={index}
                className={`menu-item ${
                  activeMenu === item.name ? "active" : ""
                }`}
                onClick={() => setActiveMenu(item.name)}
              >
                {item.icon} {item.name} {item.count}
              </div>
            ))}
          </nav>
        </aside>

        <div className="idontknow2">
          <main className="dashboard-content">{renderContent()}</main>
        </div>
      </div>
    </div>
  );
}
