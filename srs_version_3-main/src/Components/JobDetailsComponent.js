import React, { useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faBriefcase,
  faBriefcaseClock,
  faCalendar,
  faClock,
  faDoorOpen,
  faLocationDot,
  faStar,
  faWallet,
} from "@fortawesome/free-solid-svg-icons";
import {
  faLink,
  faPhone,
  faEnvelope,
  faBookmark,
} from "@fortawesome/free-solid-svg-icons";
import { useLocation, useNavigate } from "react-router-dom";
import "./JobDetails.css";
import Header from "./Header";
import {
  faFacebook,
  faFirefoxBrowser,
  faLinkedin,
  faTwitter,
  faWhatsapp,
} from "@fortawesome/free-brands-svg-icons";

const relatedJobs = [
  {
    id: 1,
    logo: "https://via.placeholder.com/50",
    companyName: "Tech Corp",
    jobName: "React Developer",
    location: "New York",
    exp: "2-4 years",
    ctc: "$80,000 - $100,000",
    employmentType: "Full-Time",
    remainingDays: 10,
  },
  {
    id: 2,
    logo: "https://via.placeholder.com/50",
    companyName: "Innovate Inc.",
    jobName: "Frontend Developer",
    location: "San Francisco",
    exp: "1-3 years",
    ctc: "$70,000 - $90,000",
    employmentType: "Part-Time",
    remainingDays: 5,
  },
  {
    id: 3,
    logo: "https://via.placeholder.com/50",
    companyName: "CodeBase",
    jobName: "JavaScript Engineer",
    location: "Austin",
    exp: "3-5 years",
    ctc: "$85,000 - $110,000",
    employmentType: "Contract",
    remainingDays: 15,
  },
  {
    id: 4,
    logo: "https://via.placeholder.com/50",
    companyName: "CodeBase",
    jobName: "JavaScript Engineer",
    location: "Austin",
    exp: "3-5 years",
    ctc: "$85,000 - $110,000",
    employmentType: "Contract",
    remainingDays: 15,
  },
];

const JobDetails = () => {
  // const jobId = parseInt(id);
  const location = useLocation();
  const navigate = useNavigate();
  const job = location.state?.job;

  const [saved, setSaved] = useState(false);

  const handleSave = () => {
    setSaved((prevSaved) => !prevSaved);
  };

  return (
    <div className="last-job-details-container">
      <Header />

      {/* Back Button */}
      {/* <div className="last-back-btn-container">
        <button onClick={() => navigate(-1)} className="last-back-btn">
          ← Back
        </button>
      </div> */}

      {/* Job Details Section */}

      <div className="last-job-content">
        <div className="lastlastlast">
          <div className="last-job-row">
            {/* Left Section */}
            <div className="last-job-left">
              <img
                src={job.logo}
                alt={`${job.companyName} logo`}
                className="last-company-logo"
              />
              <div className="last-last-job">
                <div className="last-job">
                  {/* <div className="last-job-type"></div> */}
                  <h2 className="last-job-role">{job.jobName}</h2>
                  <p className="last-job-type">{job.employmentType}</p>
                </div>

                <div className="last-job-contact">
                  <div className="last-job-contact-alignment">
                    <div className="last-job-align">
                      <strong>
                        <FontAwesomeIcon icon={faLink} />
                      </strong>
                      <a
                        href={`https://${job.companyName}.com`}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        {job.companyName}.com
                      </a>
                    </div>
                    {/* <br /> */}

                    <div className="last-job-align">
                      <strong>
                        <FontAwesomeIcon icon={faPhone} />
                      </strong>
                      {job.phone || "Not available"}
                    </div>

                    {/* <br /> */}
                    <div className="last-job-align">
                      <strong>
                        <FontAwesomeIcon icon={faEnvelope} />
                      </strong>
                      {job.email || "Not available"}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Right Section */}
            <div className="last-job-right">
              <button className="last-save-job-btn" onClick={handleSave}>
                <FontAwesomeIcon
                  icon={faBookmark}
                  style={{ color: saved ? "#1282A2" : "#c5c1c1" }} // Change icon color based on saved state
                />
                {/* {saved ? "Saved" : "Save"} */}
              </button>

              <button
                className="last-apply-now-btn"
                onClick={() => console.log("Apply Now Clicked")}
              >
                Apply Now
              </button>
              <p className="last-job-expiry">
                Job expires in <strong>{job.remainingDays} days</strong>
              </p>
            </div>
          </div>
          {/* Job Description Section */}
          <div className="last">
            <div className="last-job-description-container">
              <div className="last-job-description">
                <h3>Job Description</h3>
                <p>{job.jobDescription}</p>
              </div>

              {/* Responsibilities Section */}
              <div className="last-job-responsibilities">
                <h3>Responsibilities</h3>
                {/* <p>{job.jobshort}</p> */}
                <ul>
                  {job.jobshort
                    .split(".")
                    .map(
                      (point, idx) =>
                        point.trim() && <li key={idx}>{point.trim()}</li>
                    )}
                </ul>
              </div>
              {/* Share this job section */}
              <div className="job-share-section">
                <p className="share-text">Share this job</p>
                <div className="share-icons">
                  <a
                    href={`https://www.facebook.com/sharer/sharer.php?u=https://your-job-url.com`}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <FontAwesomeIcon
                      icon={faFacebook}
                      className="share-icon facebook"
                    />
                  </a>
                  <a
                    href={`https://twitter.com/intent/tweet?url=https://your-job-url.com&text=Check%20out%20this%20job!`}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <FontAwesomeIcon
                      icon={faTwitter}
                      className="share-icon twitter"
                    />
                  </a>
                  <a
                    href={`https://www.linkedin.com/shareArticle?url=https://your-job-url.com&title=Check%20out%20this%20job`}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <FontAwesomeIcon
                      icon={faLinkedin}
                      className="share-icon linkedin"
                    />
                  </a>
                  <a
                    href={`https://api.whatsapp.com/send?text=Check%20out%20this%20job:%20https://your-job-url.com`}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <FontAwesomeIcon
                      icon={faWhatsapp}
                      className="share-icon whatsapp"
                    />
                  </a>
                  <a
                    href={`mailto:?subject=Check out This job&body=Here is the job link: https://your-job-url.com`}
                  >
                    <FontAwesomeIcon
                      icon={faEnvelope}
                      className="share-icon email"
                    />
                  </a>
                </div>
              </div>
              <div className="related-jobs-section">
                <h3 style={{ marginTop: "10px" }}>Related Jobs</h3>
                <div className="last-card-layout" style={{ marginTop: "20px" }}>
                  {relatedJobs.map((relatedJob) => (
                    <div
                      className="related-card-layout-content"
                      key={relatedJob.id}
                    >
                      <div className="card-logo">
                        <img
                          src={relatedJob.logo}
                          alt={`${relatedJob.companyName} logo`}
                        />
                        <div className="job-card-logo-details">
                          <h4>{relatedJob.companyName}</h4>
                          <h>
                            <i className="fas fa-map-marker-alt"></i>{" "}
                            {relatedJob.location}
                            <i className="fa-solid fa-pen-fancy cardmore"></i>{" "}
                            {relatedJob.exp}
                          </h>
                        </div>
                      </div>
                      <div className="card-details">
                        <h4 className="job-role">{relatedJob.jobName}</h4>
                        <div className="card-last-alignment">
                          <h>{relatedJob.employmentType}</h>
                          <h>
                            <i className="fas fa-money-bill-alt"></i>{" "}
                            {relatedJob.ctc}
                          </h>
                          <h>
                            <i className="fas fa-clock"></i>{" "}
                            {relatedJob.remainingDays} Days Left
                          </h>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Right Container Section */}
            <div className="last-job-overview-container">
              <div className="last-job-overview">
                <h3>Job Overview</h3>
                <div className="last-job-row-align">
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon
                        icon={faCalendar}
                        className="customclr"
                      />
                      <h className="last-jobhead">Job Posted:</h>
                      <strong>{job.jobPostedOn}</strong>
                    </div>
                  </div>
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon icon={faClock} className="customclr" />
                      <h className="last-jobhead">Job expiring:</h>
                      <strong>In {job.remainingDays} Days</strong>
                    </div>
                  </div>
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon icon={faWallet} className="customclr" />
                      <h className="last-jobhead">Salary:</h>
                      <strong>{job.ctc}</strong>
                    </div>
                  </div>
                </div>

                <div className="last-job-row-align">
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon
                        icon={faBriefcase}
                        className="customclr"
                      />
                      <h className="last-jobhead">Experience:</h>
                      <strong>{job.exp}</strong>
                    </div>
                  </div>
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon
                        icon={faLocationDot}
                        className="customclr"
                      />
                      <h className="last-jobhead">Location:</h>
                      <strong>{job.location}</strong>
                    </div>
                  </div>
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon
                        icon={faBriefcase}
                        className="customclr"
                      />
                      <h className="last-jobhead">Type:</h>
                      <strong>{job.employmentType}</strong>
                    </div>
                  </div>
                </div>

                <div className="last-job-row-align">
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon icon={faStar} className="customclr" />
                      <h className="last-jobhead">Rating:</h>
                      <strong>{job.jobRating}</strong>
                    </div>
                  </div>
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon
                        icon={faBriefcaseClock}
                        className="customclr"
                      />
                      <h className="last-jobhead">Applications:</h>
                      <strong>{job.totalApplications}</strong>
                    </div>
                  </div>
                  <div className="last-job-detail-item">
                    <div className="last-jobdetail-align">
                      <FontAwesomeIcon
                        icon={faDoorOpen}
                        className="customclr"
                      />
                      <h className="last-jobhead">Openings:</h>
                      <strong>{job.openings}</strong>
                    </div>
                  </div>
                </div>

                <br></br>
                <div className="last-job-detail-item">
                  <FontAwesomeIcon icon={faBriefcase} className="customclr" />
                  <br></br>
                  <strong>Education:</strong> {job.educationRequired}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default JobDetails;
