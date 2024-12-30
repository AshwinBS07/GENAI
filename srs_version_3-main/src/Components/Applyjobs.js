import React, { useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faSearch,
  faMapMarkerAlt,
  faFolderOpen,
  faFilter,
  faTh,
  faThList,
} from "@fortawesome/free-solid-svg-icons";
import "./Applyjobs.css";
import Header from "./Header";
import AdvancedFilterModal from "./AdvancedFilterModal";
import JobDetails from "./JobDetails";
import { useNavigate } from "react-router-dom";

const ApplyJobs = () => {
  const navigate = useNavigate();
  const [filters, setFilters] = useState([]);
  const [jobTitle, setJobTitle] = useState("");
  const [location, setLocation] = useState("");
  const [category, setCategory] = useState("");
  const [isClmnLayout, setIsClmnLayout] = useState(true);
  // const [isSaved, setIsSaved] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedJob, setSelectedJob] = useState(null); // For the selected job
  const [isJobDetailsOpen, setIsJobDetailsOpen] = useState(false); // JobDetails modal state
  const [perPage, setPerPage]=useState(5);
  const [currentPage,setCurrentPage]=useState(1);

  const toggleToClmnLayout = () => {
    // console.log("c");
    setIsClmnLayout(false);
  };
  const toggleToRowLayout = () => {
    // console.log("r");
    setIsClmnLayout(true);
  };

  // const toggllLayout=(layout)=>{
  //   setIsClmnLayout(layout==="column");
  // }

  const jobsData = [
    {
      id:1,
      jobName: "Software Engineer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      location: "Bangalore, India",
      ctc: "8-12 LPA",
      exp: "2 years",
      industryType: "IT Service & Consulting",
      employmentType: "Full-Time",
      jobDescription:
        "Design and develop scalable web and mobile applications using modern frameworks. Collaborate with cross-functional teams to meet project goals. Maintain high-quality code standards, conduct code reviews, and debug complex issues. Create and implement efficient algorithms and data structures.",
      jobshort:
        "Responsible for developing and maintaining web applications using React and Nodejs. Design and develop scalable web and mobile applications. Collaborate with cross-functional teams to meet project goals.",
      jobRating: 4.5,
      roleCategory: "IT/Software Development",
      educationRequired: "B.E./B.Tech in Computer Science or equivalent",
      jobPostedOn: "2024-12-10",
      daysToApply: 20,
      openings: 5,
      totalApplications: 120,
    },
    {
      id:2,
      jobName: "Data Analyst",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      location: "Hyderabad, India",
      ctc: "8-12 LPA",
      exp: "4-6 years",
      daysToApply: 20,
      industryType: "IT Service & Consulting",
      employmentType: "Full-Time",
      jobDescription:
        "Analyze and interpret complex data to generate actionable insights for business decisions. Work with stakeholders to define data requirements and create visualizations using tools like Power BI and Tableau. Automate data workflows and ensure data integrity.",
      jobshort:
        "Analyze large datasets to identify trends and generate actionable insights. Work with stakeholders to define data requirements. Create visualizations using tools like Power BI and Tableau.",
      jobRating: 4.2,
      roleCategory: "Analytics",
      educationRequired:
        "B.Sc./M.Sc. in Statistics, Mathematics, or related fields",
      jobPostedOn: "2024-12-08",
      openings: 3,
      totalApplications: 85,
    },
    {
      id:3,
      jobName: "Digital Marketing Specialist",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      daysToApply: 20,
      exp: "5-6 years",
      location: "Mumbai, India",
      industryType: "IT Service & Consulting",
      ctc: "6-8 LPA",
      employmentType: "Part-Time",
      jobDescription:
        "Develop and execute digital marketing campaigns across multiple platforms. Optimize SEO strategies and analyze campaign performance metrics. Enhance brand visibility by managing social media accounts and paid advertising.",
      jobshort:
        "Develop and execute digital marketing strategies to enhance brand visibility. Optimize SEO strategies and analyze campaign performance metrics. Enhance brand visibility by managing social media accounts.",
      jobRating: 4.0,
      roleCategory: "Marketing",
      educationRequired: "MBA in Marketing or equivalent",
      jobPostedOn: "2024-12-05",
      openings: 2,
      totalApplications: 60,
    },
    {
      id:4,
      jobName: "HR Manager",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      daysToApply: 20,
      exp: "6 years",
      location: "Delhi, India",
      ctc: "10-12 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Oversee recruitment processes, employee engagement, and performance management. Implement HR policies that align with company goals. Foster a collaborative work environment and address employee concerns effectively.",
      jobshort:
        "Manage the end-to-end recruitment process and employee engagement programs. Oversee recruitment processes and performance management. Foster a collaborative work environment.",
      jobRating: 4.3,
      roleCategory: "Human Resources",
      educationRequired: "MBA in HR or equivalent",
      jobPostedOn: "2024-12-07",
      openings: 1,
      totalApplications: 45,
    },
    {
      id:5,
      jobName: "UI/UX Designer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      daysToApply: 20,
      exp: "2 years",
      location: "Remote",
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },
    {
      id:6,
      jobName: "UI/UX Designer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      daysToApply: 20,
      exp: "2 years",
      location: "Remote",
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },
    {
      id:7,
      jobName: "UI/UX Designer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      daysToApply: 20,
      exp: "2 years",
      location: "Remote",
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },
    {
      id:8,
      jobName: "UI/UX Designer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      exp: "2 years",
      daysToApply: 20,
      location: "Remote",
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },
    {
      id:9,
      jobName: "UI/UX Designer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      daysToApply: 20,
      exp: "2 years",
      location: "Remote",
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },
    {
      id:10,
      jobName: "UI/UX Designer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      exp: "2 years",
      location: "Remote",
      daysToApply: 20,
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },

    {
      id:11,
      jobName: "UI/UX Designer",
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      exp: "2 years",
      daysToApply: 20,
      location: "Remote",
      daysToApply: 20,
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },
    {
      id:12,
      jobName: "UI/UX Designer",
      daysToApply: 20,
      logo: "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      companyName: "Abcd Tech Private Limited",
      industryType: "IT Service & Consulting",
      exp: "2 years",
      location: "Remote",
      ctc: "9-15 LPA",
      employmentType: "Full-Time",
      jobDescription:
        "Design intuitive user interfaces and experiences for web and mobile applications. Create wireframes, prototypes, and mockups to communicate design ideas. Collaborate with developers to ensure seamless implementation of designs.",
      jobshort:
        "Design intuitive and user-friendly interfaces for web and mobile applications. Create wireframes, prototypes, and mockups. Collaborate with developers to ensure seamless implementation of designs.",
      jobRating: 4.6,
      roleCategory: "Design",
      educationRequired: "B.Des. in Graphic Design or equivalent",
      jobPostedOn: "2024-12-09",
      openings: 4,
      totalApplications: 95,
    },
  ];

  const [updated, setUpdated] = useState(jobsData);
  const [jobSaved, setJobSaved]= useState(
    jobsData.reduce((acc,job)=>({...acc, [job.id]: false}), {})
  );

  // const toggleSavesJob = (index) => {
  //   // console.log("hrlk")
  //   const updatedJobs = [...updated];
  //   updatedJobs[index].saved = !updatedJobs[index].saved;
  //   setUpdated(updatedJobs); // Update the state with the new job list
  //   console.log(updatedJobs[index]);
  // };

  const toggleSavesJob=(jobId)=>{
      setJobSaved((prevState)=>{
        const updatedState= { ...prevState, [jobId]: !prevState[jobId]};
        // console.log("updaated jobstate",updatedState);
        return updatedState
      });
  };

  const updatedJobsData = jobsData.map((job, index) => {
    const jobPostedDate = new Date(job.jobPostedOn);
    const applicationDeadline = new Date(
      // jobPostedDate.setDate(jobPostedDate.getDate() + job.daysToApply)
      jobPostedDate.getTime() + job.daysToApply * 24 * 60 * 60 * 1000
    );
    const currentDate = new Date();

    // Calculate remaining days
    const remainingDays = Math.ceil(
      (applicationDeadline - currentDate) / (1000 * 60 * 60 * 24)
    );
    // console.log(`job${index} debugging:`, {
    //   jobPostedDate,
    //   applicationDeadline,
    //   remainingDays,
    // });

    return {
      ...job,
      saved: false,
      remainingDays: remainingDays > 0 ? remainingDays : 0, // Ensure no negative values
    };
  });
  const updatedApply = updatedJobsData;

  const handleFindJob = () => {
    const newFilters = [];
    if (jobTitle) newFilters.push({ id: "jobTitle", text: jobTitle });
    if (location) newFilters.push({ id: "location", text: location });
    if (category) newFilters.push({ id: "category", text: category });
    setFilters(newFilters);
  };

  const handleRemoveFilter = (id) => {
    setFilters(filters.filter((filter) => filter.id !== id));
  };

  const toggleModal = () => setIsModalOpen(!isModalOpen);

  const handleJobClick = (job) => {
    navigate(`/jobdetails/${job.id}`, { state: { 
      job,
      
     },
     });
  };
  const closeJobDetails = () => setIsJobDetailsOpen(false);

  // const startIndex=(currentPage-1) * perPage;
  // const endIndex=startIndex+perPage;
  const updatedA=updatedApply.slice(0,perPage)
  
  return (
    <div className="apply-jobs-page">
      <Header />

      {/* Job Header Section */}
      <div className="job-header-section">
        <div className="job-header-item">
          <FontAwesomeIcon icon={faSearch} className="icon" />
          <input
            type="text"
            placeholder="Job title, keyword"
            className="job-header-input"
            value={jobTitle}
            onChange={(e) => setJobTitle(e.target.value)}
          />
        </div>
        <div className="job-header-item">
          <FontAwesomeIcon icon={faMapMarkerAlt} className="icon" />
          <input
            type="text"
            placeholder="Location"
            className="job-header-input"
            value={location}
            onChange={(e) => setLocation(e.target.value)}
          />
        </div>
        <div className="job-header-item">
          <FontAwesomeIcon icon={faFolderOpen} className="icon" />
          <select
            className="job-header-select"
            value={category}
            onChange={(e) => setCategory(e.target.value)}
          >
            <option value="">Select Category</option>
            <option value="it">IT</option>
            <option value="finance">Finance</option>
            <option value="marketing">Marketing</option>
            <option value="healthcare">Healthcare</option>
          </select>
        </div>
        <div className="job-header-item">
          <FontAwesomeIcon icon={faFilter} className="icon" />
          {/* <select className="job-header-select">
            <option value="">Advanced Filter</option>
            <option value="experience">Experience</option>
            <option value="salary">Salary</option>
            <option value="type">Job Type</option>
            <option value="location">Preferred Location</option>
          </select> */}
          <button className="job-header-select" onClick={toggleModal}>
            Advanced Filter
          </button>
        </div>
        <button className="find-job-btn" onClick={handleFindJob}>
          Find Job
        </button>
      </div>

      <div className="job-list-container">
        <div className="job-header">
          {/* Filters Section */}
          <div className="filters-section">
            {filters.map((filter) => (
              <div className="filter-item" key={filter.id}>
                {filter.text}
                <button
                  className="remove-filter-btn"
                  onClick={() => handleRemoveFilter(filter.id)}
                >
                  ×
                </button>
              </div>
            ))}
          </div>

          {/* Job List Controls */}
          <div className="job-controls">
            <div className="control-item">
              {/* <label>Job Freshness</label> */}
              <select className="control-dropdown">
                <option value="latest">Latest</option>
                <option value="week">Last Week</option>
                <option value="month">Last Month</option>
              </select>
            </div>
            <div className="control-item">
              {/* <label>Jobs Per Page</label> */}
              <select className="control-dropdown" onChange={(e) => setPerPage(Number(e.target.value))} value={perPage}>
                <option value="5">5 Per Page</option>
                <option value="10">10 Per Page</option>
                <option value="15">20 Per Page</option>
                <option value="50">All</option>
              </select>
            </div>
            <div className="layout-icons">
              <FontAwesomeIcon
                icon={faThList}
                className={`layout-icon ${!isClmnLayout ? "active" : ""}`}
                title="Column Layout"
                onClick={toggleToRowLayout}
              />

              <FontAwesomeIcon
                icon={faTh}
                className={`layout-icon ${isClmnLayout ? "active" : ""}`}
                title="Column Layout"
                onClick={toggleToClmnLayout}
              />
            </div>
          </div>
        </div>
        {/* adding here */}
        <div
          className={`job-items-scrollable ${
            isClmnLayout ? "row-layout" : "card-layout"
          }`}
        >
          {updatedA.map((job, index) => (
            <div
              key={index}
              className={`job-item ${isClmnLayout ? "row-item" : "card-item"}`}
            >
              {isClmnLayout ? (
                // Row Layout
                <div className="row-layout-content">
                  <div className="job-left">
                    {/* Logo and Job Details */}
                    <div className="job-logo">
                      <img src={job.logo} alt={`${job.companyName} logo`} />
                    </div>
                    <div className="job-details">
                      <div className="job-alignment">
                        <h3 className="job-role">{job.jobName}</h3>
                        <span className="job-type">{job.employmentType}</span>
                      </div>

                      <div className="job-meta">
                        <div className="meta-item-alignment">
                          <div className="meta-item">
                            <i className="fa-solid fa-pen-fancy"></i> {job.exp}
                          </div>
                          <div className="meta-item">
                            <i className="fa-solid fa-signs-post"></i>{" "}
                            {job.jobPostedOn}
                          </div>
                          <div className="meta-item">
                            <i className="fa-solid fa-briefcase"></i>{" "}
                            {job.openings}
                          </div>
                        </div>

                        <div className="meta-item">
                          <div className="meta-item-alignment">
                            <div className="meta-item">
                              <i className="fas fa-map-marker-alt"></i>{" "}
                              {job.location}
                            </div>
                            <div className="meta-item">
                              <i className="fas fa-money-bill-alt"></i>{" "}
                              {job.ctc}
                            </div>
                            <div className="meta-item">
                              <i className="fas fa-clock"></i>{" "}
                              <span>{job.remainingDays !== undefined ? `${job.remainingDays} Days Left` : "No Deadline"} </span>
                              
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                  {/* Right Section */}
                  <div className="job-right">
                    {/* <button
                      className={`save-job-btn ${job.saved ? "saved" : ""}`}
                      onClick={() => toggleSavesJob(index)}
                    >
                      {console.log("job sved", job.saved)}
                      <i className={`fa ${job.saved ? "fa-heart" : "fa-heart-o"}`}></i>
                    </button> */}
                    <button onClick={()=>toggleSavesJob(job.id)} className={`save-job-btn ${jobSaved[job.id] ? 'saved' : ''}`}>
                     <i className={`fa fa-heart heart-icon ${jobSaved[job.id] ? 'filled' : ''}`}></i>
                    </button>
                    <button
                      className="apply-btn"
                      onClick={() => handleJobClick(job)}
                    >
                      Apply Now
                    </button>
                  </div>
                </div>
              ) : (
                // Card Layout
                <div
                  className="card-layout-content"
                  onClick={() => handleJobClick(job)}
                >
                  {/* <div className="card-logo-details"></div> */}
                  <div className="card-logo">
                    <img src={job.logo} alt={`${job.companyName} logo`} />
                    <div className="job-card-logo-details">
                      <h4>{job.companyName}</h4>
                      <h>
                        <i className="fas fa-map-marker-alt"></i> {job.location}
                        <i className="fa-solid fa-pen-fancy cardmore"></i>{" "}
                        {job.exp}
                      </h>
                    </div>
                  </div>
                  <div className="card-details">
                    <h4 className="job-role">{job.jobName}</h4>
                    <div className="card-last-alignment">
                      <h>{job.employmentType}</h>
                      <h>
                        {" "}
                        <i className="fas fa-money-bill-alt"></i> {job.ctc}
                      </h>
                      <h>
                        <i className="fas fa-clock"></i> {" "}
                        {job.remainingDays} Days Left
                      </h>
                    </div>
                  </div>
                  {/* <button className="card-apply-btn">Apply Now</button> */}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
      <AdvancedFilterModal isOpen={isModalOpen} onClose={toggleModal} />
    </div>
  );
};

export default ApplyJobs;
