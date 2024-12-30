import mysql.connector
import pandas as pd
from mysql.connector import Error
import json
from flask import Flask, request, jsonify, render_template, redirect, url_for, flash, session
from app_modules import insert_data_to_mysql
import app_modules as am
from sqlalchemy import create_engine
from PyPDF2 import PdfReader
import os
import uuid
from datetime import datetime
from flask_cors import CORS

resume_json = None

# Ensure the uploads directory exists
os.makedirs('uploads', exist_ok=True)


app = Flask(__name__)
CORS(app)
app.secret_key = "PlusUltra+AllMight"

@app.route('/')
def home():
    if 'username' in session:
        return f"Hello, {session['username']}! <a href='/logout'>Logout</a>"
    return render_template('login.html')

@app.route('/add_user', methods=['POST'])
def add_user():
    if request.is_json:
        data = request.get_json()
        print(data["email"])
        current_datetime = datetime.now()
        current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        b_json = [{
        "UserMailId": str(data["email"]),
        "UserName": str(data["name"]),
        "login_date": str(current_datetime.strftime("%Y-%m-%d %H:%M:%S")),
        "phone": None,
        "nationality": None,
        "linkedin_url": None,
        "portfolio_url": None,
        "home_location": None,
        "languages": None,
        "UserPassword":str(data["password"])
    }]
        
        
        # json_data = json.loads(data)

        mes = insert_data_to_mysql(b_json)
        am.create_new_row(data["email"])
        print(mes)
        if mes == None:
            return jsonify({"message": "User added successfully!"}), 200
        else:
            return jsonify({"message":mes})
    else:
        return jsonify({"error": "Invalid JSON input"}), 400
    



### recruiter add user
@app.route('/add_user_rec', methods=['POST'])
def add_user_rec():
    if request.is_json:
        data = request.get_json()
        
        current_datetime = datetime.now()
        current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
    
        b_json = [{"UserMailId":data[0]["email"], 
                   "UserPassword": data[0]["password"], 
                   "UserName":data[0]["name"] }]
        
        
        # json_data = json.loads(data)

        mes = am.insert_data_to_mysql_rec(b_json)
        am.create_new_row(data[0]["email"])
        print(mes)
        if mes == None:
            return jsonify({"message": "User added successfully!"}), 200
        else:
            return jsonify({"message":mes})
    else:
        return jsonify({"error": "Invalid JSON input"}), 400



@app.route('/login', methods=['GET', 'POST'])
def login():
    data = request.get_json()
    print(data,"pr")
    
    # data = pd.DataFrame(data)
    if request.method == 'POST':
        username = data['email']
        password = data['password']
        # print(f"{username,password}>>1")
        # Authenticate user
        users = am.getUserInfo()
        users = users[["UserMailId","UserPassword"]]
        # print(users.loc[["UserMailID","UserPassword"]])
        users = users.to_json(orient="records")
        users = json.loads(users)
        # print(type(users))
        
        user_data = next((u for u in users if str(u["UserMailId"]) == str(username)), None)
        #get logged in user info
        # login_data = am.get_login_info()
        # print(login_data)
        # print(user_data["UserMailId"],user_data["UserPassword"])
        if type(user_data) != type(None):
            if str(user_data["UserMailId"]) == username and str(user_data["UserPassword"]) == password:
            # if username in users and users[username] == password:
                # print(users[username])
                session['username'] = username
                # print(session,type(session))
                status = am.update_login(username)
                if status == "OK":
                    flash('Login successful!', 'success')
                    return json.dumps({"message": f"Welcome {username}"}), 200
                # return "Login"
                elif status == "User Already Logged in":

                    return redirect(url_for('home'))

            else:
                flash('Invalid username or password', 'error')
                return 'Invalid password'
                # return redirect(url_for('login'))
        else:
            flash('User Does not Exitst', 'error')
            return 'User Does not Exitst or the user name is wrong'
            # return redirect(url_for('login'))

    # return render_template('login.html')
    return "login Page"

@app.route('/logout')
def logout():
    logout_user_id = session.pop('username', None)
    print(logout_user_id)
    am.delete_login(logout_user_id)
    flash('You have been logged out.', 'info')
    return "You have been logged out."
    return redirect(url_for('home'))

@app.route('/register', methods=['POST'])
def candidate_professional():
    data = request.get_json()
   
    
    #data = pd.DataFrame(data)
    
    try:
        print(session["_flashes"][-1][1])
        if session["_flashes"][-1][1] == "You have been logged out.":
            return f"<a href = '/login'>Login to continue UPDATING </a>"  
        data[0]["UserMailId"] = session['username']
        #data = data.to_dict(orient="records")
        
        #return data
        message = am.insert_professional_information(data)  
        return jsonify(message)
        #return data.to_json(orient="records") , session["username"]

    except Error as e:
        return e
    
@app.route('/create_job', methods=['POST'])
def create_jobs():
    data = request.get_json()
    job_id = "J"+str(int(uuid.uuid4().int % 10**9)) 
    data[0]["job_id"] = job_id
    #data = pd.DataFrame(data)
    data[0]["job_creator_id"] = session["username"]

    current_datetime = datetime.now()
    data[0]["date_posted"] = str(current_datetime.strftime("%Y-%m-%d %H:%M:%S"))

    try:
        print(session["_flashes"][-1][1])
        if session["_flashes"][-1][1] == "You have been logged out.":
            return f"<a href = '/login'>Login to continue UPDATING </a>"  
        
        #data = data.to_dict(orient="records")
        
        #return data
        message = am.insert_data_into_jobs(data)  
        return jsonify(message)
        #return data.to_json(orient="records") , session["username"]

    except Error as e:
        return e


@app.route('/apply_job', methods=['POST'])
def apply_for_job():
    data = request.get_json()
    application_id = "A"+str(int(uuid.uuid4().int % 10**9)) 
    data[0]["application_id"] = application_id
    #data = pd.DataFrame(data)
    current_datetime = datetime.now()
    data[0]["application_date"] = str(current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
    try:
        print(session["_flashes"][-1][1])
        if session["_flashes"][-1][1] == "You have been logged out.":
            return f"<a href = '/login'>Login to continue UPDATING </a>"
        data[0]["user_id"] = session['username']  
        message = am.insert_data_into_applied_jobs(data)  
        return jsonify(message)
        #return data.to_json(orient="records") , session["username"]

    except Error as e:
        return e

@app.route('/edit', methods=['POST'])
def edit_personal_info():
    data = request.get_json()
    table_name = [i for i in data.keys()][0]
    update_data = data[table_name]
    update_data[0]["UserMailId"] = session["username"]
    print(update_data)
    message = am.update_table(table_name,update_data)
    return message
# def edit_professional_info():
#     pass

# def edit_job_application():
#     pass

# def edit_created_job():
#     pass

@app.route('/login_rec', methods=['GET', 'POST'])
def login_rec():
    data = request.get_json()

    data = pd.DataFrame(data)
    if request.method == 'POST':
        username = data['email'][0]
        password = data['password'][0]
        # print(f"{username,password}>>1")
        # Authenticate user
        users = am.getRecInfo()
        users = users[["UserMailId","UserPassword"]]
        # print(users.loc[["UserMailID","UserPassword"]])
        users = users.to_json(orient="records")
        users = json.loads(users)
        # print(type(users))
        
        user_data = next((u for u in users if str(u["UserMailId"]) == str(username)), None)
        #get logged in user info
        # login_data = am.get_login_info()
        # print(login_data)
        # print(user_data["UserMailId"],user_data["UserPassword"])
        if type(user_data) != type(None):
            if str(user_data["UserMailId"]) == username and str(user_data["UserPassword"]) == password:
            # if username in users and users[username] == password:
                # print(users[username])
                session['username'] = username
                # print(session,type(session))
                status = am.update_login(username)
                if status == "OK":
                    flash('Login successful!', 'success')
                    return f"Welcome {username}"
                # return "Login"
                elif status == "User Already Logged in":

                    return redirect(url_for('home'))

            else:
                flash('Invalid username or password', 'error')
                return 'Invalid password'
                # return redirect(url_for('login'))
        else:
            flash('User Does not Exitst', 'error')
            return 'User Does not Exitst or the user name is wrong'
            # return redirect(url_for('login'))

    # return render_template('login.html')
    return "login Page"

# reading pdf
@app.route('/upload_pdf', methods=['POST'])
def upload_pdf():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.pdf'):
        try:
            # Save the PDF file temporarily
            file_path = os.path.join('uploads', file.filename)
            file.save(file_path)

            # Extract text from the PDF
            text = am.extract_text_from_pdf(file_path)


            ##########################
            system_content = """You extract text without adding information. Respond only in JSON.
                For MySQL storage, use supported data types. 
                Use comma-separated text, not lists. 
                If information is missing, return 'None.' For repeated fields, fill once."""

            description = """
            Ashwin  B S Aspiring  Data Scientis t \n    +91-9380245317   |        ashwinbs111@gmail.com   |         LinkedIn      |        GitHub            \n  \nSKILLS   \n  \n \no Programming Languages and Tools:  Python, MySQL, Tableau, Scikit Learn, NumPy, Pandas  \no Statistics and Data Analysis:  Descriptive Statistics, Inferential Statistics, Hypothesis Testing, A/B Testing, RFM \nAnalysis, Data Analysis, Time Series Analysis, Product Analytics  \no Machine Learning and Algorithms:  Linear Regression, Logistic Regression, K -Means Clustering, Decision Trees, \nRandom Forest, XG Boost, SVM, RecSys  \no MLOps and Advanced Techniques:  MLOps, Prompt Engineering  \n \nEXPERIENCE  \nSOFTWARE  ENGINEER  Aug 2022  - Present  \nHexaware  Technologies  \nProject - META  \no Maintained data quality  by developing and optimizing data pipelines, ensuring accuracy in bug tracking of various teams in \na project  related to  Meta  at Hexaware.  \no Developed a Python module  to analyse  the quality of bug s report ed, automating report generation for real -time insights \ninto QA performance, which were leveraged by teams.  \no Built and maintained Power BI dashboards , tracking key performance metrics to inform data -driven decision -making, \nsupporting QA teams.  \no Streamlined testing methodologies through data analysis , improving the speed and accuracy of quality assurance \nprocesses, resulting in optimized product delivery.  \no Awarded for driving data insights  within the smallest but best-performing team at Hexaware, recognized for contributions \nto analytics -driven quality improvements.  \n \nPROJECTS  \nExploratory Data Analysis (EDA)  & Feature Engineering              \no Conducted comprehensive EDA and engineered features to enhance data quality, handling missing values and outliers. \nProvided actionable insights for improving data models.  \no Skills Used: Python (Pandas, Matplotlib, Seaborn, Plotly), Feature Creation, Outlier Treatment  \nLinear  & Logistic Regression   \no Built and evaluated regression models for graduate admissions and loan data, predicting acceptance probabilities and loan \nstatuses. Delivered insights for process improvements and risk management.  \no Skills Used: Python (Pandas, Matplotlib, Seaborn), Linear Regression, Logistic Regression, Feature Engineering  \nEnsemble Learning  & Clustering   \no Developed predictive models using ensemble learning techniques for driver retention and applied clustering techniques to \nemployee data for business decisions.  \no Skills Used: Ensemble Learning (Bagging, Boosting), KNN Imputation, K -means, Hierarchical Clustering  \nTime Series Analysis  \no Forecasted Wikipedia page views using ARIMA, SARIMAX, and Prophet models to optimize ad placements across regions.  \no Skills Used: ARIMA, SARIMAX, Prophet  \n \n \n \nDescriptive Statistics, Probability,  & Hypothesis Testing  \no Analyzed target audience data for fitness products and demand factors for electric cycle rentals using descriptive statistics  \nand hypothesis testing.  \no Skills Used: Python (Pandas, Matplotlib, Seaborn), ANOVA, Chi -square, 2 -sample t -test \nConfidence Intervals & Central Limit Theorem   \no Conducted a detailed analysis of customer spending behaviors using confidence intervals to compare demographics and \nsegments.  \no Skills Used: Python (Pandas, Matplotlib, Seaborn, NumPy), Statistical Analysis  \nData Analysis Using SQL   \no Conducted in -depth data analysis using SQL for business intelligence purposes.  \n \nACHIEVEMENTS  \no Designathon Winner  \no Top Scorer at Mavericks Learning Program  \no Rockstar Award  \nCERTIFICATION  \no Oracle Cloud Infrastructure 2024 Certified AI Foundations Associate  \no Microsoft Certified: Azure Fundamentals  \no Gen AI - Level 1 Foundation Course  \n \nEDUCATION   \nB.E. - ADICHUNCHANAGIRI INSTITUTE OF TECHNOLOGY
            """
            output_format = """UserMailId, years_of_exp, skills, year_of_graduation, highest_degree, most_recent_job_title, company_name, certifications, achievements, projects, agg_job_desc, ctc, expected_ctc
            UserMailId, UserName, login_date, phone, nationality, linkedin_url, portfolio_url, home_location, languages, UserPassword"""

            user_content = f"Description: {text}, Output format(JSON only): {output_format}"

            prompt=[
                {
                    "role":"system",
                "content":system_content
                },

                {
                    "role":"user",
                "content": user_content
                    }
                ]




            ########################

            # Remove the temporary file
            os.remove(file_path)
            
            resume_df =  am.get_resumes(session["username"])
            #print(resume_df["resume"].values[0],type(resume_df["resume"].values[0]))
            print("--",resume_df,"noprofile")
            ai_text = am.ai_model(prompt)
            
            previous_data = resume_df["resume"].values

            if previous_data == None:
                name = "resume1"
                resume_df.drop(resume_df[resume_df["resume"].values == None].index[0],inplace = True)
                resume_df.loc[1] = json.dumps({name:ai_text})
                #return this to database
                json_data = json.loads(json.loads(resume_df.to_json(orient="records"))[0]["resume"])
                
                return am.store_resume(json.dumps(json_data),session["username"])

            else:
                
                json_data_new = json.loads(previous_data[0])
                length = len(json_data_new)+1
                if length <=3:
                    json_data_new[f"resume{length}"] = ai_text
                    
                    #print(json.dumps(json_data_new))
                    
                    resume_df.loc[0] = json.dumps(json_data_new)
                    #print(resume_df["resume"].values) 
                    #print(resume_df["resume"].values,"i")
                    text_json  = resume_df["resume"].values[0]
                    #print(text_json)
                    json_data_final = json.loads(text_json)
                    #print(json.dumps(json_data_final))
                    return am.store_resume(json.dumps(json_data_final),session["username"]),length
                else:
                    return "max resume limit is 3"
                


            # #print(len(resume_df))
            # if len(resume_df) <=3:
                
            #     resume_df.loc[len(resume_df)] = jsonify(text)
            #     #print(resume_df.loc[len(resume_df)])
            #     index_drop = resume_df[resume_df["resume"].values==None].index[0]
            #     if index_drop != None:
            #         resume_df.drop(index=index_drop,inplace=True)
            #     #print(resume_df)
            #     json_resume =  resume_df[resume_df.loc[len(resume_df)]].to_json(orient="index")
            #     return am.store_resume(json_resume,session["username"])
            # return "resume limit is 3"




            #return jsonify({f'resume': text}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file format. Only PDF allowed.'}), 400


@app.route('/update_profile', methods=['GET','POST'])
def update_profile():
    resume_df = am.get_resumes(session["username"])
    print(resume_df["resume"].values[0])
    json_resumes =  json.loads(resume_df["resume"].values[0])
    return json_resumes[f"resume{len(json_resumes)}"]

@app.route('/recent_applies', methods=['GET','POST'])
def recent_applies():

    
    df =  am.get_recent_jobs(session["username"])
    df["application_date"] = pd.to_datetime(df["application_date"], format="%a, %d %b %Y %H:%M:%S %Z").dt.strftime("%Y-%m-%d")

    print(df["job_status"].values.tolist())
    di = {
        "id": df["application_id"].values.tolist(), 
        "job": df["job_title"].values.tolist(), 
        "companyLogo": "https://via.placeholder.com/50",
        "shift": "Remote",
        "location": df["job_location"].values.tolist(),
        "salary": df["salary"].values.tolist(), 
        "status": df["job_status"].values.tolist(),
        "date": df["application_date"].values.tolist(), 
        "action": "View Details",
      }
    data = pd.DataFrame(di)

# Convert to the required JSON format
    recentApplications = data.to_dict(orient="records")

    # Result
    print(recentApplications)
    return recentApplications
    """{
        id: 1, application_id
        job: "Software Engineer", job_title
        companyLogo: "https://via.placeholder.com/50",
        shift: "Remote",
        location: "New York, USA", job_location
        salary: "$120,000/year", salary
        status: "Active", job_status
        date: "2024-12-10", application_date
        action: "View Details",
      }"""
    return "ok"


@app.route('/all_jobs', methods=['GET','POST'])
def all_jobs():
    df =  am.get_job_openings("job_openings")
    application_df = am.get_job_openings('job_applications')
    df["days_to_exp"] = (df["application_deadline"]-df["date_posted"]).astype(str)
    df["date_posted"] = pd.to_datetime(df["date_posted"], format="%a, %d %b %Y %H:%M:%S %Z").dt.strftime("%Y-%m-%d")
    
    count_df = pd.DataFrame(application_df.groupby("job_id")["job_id"].value_counts()).reset_index()
    merged_df = df.merge(count_df,left_on="job_id",right_on="job_id",how="left")
    
    dictionary = {
      "id":df["job_id"].values.tolist(),
      "jobName": df["job_title"].values.tolist(),
      "logo": "https://via.placeholder.com/100/1282A2/FFFFFF?text=TechCorp",
      "companyName": "Hexaware Technologies",
      "location":df["job_location"].values.tolist(),
      "ctc": df["salary"].values.tolist(),
      "exp": df["yoe_required"].values.tolist(),
      "industryType": df["job_category"].values.tolist(),
      "employmentType": df["employment_type"].values.tolist(),
      "jobDescription":df["job_description"].values.tolist(),
      "jobshort":df["job_description"].values.tolist(),
      "jobRating": 4.5,
      "roleCategory": (df["job_category"] +"/"+ df["job_title"]).values.tolist(),
      "educationRequired": df["educationalRequirement"].values.tolist(),
      "jobPostedOn": df["date_posted"].values.tolist(),
      "daysToApply": df["days_to_exp"].values.tolist(),
      "openings": df["number_of_openings"].values.tolist(),
      "totalApplications": merged_df["count"].values.tolist(),
    }
    print(dictionary)
    
    data = pd.DataFrame(dictionary)
    print(data)
# Convert to the required JSON format
    allApplications = data.to_dict(orient="records")

    # Result
    print(allApplications)
    return allApplications

    """{
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
    }"""



if __name__ == "__main__":
    app.run(debug=True)