import mysql.connector
import pandas as pd
from mysql.connector import Error
import json
from flask import Flask, request, jsonify
import uuid
import re
import traceback
from sqlalchemy import create_engine
import numpy as np
from PyPDF2 import PdfReader
from openai import AzureOpenAI
from datetime import datetime
import uuid
from flask_cors import CORS


from dotenv import load_dotenv

import os


def insert_data_to_mysql(data):
    print(data,"data")
    data_df = pd.DataFrame(data)
    print(data_df,"data_df")
    # data_vals = data_df.values.tolist()
    # print(data_vals)
    # uu = uuid.uuid4()
    # data_df.loc[0,"user_id"] =  str(uu)
    #print(data_df)
    data = data_df.loc[0].values.tolist()
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Write your SQL insert query
            
            # df = pd.read_sql(query,connection)
            # Execute the insert query
            # print(df.columns.to_list)
            # cursor = connection.cursor()

            # Write your SQL insert query
            # for i in range(len())
            sql_insert_query = f"""
            INSERT INTO candidate_personal (UserMailID, UserName, login_date, phone, nationality, linkedin_url, portfolio_url, home_location, languages,UserPassword)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
            # Execute the insert query
            cursor.execute(sql_insert_query, data)

            # Commit the transaction
            connection.commit()
            print("Data inserted successfully")

    except Error as e:
         e = str(e)
         err = e.split(":")
         print(err[0])
         if str(err[0]) == "1062 (23000)":
             print("user")
             return f"User Already Exist! <a href='/login'>login</a>"
         return e
        #  err = traceback.format_exc()
        #  print(err)
        #  err = str(e._full_msg)
        #  errcode = re.findall("/\d{4}\ \(\d{5}\)/gm")
        #  print(errcode)

    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")



#### recruiter creation
def insert_data_to_mysql_rec(data):
    print(data,"data")
    data_df = pd.DataFrame(data)
    print(data_df,"data_df")
    # data_vals = data_df.values.tolist()
    # print(data_vals)
    # uu = uuid.uuid4()
    # data_df.loc[0,"user_id"] =  str(uu)
    #print(data_df)
    data = data_df.loc[0].values.tolist()
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )
        
        if connection.is_connected():
            cursor = connection.cursor()

            # Write your SQL insert query
            
            # df = pd.read_sql(query,connection)
            # Execute the insert query
            # print(df.columns.to_list)
            # cursor = connection.cursor()

            # Write your SQL insert query
            # for i in range(len())
            sql_insert_query = f"""
            INSERT INTO recruitor (UserMailId, UserPassword, UserName)
    VALUES (%s, %s, %s)
            """
            # Execute the insert query
            cursor.execute(sql_insert_query, data)

            # Commit the transaction
            connection.commit()
            print("Data inserted successfully to recruiter")

    except Error as e:
         e = str(e)
         err = e.split(":")
         print(err[0])
         if str(err[0]) == "1062 (23000)":
             print("user")
             return f"User Already Exist! <a href='/login'>login</a>"
         return e
        #  err = traceback.format_exc()
        #  print(err)
        #  err = str(e._full_msg)
        #  errcode = re.findall("/\d{4}\ \(\d{5}\)/gm")
        #  print(errcode)

    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")



def getUserInfo():
    db_user = "root"
    db_password = "Password098"
    db_host = "localhost"  # or the IP address of your database
    db_name = "GENAIDB"

# Create an SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
    query = "SELECT * FROM candidate_personal"

# Use Pandas to execute the query
    df = pd.read_sql(query, engine)
    # print(df)
    return df
    # try:
    #     connection = mysql.connector.connect(
    #     host="localhost",
    #     user="root",
    #     password="Password098",
    #     database ="GENAIDB"    
    #     )
    #     if connection.is_connected():
            
    #         query = "select * from candidate_personal;"

    #         df = pd.read_sql(query,connection)
    #     # print(df)
    #     return(df)
    # except Error as e:
    #     print(e)
        
        
    # finally:
    #     if connection.is_connected():

    #         connection.close()

    #         print("connection closed")
def update_login(user_id):
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
    
    
            # sql_query = """INSERT INTO login(user_id)
            #     values (%s)"""
            sql_query = """
        INSERT INTO login(user_id)
            values (%s);"""
            cursor.execute(sql_query, [user_id])

            connection.commit()
            print("Data inserted successfully to login table")
            message_time = update_login_time(user_id)
            print(message_time)
            return "OK"
            
            
    except Error as e:
        e = str(e)
        err = e.split(":")
        print(err, "login data")
        #  return e
        if str(err[0]) == "1062 (23000)":
            print("user")
            return "User Already Logged in"
        print(e, "Login")
        return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def update_login_time(user_id):
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
    
    
            # sql_query = """INSERT INTO login(user_id)
            #     values (%s)"""
            current_datetime = datetime.now()
            sql_query = f"""
        UPDATE candidate_personal
        SET login_date = '{current_datetime.strftime("%Y-%m-%d %H:%M:%S")}'
        WHERE UserMailId = '{user_id}'"""
            print(sql_query)
            cursor.execute(sql_query)

            connection.commit()
            print("Login Date updated successfully to candidate personal table")
            return "Login Date updated"
    except Error as e:
        e = str(e)
        err = e.split(":")
        print(err, "login time")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def delete_login(user_id):
    try:
        
        # Connect to the MySQL database
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
    
    
            # sql_query = """INSERT INTO login(user_id)
            #     values (%s)"""
            sql_query = """
        DELETE FROM login
WHERE user_id = %s
LIMIT 1;"""
            cursor.execute(sql_query, [user_id])

            connection.commit()
            print("Data deleted successfully from login table")
            
            
    except Error as e:
         e = str(e)
         print(e, "delete")
         return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def get_login_info():
    try:
        db_user = "root"
        db_password = "Password098"
        db_host = "localhost"  # or the IP address of your database
        db_name = "GENAIDB"

# Create an SQLAlchemy engine
        engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
        query = """
        SELECT * FROM login;"""
        

# Use Pandas to execute the query
        df = pd.read_sql(query, engine)
    # print(df)
        return df
        # Connect to the MySQL database
            
            
    except Error as e:
        e = str(e)
        err = e.split(":")
        print(err, "login data")
        #  return e
        if str(err[0]) == "1062 (23000)":
            print("user")
            return "User Already Logged in"
        

def insert_professional_information(data):
    #print(data,"apmod")
    data_df = pd.DataFrame(data)
    #data_df["years_of_exp"] = data_df["years_of_exp"].astype(int)
    #data_df["year_of_graduation"] = data_df["year_of_graduation"].astype(int)
    #data_df["ctc"] = data_df["ctc"].astype(int)

    #data_df["expected_ctc"] = data_df["expected_ctc"].astype(int)

    data_list = list(map(lambda x: int(x) if isinstance(x, (np.int64, np.int32)) else x, data_df.loc[0].values.tolist()))
    
    #data_list = data_df.loc[0].values.tolist()
    
    try:
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )

        if connection.is_connected():
            cursor = connection.cursor()

            sql_insert_query = """
            INSERT INTO candidate_professional (UserMailId, years_of_exp, skills, year_of_graduation, highest_degree, most_recent_job_title, company_name, certifications, achievements, projects, agg_job_desc, ctc, expected_ctc)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s)
            """
            # Execute the insert query
            cursor.execute(sql_insert_query, data_list)

            # Commit the transaction
            connection.commit()
            print("Professional Data inserted successfully")
            return "Professional Data inserted successfully"

    except Error as e:
         e = str(e)
         err = e.split(":")
         
         if str(err[0]) == "1062 (23000)":
             
             return f"User Already Exist! href of apply jobs or any other field"
         return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def insert_data_into_jobs(data):
    #print(data,"apmod")
    data_df = pd.DataFrame(data)
    print(data_df)
    #data_df["years_of_exp"] = data_df["years_of_exp"].astype(int)
    #data_df["year_of_graduation"] = data_df["year_of_graduation"].astype(int)
    #data_df["ctc"] = data_df["ctc"].astype(int)

    #data_df["expected_ctc"] = data_df["expected_ctc"].astype(int)

    data_list = list(map(lambda x: int(x) if isinstance(x, (np.int64, np.int32)) else x, data_df.loc[0].values.tolist()))
    
    #data_list = data_df.loc[0].values.tolist()
    
    try:
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )

        if connection.is_connected():
            cursor = connection.cursor()

            sql_insert_query = """
            INSERT INTO job_openings (job_id, job_title, department, job_skills, yoe_required, job_description, job_location, salary, number_of_openings, employment_type, date_posted, application_deadline, job_status, job_category, job_creator_id,educationalRequirement)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s,%s,%s)
            """
            # Execute the insert query
            cursor.execute(sql_insert_query, data_list)

            # Commit the transaction
            connection.commit()
            print("Job Data inserted successfully")
            return "Job Data inserted successfully"

    except Error as e:
         e = str(e)
         err = e.split(":")
         
         if str(err[0]) == "1062 (23000)":
             
             return f"Job already exist"
         return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def get_applied_job_info():
    db_user = "root"
    db_password = "Password098"
    db_host = "localhost"  # or the IP address of your database
    db_name = "GENAIDB"

# Create an SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
    query = """
    select user_id,job_id from job_applications;"""
    

# Use Pandas to execute the query
    df = pd.read_sql(query, engine)
    # print(df)
    return df
    


def insert_data_into_applied_jobs(data):
    data_df = pd.DataFrame(data)
    print(data_df["user_id"].values)

    data_list = list(map(lambda x: int(x) if isinstance(x, (np.int64, np.int32)) else x, data_df.loc[0].values.tolist()))
    data_list = list(map(lambda x: bool(x) if isinstance(x, (np.bool)) else x, data_df.loc[0].values.tolist()))

    # check if the user has applied for the job 
    job_df = get_applied_job_info()
    print(job_df[job_df["user_id"] == data_df["user_id"].values[0]]["job_id"].values)
    if data_df["job_id"].values[0] in job_df[job_df["user_id"] == data_df["user_id"].values[0]]["job_id"].values:
        return "You have already applies to this job, Cannot apply anymore!"
    try:
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )

        if connection.is_connected():
            cursor = connection.cursor()


            sql_insert_query = """
            INSERT INTO job_applications (application_id, user_id, job_id, is_referral, resume, application_status, application_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            # Execute the insert query
            cursor.execute(sql_insert_query, data_list)

            # Commit the transaction
            connection.commit()
            print("Job application successful")
            return "Job application successful"

    except Error as e:
         e = str(e)
         err = e.split(":")
         
         if str(err[0]) == "1062 (23000)":
             
             return f"application exist"
         return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


# edit function
def update_table(table_name,update_data):
    #data_df = pd.DataFrame(update_data)
    #data_list = list(map(lambda x: int(x) if isinstance(x, (np.int64, np.int32)) else x, data_df.loc[0].values.tolist()))
    #data_list = list(map(lambda x: bool(x) if isinstance(x, (np.bool)) else x, data_df.loc[0].values.tolist()))

    print(update_data[0]["column"])
    try:
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )

        if connection.is_connected():
            cursor = connection.cursor()
            sql_update_query = f"""UPDATE {table_name}
                        SET {update_data[0]["column"]} = '{update_data[0]["value"]}'
                        WHERE UserMailId = '{update_data[0]["UserMailId"]}';"""

            #print(sql_update_query)
            # Execute the insert query
            cursor.execute(sql_update_query)

            # Commit the transaction
            connection.commit()
            print(f"Update {table_name} successful")
            return f"Update {table_name} successful"

    except Error as e:
         e = str(e)
         err = e.split(":")
         
         if str(err[0]) == "1062 (23000)":
             
             return f"application exist"
         return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def getRecInfo():
    db_user = "root"
    db_password = "Password098"
    db_host = "localhost"  # or the IP address of your database
    db_name = "GENAIDB"

# Create an SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
    query = "SELECT * FROM recruitor;"

# Use Pandas to execute the query
    df = pd.read_sql(query, engine)
    # print(df)
    return df

#read pdf to text
def extract_text_from_pdf(pdf_path):
    text = ''
    reader = PdfReader(pdf_path)
    for page in reader.pages:
        text += page.extract_text()
    return text


def get_resumes(username):
    db_user = "root"
    db_password = "Password098"
    db_host = "localhost"  # or the IP address of your database
    db_name = "GENAIDB"

# Create an SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
    query = f"SELECT resume FROM candidate_professional where UserMailId = '{username}';"

# Use Pandas to execute the query
    df = pd.read_sql(query, engine)
    # print(df)
    return df


def store_resume(data_list,username):
    print(data_list,"dl")
    try:
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )

        if connection.is_connected():
            cursor = connection.cursor()

            sql_update_query = """
    UPDATE candidate_professional
    SET resume = %s
    WHERE UserMailId = %s
"""
            
            # Execute the insert query
            cursor.execute(sql_update_query, (data_list,username))

            # Commit the transaction
            connection.commit()
            print("resume upload successful")
            return "resume upload successful"

    except Error as e:
         e = str(e)
         err = e.split(":")
         
         if str(err[0]) == "1062 (23000)":
             
             return f"resume already exist"
         return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def ai_model(prompt):
    # load_dotenv()


    # OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION") #For app user: you need to pass the version configured by the admin

    # AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT") #Eg: {BASE_URL}/api/azureai

    # AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY") #For App User: use the app-registration key along with the app configuration unique key name eg. app123key-configName, For Api User: Substitute the key generated from Key Config Panel


    # client = AzureOpenAI()


    # res = client.chat.completions.create(
    #     model="gpt-4o", #Allowed values for ApiUser: gpt-4o,gpt-4o-mini
    #     messages=prompt,
    #     temperature= 0.7,
    #     max_tokens= 1500,
    #     top_p= 0.6,
    #     frequency_penalty= 0.7)
    # model_txt = res.choices[0].message.content
    model_txt = "```json\n{\n  \"UserMailId\": \"ashwinbs111@gmail.com\",\n  \"years_of_exp\": \"1\",\n  \"skills\": \"Python, MySQL, Tableau, Scikit Learn, NumPy, Pandas, Descriptive Statistics, Inferential Statistics, Hypothesis Testing, A/B Testing, RFM Analysis, Data Analysis, Time Series Analysis, Product Analytics, Linear Regression, Logistic Regression, K-Means Clustering, Decision Trees, Random Forest, XG Boost, SVM, RecSys, MLOps, Prompt Engineering\",\n  \"year_of_graduation\": \"None\",\n  \"highest_degree\": \"B.E.\",\n  \"most_recent_job_title\": \"SOFTWARE ENGINEER\",\n  \"company_name\": \"Hexaware Technologies\",\n  \"certifications\": \"Oracle Cloud Infrastructure 2024 Certified AI Foundations Associate, Microsoft Certified: Azure Fundamentals, Gen AI - Level 1 Foundation Course\",\n  \"achievements\": \"Designathon Winner, Top Scorer at Mavericks Learning Program, Rockstar Award\",\n  \"projects\": \"Exploratory Data Analysis (EDA) & Feature Engineering , Linear & Logistic Regression , Ensemble Learning & Clustering , Time Series Analysis , Descriptive Statistics , Probability & Hypothesis Testing , Confidence Intervals & Central Limit Theorem , Data Analysis Using SQL\",\n  \"agg_job_desc\": null,\n  \"ctc\": null,\n  \"expected_ctc\": null,\n  \"UserName\": null,\n  \"login_date\": null,\n  \"phone\": \"+91-9380245317\",\n  \"nationality\": null,\n  \"linkedin_url\": null,\n  \"portfolio_url\": null,\n  \"home_location\": null,\n  \"languages\": null,\n  \"UserPassword\": null\n}\n```"
    cleaned_content = model_txt.strip("```json\n").strip("```")
    return cleaned_content


def get_recent_jobs(username):
    db_user = "root"
    db_password = "Password098"
    db_host = "localhost"  # or the IP address of your database
    db_name = "GENAIDB"

# Create an SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
    # query = f"SELECT * FROM job_applications where user_id = '{username}';"
    query = f"""select * from job_applications ja
        left join job_openings jo 
        on ja.job_id = jo.job_id
        where ja.user_id = '{username}';"""
# Use Pandas to execute the query
    df = pd.read_sql(query, engine)
    # print(df)
    return df


def get_job_openings(column):
    db_user = "root"
    db_password = "Password098"
    db_host = "localhost"  # or the IP address of your database
    db_name = "GENAIDB"

# Create an SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
    query = f"SELECT * FROM {column};"
    
# Use Pandas to execute the query
    df = pd.read_sql(query, engine)
    # print(df)
    return df

def create_new_row(data_list):
    try:
        connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password098",
        database ="genaidb"    
        )

        if connection.is_connected():
            cursor = connection.cursor()


            sql_insert_query = """
            INSERT INTO candidate_professional (UserMailId)
    VALUES (%s)
            """
            # Execute the insert query
            cursor.execute(sql_insert_query, [data_list])

            # Commit the transaction
            connection.commit()
            print("new row created in candidate professional")
            return "new row created in candidate professional"

    except Error as e:
         e = str(e)
         err = e.split(":")
         
         if str(err[0]) == "1062 (23000)":
             
             return f"candidate proff already exist"
         return e
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
