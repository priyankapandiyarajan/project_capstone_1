# YOUTUBE DATA HARVESTING and WAREHOUSING using SQL and STREAMLIT
## Introduction 
* YouTube, the online video-sharing platform, has revolutionized the way we consume and interact with media. Launched in 2005, it has grown into a global phenomenon, serving as a hub for entertainment, education, and community engagement. With its vast user base and diverse content library, YouTube has become a powerful tool for individuals, creators, and businesses to share their stories, express themselves, and connect with audiences worldwide.

* This project extracts the particular youtube channel data by using the youtube channel id, processes the data, and stores it in the SQL database. Then analyse the data and give the results depending on the customer questions.
  
NAME : Priyanka.P

BATCH: MDTE07

DOMAIN : DATA SCIENCE

LINKED IN URL : 

![Intro GUI](https://github.com/priyankapandiyarajan/project_capstone_1/blob/main/Youtube%20Data.png)

## Developer Guide 

### 1. Tools Install

* Visual Studio code.
* Jupyter notebook.
* Python 3.12.3 or higher.
* MySQL.
* Youtube API key.

### 2. Requirement Libraries to Install

* pip install google-api-python-client, mysql-connector-python, pandas, plotly-express, streamlit.

### 3. Import Libraries

**Youtube API libraries**
* import googleapiclient.discovery
* from googleapiclient.discovery import build
 
**SQL libraries**
* import mysql.connector

**pandas**
* import pandas as pd
* import datetime
* import isodate

**Dashboard libraries**
* import streamlit as st
* import plotly.express as px

## The application should have the following features:

$ Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.

$ Option to store the data in a SQL database as a database. Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button. Option to select a channel name and migrate its data to a SQL database as tables.

$ Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

$ YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.

$ Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.

$ Query the SQL data warehouse: You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.

$ Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.

## User Guide

#### Step 1. Data collection zone

* Search **channel_id**, copy and **paste on the input box** and click the **Extract Data** button in the **EXTRACT Tab**.

#### Step 2. Data Migrate zone

* Select the **channel name** and click the **Upload Data to SQL** button to migrate the specific channel data to the MySQL database in the **TRANSFORM Tab**.

#### Step 3. Analysis zone

* **Show Channels Table** from the checkbox option you can get the result of **Available Channels for Analysing**.

#### Step 4. Query zone

* **Select your question** from the dropdown option you can get the **results in Dataframe format or bar chat format**.

## Configuration:

1.Open the streamlit.py file in the project directory.

2.Set the desired configuration options.

3.Specify your YouTube API key.

4.Choose the database connection details (SQL).

5.Get the Youtube Channel ID from the Youtube's sourcepage

6.Provide the Youtube Channel ID data to be harvested.

7.Set other configuration options as needed.

## Usage:

1.Launch the Streamlit app: streamlit run streamlit.py

2.Run the streamlit.py script, make sure you have main and sql files in the same folder.

3.The app will start and open in your browser. You can explore the harvested YouTube data and visualize the results.
