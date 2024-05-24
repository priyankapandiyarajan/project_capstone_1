##### imports #####
import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
import mysql.connector
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import isodate
import pandas as pd
import plotly.express as px

##### Assigning API variables #####
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyDRKlBkcYTQxFbtPmjseh9Kf9Y4SFBCE0g"
youtube = build(api_service_name, api_version, developerKey=api_key)

##### Establish connection to database #####
mydb = mysql.connector.connect(
    host = "localhost",
    user="root",
    password="root",
    database="youtube_db"
)

# create cursor object
cursor = mydb.cursor()

##### Streamlit app tab title setup #####
icon=Image.open("YOUTUBE_LOGO.JPEG")
st.set_page_config(page_title = "YOUTUBE DATA HARVESTING AND WAREHOUSING",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded")

##### Streamlit sidebar menus #####
with st.sidebar:
    selected = option_menu(None,["Home","Extract & Transform","Analysis Zone","Query Zone"],
                         icons=["house-door-fill","tools","card-text","question"],
                        )
    
##### HOME PAGE #####
if selected == "Home":
    st.markdown('<h1 style="color:red">YOUTUBE DATA HARVESTING and WAREHOUSING using SQL and STREAMLIT</h1>', unsafe_allow_html=True)

    st.markdown("<span style='color:purple; font-size:30px; font-weight: bold; font-style:italic'>Domain :</span> <span style='font-size:20px; font-weight: bold;'>Social Media</span>", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-size:30px; font-weight: bold; font-style:italic'>Skills take away :</span> <span style='font-size:20px; font-weight: bold;'>Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL</span>", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-size:30px; font-weight: bold; font-style:italic'>Overall view :</span> <span style='font-size:20px; font-weight: bold;'> Building a simple UI with Streamlit,retrieving data from Youtube API, storing the date SQL as a WH, querying the data warehouse with SQL, and displaying the data in the streamlit app  </span>", unsafe_allow_html=True)
    st.markdown("<span style='color:purple; font-size:30px; font-weight: bold; font-style:italic'>Developed by :</span> <span style='font-size:20px; font-weight: bold;'> Priyanka Pandiyarajan</span>", unsafe_allow_html=True)


##### Function to fetch Channel details #####
def channel_details(channel_ids):
    channel_datas=[]  #emptylist

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,status",
        id=','.join(channel_ids)
    )
    response = request.execute()

    #data collection
    for data in response['items']:    
        channel_data = dict(
            Channel_name = data['snippet']['title'],
            Channel_id = data['id'],
            Subscription_count = data['statistics'].get('subscriberCount'),
            Channel_views = data['statistics'].get('viewCount'),
            Channel_description = data['snippet']['description'],
            Playlist_id = data['contentDetails']['relatedPlaylists']['uploads'],
            Channel_type = data['kind'],
            Privacy_status= data['status']['privacyStatus']
        )
        channel_datas.append(channel_data)

    return channel_datas

##### Function to fetch all playlist IDs #####
def get_playlists(channel_data):
    playlists = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
            part="snippet",
            channelId=channel_data['Channel_id'],
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        #data collection
        for item in response['items']:
            playlists.append({
                "playlist_id": item['id'],
                "playlist_name": item['snippet']['title']
            })

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return playlists

##### Function to fetch video details #####
def video_details(playlists):
    video_lists = []

    for playlist in playlists:
        next_page_token = None

        while True:
            playlist_request = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist['playlist_id'],
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response = playlist_request.execute()

            for item in playlist_response['items']:
                video_id = item['snippet']['resourceId']['videoId']
                video_request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id
                )
                video_response = video_request.execute()

                for data in video_response['items']:

                    # Convert the ISO 8601 date format to MySQL DATETIME format
                    published_date = data['snippet']['publishedAt']
                    formatted_date = datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Convert ISO 8601 duration to total seconds
                    duration = isodate.parse_duration(data['contentDetails']['duration'])
                    total_seconds = int(duration.total_seconds())
                    
                    #data collection
                    video_details = dict(
                        Video_id=data['id'],
                        Video_name=data['snippet']['title'],
                        Video_description=data['snippet']['description'],
                        Tags=data['snippet'].get('tags'),
                        Published_date=formatted_date,
                        View_count=data['statistics']['viewCount'],
                        Like_count=data['statistics'].get('likeCount'),
                        Dislike_count=data['statistics'].get('dislikeCount'),
                        Favorite_count=data['statistics']['favoriteCount'],
                        Comment_count=data['statistics'].get('commentCount'),
                        Duration=total_seconds,
                        Thumbnail=data['snippet']['thumbnails']['default']['url'],
                        Caption_status=data['contentDetails']['caption']
                    )
                    video_lists.append(video_details)

            next_page_token = playlist_response.get('nextPageToken')

            if not next_page_token:
                break

    return video_lists


##### Function to fetch comment details #####
def get_comment_info(video_lists):
    comment_datas = []
    
    # if comment disabled in channel
    try:
        for video in video_lists: 
            video_id = video['Video_id']
            next_page_token = None
            
            while True:
                comment_response = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token
                ).execute()

                for comment_item in comment_response['items']:

                    # Convert the ISO 8601 date format to MySQL DATETIME format
                    comment_published_date = comment_item['snippet']['topLevelComment']['snippet']
                    published_at = datetime.strptime(comment_published_date['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                    
                    #data collection
                    comment_data = dict(
                        Comment_id = comment_item['snippet']['topLevelComment']['id'],
                        Video_id=video_id,
                        Comment_Text = comment_item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author = comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt = published_at
                    )
                    
                    comment_datas.append(comment_data)

                next_page_token = comment_response.get('nextPageToken')
                if not next_page_token:
                    break 

    except Exception as e:
        pass

    return comment_datas

##### function to create tables #####
def create_tables():

# create channel table
    channel_table= """CREATE TABLE IF NOT EXISTS channel(
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        channel_type VARCHAR(255),
        channel_views INT,
        channel_description TEXT,
        channel_status VARCHAR(255)
    )"""

    #execute the query
    cursor.execute(channel_table)

# create playlist table
    playlist_table= """CREATE TABLE IF NOT EXISTS playlist(
        playlist_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        playlist_name VARCHAR(255),
        FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
    )"""

    #execute the query
    cursor.execute(playlist_table)

# create video table
    video_table= """CREATE TABLE IF NOT EXISTS video(
        video_id VARCHAR(255) PRIMARY KEY,
        playlist_id VARCHAR(255),
        video_name VARCHAR(255),
        video_description TEXT,
        published_date DATETIME,
        view_count INT,
        like_count INT,
        dislike_count INT,
        favorite_count INT,
        comment_count INT,
        duration INT,
        thumbnail VARCHAR(255),
        caption_status VARCHAR(255),
        FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
    )"""

    #execute the query
    cursor.execute(video_table)


# create comment table
    comment_table= """CREATE TABLE IF NOT EXISTS comment(
        comment_id VARCHAR(255) PRIMARY KEY,
        video_id VARCHAR(255),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_published_date DATETIME,
        FOREIGN KEY (video_id) REFERENCES video(video_id)
    )"""

    #execute the query
    cursor.execute(comment_table)

    # Commit changes
    mydb.commit()

##### function to upload data into sql #####
def upload_to_sql(channel_datas, playlists, video_lists, comment_datas):

    # Insert data into channel table
    for data in channel_datas:
        cursor.execute("""INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
                          VALUES (%s, %s, %s, %s, %s, %s)
                          ON DUPLICATE KEY UPDATE
                          channel_name=VALUES(channel_name), channel_type=VALUES(channel_type),
                          channel_views=VALUES(channel_views), channel_description=VALUES(channel_description),
                          channel_status=VALUES(channel_status)""",
                       (data['Channel_id'], data['Channel_name'], data['Channel_type'], data['Channel_views'],
                        data['Channel_description'], data['Privacy_status']))

    # Insert data into playlist table
    for playlist in playlists:
        cursor.execute("""INSERT INTO playlist (playlist_id, channel_id, playlist_name)
                          VALUES (%s, %s, %s)
                          ON DUPLICATE KEY UPDATE
                          playlist_name=VALUES(playlist_name)""",
                       (playlist['playlist_id'], data['Channel_id'], playlist['playlist_name']))

    # Insert data into video table
    for video in video_lists:
        cursor.execute("""INSERT INTO video (video_id, playlist_id, video_name, video_description, published_date,
                                              view_count, like_count, dislike_count, favorite_count, comment_count,
                                              duration, thumbnail, caption_status)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                          ON DUPLICATE KEY UPDATE
                          video_name=VALUES(video_name), video_description=VALUES(video_description),
                          published_date=VALUES(published_date), view_count=VALUES(view_count),
                          like_count=VALUES(like_count), dislike_count=VALUES(dislike_count),
                          favorite_count=VALUES(favorite_count), comment_count=VALUES(comment_count),
                          duration=VALUES(duration), thumbnail=VALUES(thumbnail), caption_status=VALUES(caption_status)""",
                       (video['Video_id'], playlist['playlist_id'], video['Video_name'], video['Video_description'],
                        video['Published_date'], video['View_count'], video['Like_count'],
                        video['Dislike_count'], video['Favorite_count'], video['Comment_count'],
                        video['Duration'], video['Thumbnail'], video['Caption_status']))

    # Insert data into comment table
    for comment in comment_datas:
        cursor.execute("""INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date)
                          VALUES (%s, %s, %s, %s, %s)
                          ON DUPLICATE KEY UPDATE
                          comment_text=VALUES(comment_text), comment_author=VALUES(comment_author),
                          comment_published_date=VALUES(comment_published_date)""",
                       (comment['Comment_id'], comment['Video_id'], comment['Comment_Text'],
                        comment['Comment_Author'], comment['Comment_PublishedAt']))

    # Commit changes
    mydb.commit()

# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    tab1, tab2 = st.tabs(["$\huge EXTRACT $"  ,  "$\huge TRANSFORM $"])

    # initialize a session state to store extract data
    if 'ch_details' not in st.session_state:
        st.session_state['ch_details'] = None
    if 'playlists' not in st.session_state:
        st.session_state['playlists'] = None
    if 'videos' not in st.session_state:
        st.session_state['videos'] = None
    if 'comments' not in st.session_state:
        st.session_state['comments'] = None

    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID's :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
        
        if ch_id and st.button("Extract Data"):
            ch_details = channel_details(ch_id)

            if ch_details:
                for channel in ch_details:
                    st.write(f'#### Extracted data from :green["{channel["Channel_name"]}"] channel')

                st.table(ch_details)
                st.session_state['ch_details'] = ch_details #store extract details

                # Extract playlists
                playlists = []
                for channel in ch_details:
                    channel_playlists = get_playlists(channel)
                    playlists.extend(channel_playlists)
                    st.write(f'#### Playlist Ids')

                st.table(playlists)
                st.session_state['playlists'] = playlists
                
                # Extract videos
                videos = video_details(playlists)
                st.write(f'#### Video Details')
                st.table(videos)
                st.session_state['videos'] = videos
                
                # Extract comments
                comments = get_comment_info(videos)
                st.write(f'#### Comment info')
                st.table(comments)
                st.session_state['comments'] = comments

            else:
                st.write("No data found for the given channel IDs.")
        
    # TRANSFORM TAB
    with tab2: 
        
        #Assigning session state values
        if st.session_state['ch_details'] is not None:
            st.write("### Transform Data")
            ch_details = st.session_state['ch_details']
        if st.session_state['playlists'] is not None:
            playlists= st.session_state['playlists']
        if st.session_state['videos'] is not None:
            videos= st.session_state['videos']
        if st.session_state['comments'] is not None:
            comments= st.session_state['comments'] 

            # upload data into sql
            if st.button("Upload Data to SQL"):
                with st.spinner('Uploading data to SQL...'):
                    create_tables()
                    upload_to_sql(ch_details,playlists,videos,comments)
                    
                    st.success("Transformed data successfully uploaded to SQL database.")
        else:
            st.write("No data to transform. Please extract data in the EXTRACT tab first.")


##### Analysis Zone #####
if selected == "Analysis Zone":
    st.markdown("<span style='color:red;font-size:30px;font-weight: bold; font-style:italic'> Available Channels for Analysing</span>",unsafe_allow_html=True)

    # Checkbox to toggle displaying the table
    display_table = st.checkbox("Show Channels Table")

    # Query the database to fetch available channels
    cursor.execute("SELECT channel_id, channel_name FROM channel")
    channels = cursor.fetchall()

    # Display channels as a table
    if display_table:
        if channels:
            df_channels = pd.DataFrame(channels, columns=["Channel ID", "Channel Name"])
            st.write(df_channels)
    else:
        st.write("No channels found in the database.")


##### Query Zone #####
if selected == "Query Zone":
    st.markdown("<span style='color:red;font-size:30px;font-weight: bold; font-style:italic'> Queries and Results</span>",unsafe_allow_html=True)
    st.write("(Queries were answered based on :orange[Channel Data analysis])")

    # Question Dropdown menu
    questions = st.selectbox('Select your question:',
        ['1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    # query for each questions
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT video_name AS Video_name, channel_name AS Channel_name
                            FROM video,channel
                            ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT c.channel_name AS Channel_Name, COUNT(v.video_id) AS Total_Videos
                        FROM video v
                        JOIN playlist p ON p.playlist_id = v.playlist_id
                        JOIN channel c ON p.channel_id = c.channel_id
                        GROUP BY p.playlist_id
                        ORDER BY total_videos DESC;""")
       
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

        # plots
        st.write("### :blue[Number of videos in each channel :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT v.video_name, v.view_count, c.channel_name AS Channel_Name
                        FROM video v
                        JOIN playlist p ON p.playlist_id = v.playlist_id
                        JOIN channel c ON p.channel_id = c.channel_id
                        ORDER BY v.view_count DESC
                        LIMIT 10;""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
        # plots
        st.write("### :blue[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[1]
                    )
        st.plotly_chart(fig,use_container_width=False)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT video_name AS Video_Title, b.Total_Comments
                            FROM video AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comment GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC;""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT c.channel_name AS Channel_Name, v.video_name AS Title, v.like_count AS Highest_likes
                            from video v
                            JOIN playlist p ON p.playlist_id = v.playlist_id
                            join channel c on p.channel_id = c.channel_id
                            order by v.like_count desc; """)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
        # plots
        st.write("### :blue[Most liked videos with their Channel name :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[2],
                     orientation='v',
                     color=cursor.column_names[2]
                    )
        st.plotly_chart(fig,use_container_width=False)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT v.video_name AS Video_Name, 
                        SUM(v.like_count) AS Total_Likes, 
                        SUM(v.dislike_count) AS Total_Dislikes
                        FROM video v
                        GROUP BY v.video_name
                        ORDER BY Total_Likes DESC; """)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name , channel_views AS Views
                            FROM channel
                            ORDER BY channel_views desc;""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
        # plots
        st.write("### :blue[Most viewed channels with their corresponding name :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[1]
                    )
        st.plotly_chart(fig,use_container_width=False)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT c.channel_name AS Channel_Name, v.published_date AS Published_At
                            FROM video v
                            JOIN playlist p ON p.playlist_id = v.playlist_id
                            JOIN channel c ON p.channel_id = c.channel_id
                            WHERE v.published_date LIKE '2022%'
                            ORDER BY v.published_date; """)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT c.channel_name AS Channel_Name, 
                        ROUND(AVG(v.duration)/60,2) AS Average_Duration
                        FROM video v
                        JOIN playlist p ON p.playlist_id = v.playlist_id
                        JOIN channel c ON p.channel_id = c.channel_id
                        GROUP BY c.channel_name
                        ORDER BY Average_Duration desc; """)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
        # plots
        st.write("### :blue[Average duratin of each channel's video:]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[1]
                    )
        st.plotly_chart(fig,use_container_width=False)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""select v.video_name AS Video_Name, c.channel_name AS Channel_Name,
                        v.comment_count as Highest_comments
                        FROM video v
                        JOIN playlist p ON p.playlist_id = v.playlist_id
                        JOIN channel c ON p.channel_id = c.channel_id
                        ORDER BY Highest_comments desc; """)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
        # plots
        st.write("### :blue[Highest comments for each channel's videos:]")
        fig = px.bar(df,
                     x=cursor.column_names[1],
                     y=cursor.column_names[2],
                     orientation='v',
                     color=cursor.column_names[2]
                    )
        st.plotly_chart(fig,use_container_width=False)