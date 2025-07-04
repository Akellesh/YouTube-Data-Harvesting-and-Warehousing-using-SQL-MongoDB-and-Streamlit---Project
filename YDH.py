# ---Import Basic Packages--- #
import pandas as pd
import numpy as np
import json
import time
import re
from datetime import datetime
import streamlit as st
from streamlit_option_menu import option_menu

# ----Import Packages for DB -----#
from pymongo import MongoClient
import psycopg2
from psycopg2 import DatabaseError

# ------Import Package for Googel API -----#
from googleapiclient.discovery import build

# ---- MongoDB Setup --------------------------- #
# Refers Connection with MongoDB
connection_url = "mongodb+srv://akelleshv:Guvi2023@youtubecluster.fv56pkj.mongodb.net/?retryWrites=true&w=majority&appName=YoutubeCluster"
# Creating Client Object for connection based on pymongo and refers connection link
client = MongoClient(connection_url)
# Creating mg_yth_db object for DataBase based on client and refers YouTubeHarvest
mg_yth_db = client['YouTubeHarvest']
# Creating collection_list for Collection based on mg_yth_db
collection_list = mg_yth_db.list_collection_names()

# ---- Helper Functions ---- #
# def sanitize(name):
#     return re.sub(r'[.$]', '_', name)

# ----------YouTube API ------------#
api_key = "AIzaSyDFWDGYi9U5UJJn_KvrvG8t55Q-qSzolEs"
youtube_api = build('youtube', 'v3', developerKey=api_key)

def safe_api_call(func, *args, retries=3, delay=2):
    for i in range(retries):
        try:
            return func(*args)
        except Exception as e:
            time.sleep(delay)
    return None

# --------Initialize Postgres DataBase connection. -----------#
# --Uses st.cache_resource to only run once, for models, connection, tools. --#
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

# ------PostgreSQL - DB Operations -------#
def create_table():
    conn = init_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Channel_table (Channel_id VARCHAR(255) PRIMARY KEY, Channel_Name VARCHAR(255), 
                Channnel_Type VARCHAR(255),Channnel_views INT, Channel_description TEXT, Channel_status VARCHAR(255));""")
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS Playlist_table (Playlist_id VARCHAR(255) PRIMARY KEY, Playlist_Name VARCHAR(255),
                       Channel_Name VARCHAR(255), Channel_id VARCHAR(255));""")
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS Comments_table (Comment_id VARCHAR(255), Video_id VARCHAR(255), 
            Comment_text TEXT, Comment_type VARCHAR(255), Comment_author VARCHAR(255), Comment_published_date DATETIME, 
            Comment_status VARCHAR(255));""")
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS Videos_table (Video_id VARCHAR(255), Playlist_id VARCHAR(255), 
                    Video_name VARCHAR(255), Video_description TEXT, Published_date DATETIME, View_count INT
            Like_count INT, Dislike_count INT, Favorite_count INT, Comments_count INT, Duration INT, 
            Thumbnail VARCHAR(255), Caption_status VARCHAR(255));""")
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

def get_channel_stats(youtube_api, channel_id):
    request = youtube_api.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()
    # st.write(response)
    try:
        data = dict(Channel_Id=channel_id,
          Channel_name=response['items'][0]['snippet']['title'],
          Subscribers=response['items'][0]['statistics']['subscriberCount'],
          Views=response['items'][0]['statistics']['viewCount'],
          Total_videos=response['items'][0]['statistics']['videoCount'],
          playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
        return data
    except KeyError:
        return False


def update_comment_stats(youtube, param, param1):
    pass


def get_video_stats(youtube, playlist_id):
    pass


def extract_channel_all_details(youtube, channel_id):
    # Get channel info
    channel_statistics = get_channel_stats(youtube, channel_id)
    if not channel_statistics:
        return None

    playlist_id = channel_statistics.get('playlist_id')
    if not playlist_id:
        return None

    # Get all videos
    # Show Progress Indication
    with st.spinner('Fetching video statistics...'):
        video_statistics = get_video_stats(youtube, playlist_id)

    # Extract video IDs
    video_ids = [video.get('video_id') for video in video_statistics if video.get('video_id')]

    # Get all comments
    comment_statistics = []
    # Showing Progress Indication for Each video / comments
    progress_bar = st.progress(0, text="Fetching comments...")
    for i, vid in enumerate(video_ids):
        comment_statistics += update_comment_stats(youtube, [vid], channel_statistics.get("Channel_name", "Unknown"))
        progress_bar.progress((i + 1) / len(video_ids))
    # comment_statistics = update_comment_stats(youtube, video_ids, channel_statistics.get("Channel_name", "Unknown_Channel"))
    # Pack into a single dictionary
    channel_data = {
        'Channel_info': channel_statistics,
        'Video_info': video_statistics,
        'Comment_info': comment_statistics,
        'Meta': {
            'Total Videos': len(video_statistics),
            'Total Comments': len(comment_statistics)
        },
        'last_updated': datetime.now().isoformat() # Added Timestamps for Tracking Updates
    }
    return channel_data







# Home Page Title
st.set_page_config(page_title="Youtube_Data_Harvesting", layout="wide")

with st.sidebar:
    selected = option_menu(
        menu_title="Youtube_Data_Harvesting Menu",
        options=["Home","---", "YDH_DB", "---","Contact"],
        icons=["house", "upload", "envelope"], # "gear",
        menu_icon="cast",
        default_index=0,
        # orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "#AFBFAB"},
            "icon": {"color": "orange", "font-size": "15px"},
            "nav-link": {
                "font-size": "15px",
                "text-align": "left",
                "margin": "5px",
                "--hover-color": "#eee",
            },
            "nav-link-selected": {"background-color": "grey"},
        },
    )

if selected == "YDH_DB":
    selected = option_menu(
        menu_title="Youtube_Data_Harvesting_DataBase Menu",
        options=["View Youtube Channel", "View Saved Channels", "Analyse Youtube Channel"],
        icons=["database", "database-add", "gear"], menu_icon="database-gear",
        default_index=0, orientation="horizontal")

    if selected == "View Youtube Channel":
        col1, col2, col3 = st.columns([4, 1, 2])
        with col1:
            channel_id = st.text_input("Enter Channel Id:")
        col4, col5, col6, col7 = st.columns([4, 4, 3, 3])
        with col4:
            Search = st.button("Search Youtube Channel")
        with col5:
            Extract = st.button("Extract Youtube Channel")
        if Search:
            view_data = safe_api_call(get_channel_stats,youtube_api, channel_id)
            if view_data:
                view_data_df = pd.DataFrame([view_data]).T
                st.success("Found Youtube Channel")
                st.dataframe(view_data_df[1:-1].style.background_gradient())
                # st.table(view_data_df)
            else:
                st.error("Youtube Channel not found. Check the ID and try again.", icon="🚨")

        if Extract:
            extracted_data = get_channel_stats(youtube_api, channel_id)
            if extracted_data:
                channel_name = extracted_data['Channel_info']['Channel_name']

                # Separate Collections for channel, videos, comments
                mg_yth_db[f"{channel_name}_meta"].delete_many({})
                mg_yth_db[f"{channel_name}_meta"].insert_one(extracted_data['Channel_info'])

                mg_yth_db[f"{channel_name}_videos"].delete_many({})
                if extracted_data['Video_info']:
                    mg_db[f"{channel_name}_videos"].insert_many(extracted_data['Video_info'])

                mg_yth_db[f"{channel_name}_comments"].delete_many({})
                if extracted_data['Comment_info']:
                    mg_yth_db[f"{channel_name}_comments"].insert_many(extracted_data['Comment_info'])

                st.success("✅ Harvest complete and saved to MongoDB")

                # Summary
                st.write(f"📺 Channel: {channel_name}")
                st.write(f"🎞️ Videos: {extracted_data['Meta']['Total Videos']}")
                st.write(f"💬 Comments: {extracted_data['Meta']['Total Comments']}")

                # Download as JSON
                st.download_button("Download JSON", json.dumps(extracted_data, indent=2), f"{channel_name}_data.json")

                # Plot Chart
                video_df = pd.DataFrame(extracted_data['Video_info'])
                if not video_df.empty:
                    video_df['view_count'] = pd.to_numeric(video_df['view_count'], errors='coerce')
                    fig = px.bar(video_df.head(10), x='video_title', y='view_count', title='Top 10 Videos by Views')
                    st.plotly_chart(fig)
                st.json(extracted_data['Channel_info'])  # or use st.dataframe if tabular

            else:
                st.error("Youtube Channel not found. Check the ID and try again. or failed to retrieve.", icon="🚨")

# ---- Admin Section: View Saved Channels ---- #
#     st.sidebar.header("View Saved Channels")
#     if selected == "View Saved Channels":
        # saved_collections = [c for c in mg_db.list_collection_names() if c.endswith('_meta')]
        # selected_collection = st.sidebar.selectbox("Select a Channel", saved_collections)

        # if selected_collection:
        #     doc = mg_yth_db[selected_collection].find_one()
        #     st.sidebar.subheader("Channel Info")
        #     st.sidebar.json(doc)
        #
        #     video_collection = selected_collection.replace("_meta", "_videos")
        #     if video_collection in mg_yth_db.list_collection_names():
        #         videos_df = pd.DataFrame(mg_yth_db[video_collection].find())
        #         st.subheader("Video Data")
        #         st.dataframe(videos_df)
        #
        #     comment_collection = selected_collection.replace("_meta", "_comments")
        #     if comment_collection in mg_yth_db.list_collection_names():
        #         comments_df = pd.DataFrame(mg_yth_db[comment_collection].find())
        #         st.subheader("Comment Data")
        #         st.dataframe(comments_df)



# if selected == "Contact":
#    st.header('Youtube_Data_Harvesting')
#
# if selected == "Home":


