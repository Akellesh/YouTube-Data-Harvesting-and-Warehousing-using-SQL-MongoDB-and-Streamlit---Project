# Import Basic Packages
import pandas as pd
import numpy as np
import streamlit as st
from streamlit_option_menu import option_menu

# Import Packages for DB
import pymongo
import psycopg2
from psycopg2 import DatabaseError
from googleapiclient.discovery import build

# YouTube API
api_key = "AIzaSyDFWDGYi9U5UJJn_KvrvG8t55Q-qSzolEs"
youtube_api = build('youtube', 'v3', developerKey=api_key)


# Initialize DataBase connection.
# Uses st.cache_resource to only run once, for models, connection, tools.
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

# 2. PostgreSQL - DB Operations
# -----------------------------
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
        options=["View Youtube Channel", "Analyse Youtube Channel"],
        icons=["database", "database-add"], menu_icon="database-gear",
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
            view_data = get_channel_stats(youtube_api, channel_id)
            if view_data:
                view_data_df = pd.DataFrame([view_data])
                st.success("Found Youtube Channel")
                st.dataframe(view_data_df)

                st.table(view_data_df)

            else:
                st.success("No Youtube Channel Found")



if selected == "Contact":
    st.subheader('Youtube_Data_Harvesting')

# if selected == "Home":

