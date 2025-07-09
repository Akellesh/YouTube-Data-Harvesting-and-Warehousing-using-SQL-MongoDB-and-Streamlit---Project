# ----- Import Basic Packages ----- #
import pandas as pd
import numpy as np
import json
import bson
import time
import re
from datetime import datetime
import streamlit as st
from streamlit.runtime.caching import save_media_data
from streamlit_option_menu import option_menu
import plotly.express as px

# --------- Import Packages for DB --------- #
from pymongo import MongoClient, errors
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import DatabaseError

# ------ Import Package for Google API ------ #
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --------------- YouTube API ------------------ #
@st.cache_resource
def get_youtube_api():
    try:
        api_key = "AIzaSyDFWDGYi9U5UJJn_KvrvG8t55Q-qSzolEs"
        youtube_api_call = build('youtube', 'v3', developerKey=api_key)
        return youtube_api_call
    except HttpError as e:
        if e.resp.status == 403:
            st.error("🔒 Quota exceeded or API key invalid.")
        else:
            st.error(f"❌ YouTube API error: {e}")
        st.stop()

youtube_api = get_youtube_api()

# -------------- Safely Call Youtube API -------------- #
def safe_api_call(func, *args, retries=3, delay=2):
    for i in range(retries):
        try:
            return func(*args)
        except Exception as e:
            time.sleep(delay)
        except HttpError as e:
            # status = e.resp.status
            # error_msg = str(e)
            time.sleep(delay)
    return None

# ----------- MongoDB Setup -------------- #
# Refers Connection with MongoDB
@st.cache_resource
def get_mongo_client():
    connection_url = "mongodb+srv://akelleshv:Guvi2023@youtubecluster.fv56pkj.mongodb.net/?retryWrites=true&w=majority&appName=YoutubeCluster"

# Creating Client Object for connection based on pymongo and refers connection link
    try:
        client = MongoClient(connection_url, serverSelectionTimeoutMS=3000)
        client.admin.command('ping') # test Connection
        return client
    except errors.ServerSelectionTimeoutError as e:
        st.error(f"❌ Failed to connect to MongoDB. Check URI or server. {e}")
        st.stop()

# Cached MongoDB client with Auto-Connect
client = get_mongo_client()
# Creating mg_yth_db object for DataBase based on client and refers YouTubeHarvest
mg_yth_db = client['YouTubeHarvest']
# Creating collection_list for Collection based on mg_yth_db
# Optionally get collection list once
collection_list = mg_yth_db.list_collection_names()

# ---- Helper Functions ---- #
# def sanitize(name):
#     return re.sub(r'[.$]', '_', name)

# -------- Initialize Postgres DataBase connection. -----------#
# --Uses st.cache_resource to only run once, for models, connection, tools. --#
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])
conn = init_connection()

# ---------------- PostgreSQL - DB Operations ---------------- #
def create_postgrsql_tables():
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Channel_table (Channel_id VARCHAR(50) PRIMARY KEY, Channel_Name VARCHAR(50), 
                Subscribers INT,Channnel_views INT, Total_videos INT, harvested_time TIMESTAMP);""")
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
        cur.execute("""CREATE TABLE IF NOT EXISTS Channel_Videos (Video_id VARCHAR(50) PRIMARY KEY, Playlist_id VARCHAR(50), 
                    Video_name VARCHAR(120), Video_description TEXT, Published_date TIMESTAMP, View_count INT
            Like_count INT, Dislike_count INT, Favorite_count INT, Comments_count INT, Duration INT, 
            Thumbnail VARCHAR(255), Caption_status VARCHAR(255));""")
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS Channel_Comments (Comment_id VARCHAR(50) PRIMARY KEY, Video_id VARCHAR(50), 
            Comment_text TEXT, Comment_date TIMESTAMP, Comment_author VARCHAR(50), Comment_like INT, 
            ReplyCount INT);""")
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

# ---------- Insert Channel meta - PostgreSQL------------- #
def insert_channel_meta():
    try:
        cur = conn.cursor()
        cur.executemany("""INSERT INTO channel_meta (channel_name, subscribers, views, total_videos, harvested_at)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (channel_name) DO
                       UPDATE  SET subscribers = EXCLUDED.subscribers, views = EXCLUDED.views, total_videos = EXCLUDED.total_videos, 
                           harvested_at = EXCLUDED.harvested_at;""", meta_rows)
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

# ---------- Insert Channel Videos - PostgreSQL------------- #
def insert_channel_videos():
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO channel_videos (channel_name, video_id, video_title,
                                                   published_at, view_count)
                       VALUES %s ON CONFLICT (video_id) 
                        DO NOTHING;""", video_rows)
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

# ---------- Insert Channel comments - PostgreSQL------------- #
def insert_channel_comments():
    try:
        cur = conn.cursor()
        execute_values(cur, """INSERT INTO channel_comments (channel_name, video_id, comment_text, author,
                                                     published_at)
                       VALUES %s;""", comment_rows)
        conn.commit()
    except DatabaseError as e:
        conn.rollback()
        st.error(f"Database Error: {e}")
    finally:
        cur.close()

# ------ Function to get Channel Stats -------- #
def get_channel_stats(youtube_api, channel_id):
    request = youtube_api.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()
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

# --------------------- Function to get Video Stats -------------------------- #
def get_video_stats(youtube_api, playlist_id):
    videos = []
    next_page_token = None
    count = 0

    while True:
        playlist_response = youtube_api.playlistItems().list(part="contentDetails",playlistId=playlist_id,
            maxResults=50,pageToken=next_page_token).execute()
        # maxResults=min(max_results - count, 50)
        video_ids = [item['contentDetails']['videoId'] for item in playlist_response['items']]
        count += len(video_ids)

        if not video_ids:
            break

        video_response = youtube_api.videos().list(part="snippet,contentDetails,statistics",
            id=",".join(video_ids)).execute()

        for item in video_response['items']:
            video_data = {
                "video_id": item['id'],
                "video_title": item['snippet']['title'],
                "published_at": item['snippet']['publishedAt'],
                "view_count": item['statistics'].get('viewCount', 0),
                "like_count": item['statistics'].get('likeCount', 0),
                "comment_count": item['statistics'].get('commentCount', 0),
                "duration": item['contentDetails']['duration'],
                "description": item['snippet'].get('description', '')
            }
            videos.append(video_data)

        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token: # or count >= max_results:
            break

    return videos

# --------------------- Function to get Comments Stats --------------------- #
def update_comment_stats(youtube_api, video_ids, channel_name, max_comments_per_video=50):
    all_comments = []

    for video_id in video_ids:
        try:
            next_page_token = None
            count = 0

            while True:
                comment_response = youtube_api.commentThreads().list(part="snippet", videoId=video_id,
                    maxResults=min(max_comments_per_video - count, 100), pageToken=next_page_token,
                    textFormat="plainText").execute()

                for item in comment_response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comment_data = {
                        "video_id": video_id,
                        "comment_id": item['id'],
                        "author": comment.get('authorDisplayName'),
                        "text": comment.get('textDisplay'),
                        "like_count": comment.get('likeCount', 0),
                        "published_at": comment.get('publishedAt')
                    }
                    all_comments.append(comment_data)

                count += len(comment_response['items'])

                next_page_token = comment_response.get('nextPageToken')
                if not next_page_token: # or count >= max_comments_per_video
                    break

        except Exception as e:
            print(f"Failed to fetch comments for video {video_id}: {e}")
            continue

    return all_comments

# ----------- Function to get all the channel video details -------------- #
def extract_channel_all_details(youtube_api, channel_id):
    # Get channel info
    with st.spinner('Fetching channel statistics...'):
        channel_statistics = get_channel_stats(youtube_api, channel_id)
        if not channel_statistics:
            return None
    with st.spinner('Fetching playlists...'):
        playlist_id = channel_statistics.get('playlist_id')
        if not playlist_id:
            return None

    # Get all videos
    # Show Progress Indication
    with st.spinner('Fetching video statistics...'):
        video_statistics = get_video_stats(youtube_api, playlist_id)

    # Extract video IDs
    video_ids = [video.get('video_id') for video in video_statistics if video.get('video_id')]

    # Get all comments
    comment_statistics = []
    # Showing Progress Indication for Each video / comments
    progress_bar = st.progress(25, text="Fetching comments for each videos...")
    for i, vid in enumerate(video_ids):
        comment_statistics += update_comment_stats(youtube_api, [vid], channel_statistics.get("Channel_name", "Unknown"))
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

# ---------------------------------- Streamlit UI -------------------------------------- #
# -------------------------------------------------------------------------------------- #
# ------------------------ Streamlit Sidebar Menu and YDH_DB --------------------------- #
st.set_page_config(page_title="Youtube_Data_Harvesting", layout="wide")

with st.sidebar:
    selected = option_menu(
        menu_title="Youtube_Data_Harvesting Menu", options=["Home","---", "YDH_DB", "---","Contact"],
        icons=["house", "upload", "envelope"], # "gear",
        menu_icon="cast", default_index=0,
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
api_status = "Waiting for Test YouTube API Connection response..."
if selected == "YDH_DB":
    selected = option_menu(
        menu_title="Youtube_Data_Harvesting_DataBase Menu",
        options=["Search and Extract Youtube Channel", "View Saved Channels and Migrate", "Analyse Youtube Channel"],
        icons=["database", "database-add", "gear"], menu_icon="database-gear",
        default_index=0, orientation="horizontal")

    # ----------------- Search and Extract Youtube Channel ---------------- #
    if selected == "Search and Extract Youtube Channel":
        st.markdown("### 🔍 Test YouTube API Key")
        # api_status = "Waiting for Test YouTube API Connection response..."
        if st.button("Test YouTube API Connection"):
            try:
                # Try a basic call using Google Developers Channel ID
                test_channel_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw"
                response = youtube_api.channels().list(part="snippet", id=test_channel_id).execute()

                channel_info = response['items'][0]['snippet']
                # st.success("✅ YouTube API key is valid!")
                api_status = "✅ YouTube API key is valid, API connection is established!"
                # st.write(f"**Channel Name:** {channel_info['title']}")
                # st.write(f"**Description:** {channel_info['description'][:150]}...")

            except Exception as e:
                st.error(f"❌ YouTube API test failed: {e}")

        col1, col2, col3 = st.columns([4, 1, 2])
        with col1:
            st.subheader("🔍 Enter Channel Id:")
            channel_id = st.text_input("")
            st.markdown(api_status)
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
            # extracted_data = safe_api_call(extract_channel_all_details,youtube_api, channel_id)
            # st.markdown(api_status)
            extracted_data = extract_channel_all_details(youtube_api, channel_id)
            if extracted_data:
                channel_name = extracted_data.get('Channel_info', {}).get('Channel_name')
                if not channel_name:
                    st.error("❌ Channel name is missing in extracted data. Cannot proceed with storage.")
                    st.stop()
                # Separate Collections for channel, videos, comments
                # Save metadata with timestamp
                mg_yth_db[f"{channel_name}_meta"].delete_many({})
                mg_yth_db[f"{channel_name}_meta"].insert_one({
                    "Channel_name": extracted_data.get('Channel_name'),
                    "Subscribers": extracted_data.get('Subscribers'),
                    "Views": extracted_data.get('Views'),
                    "Total_videos": extracted_data.get('Total_videos'),
                    "Harvested_at": datetime.now().isoformat()
                })

                # Save videos
                mg_yth_db[f"{channel_name}_videos"].delete_many({})
                if 'Video_info' in extracted_data and extracted_data['Video_info']:
                    mg_yth_db[f"{channel_name}_videos"].insert_many(extracted_data['Video_info'])

                # Save comments
                mg_yth_db[f"{channel_name}_comments"].delete_many({})
                if 'Comment_info' in extracted_data and extracted_data['Comment_info']:
                    mg_yth_db[f"{channel_name}_comments"].insert_many(extracted_data['Comment_info'])


                st.success("✅ Harvest complete and saved to MongoDB")

                # Summary
                st.write(f"📺 Channel: {channel_name}")
                st.write(f"🎞️ Videos: {len(extracted_data.get('Video_info', []))}")
                st.write(f"💬 Comments: {len(extracted_data.get('Comment_info', []))}")

                # Download as JSON
                def convert_bson(obj):
                    if isinstance(obj, bson.ObjectId):
                        return str(obj)
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    raise TypeError(f"Type {type(obj)} not serializable")

                st.download_button("Download JSON", json.dumps(extracted_data, indent=2, default=convert_bson), f"{channel_name}_data.json")

                # Plot Chart
                video_df = pd.DataFrame(extracted_data.get('Video_info', []))
                if not video_df.empty:
                    video_df['view_count'] = pd.to_numeric(video_df.get('view_count', 0), errors='coerce')
                    fig = px.bar(video_df.head(10), x='video_title', y='view_count', title='Top 10 Videos by Views')
                    st.plotly_chart(fig)

                # Optional channel preview
                # st.subheader("📋 Channel Info")
                # st.json(extracted_data)  # or use st.dataframe if tabular
            else:
                st.error("Youtube Channel not found. Check the ID and try again. or failed to retrieve.", icon="🚨")

    # ----------------- View Saved Channels and Migrate ---------------- #
    if selected == "View Saved Channels and Migrate":
        collection_names = mg_yth_db.list_collection_names()
        user_channels = [c for c in mg_yth_db.list_collection_names() if c.endswith('_meta')]
        st.markdown("### 🔍 Select Saved Youtube Channel from the MongoDB")
        # selected_collection = st.selectbox("",saved_collections)

        # user_channels = sorted(set(name.rsplit('_', 1)[0] for name in collection_names if name.endswith('_meta')))
        selected_channel = st.selectbox("", user_channels)

        if selected_channel:
            migrate_to_sql = st.button("Migrate Youtube Channel to PostgreSQL")
            doc = mg_yth_db[selected_channel].find_one()
            # st.subheader("Channel Info")
            # st.json(doc)

            video_collection = selected_channel.replace("_meta", "_videos")
            if video_collection in mg_yth_db.list_collection_names():
                videos_df = pd.DataFrame(mg_yth_db[video_collection].find())
                st.subheader("Video Data")
                st.dataframe(videos_df)

            # comment_collection = selected_channel.replace("_meta", "_comments")
            # if comment_collection in mg_yth_db.list_collection_names():
            #     comments_df = pd.DataFrame(mg_yth_db[comment_collection].find())
                # st.subheader("Comment Data")
                # st.dataframe(comments_df)

            if migrate_to_sql:
                create_postgrsql_table()
                try:
                    # ------ Get MonngoDB Data ------ #
                    meta = mg_yth_db[f"{selected_channel}_meta"].find_one()
                    videos = list(mg_yth_db[f"{selected_channel}_videos"].find())
                    comments = list(mg_yth_db[f"{selected_channel}_comments"].find())
                    meta_rows = [(selected_channel, meta.get('Channel_name'), meta.get('Subscribers', 0), meta.get('Views', 0),
                                  meta.get('Total_videos', 0), meta.get('Harvested_at'))]
                    if meta_rows:
                        insert_channel_meta(meta_rows)

                    # -- Insert Videos
                    video_rows = [(selected_channel, v.get("video_id"), v.get("video_title"), v.get("published_at"),
                            int(v.get("view_count", 0))) for v in videos]
                    if video_rows:
                        insert_channel_videos(video_rows)

                    comment_rows = [(selected_channel, c.get("video_id"), c.get("comment_text"), c.get("author"),
                                     c.get("published_at")) for c in comments]
                    if comment_rows:
                        insert_channel_comments(comment_rows)

                    st.success(f"✅ Channel '{selected_channel}' migrated to PostgreSQL")

                except Exception as e:
                    st.error(f"❌ Migration failed: {e}")


    # ----------------- Analyse Youtube Channel ---------------- #
    if selected == "Analyse Youtube Channel":
        st.markdown("###")

if selected == "Contact":
    st.header('Project: Youtube_Data_Harvesting')
    st.subheader("My Contact Details")
    st.write("Created by: Akellesh Vasudevan")
    st.write("LinkedIn Profile:")
    st.markdown("https://www.linkedin.com/in/akellesh/")
    st.write("Github Profile:")
    st.markdown("https://github.com/Akellesh/YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit---Project")

if selected == "Home":
    st.header('Project: Youtube_Data_Harvesting')

