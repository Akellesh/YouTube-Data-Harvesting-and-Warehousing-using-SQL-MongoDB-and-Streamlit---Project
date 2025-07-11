# ----- Import Basic Packages ----- #
import pandas as pd
import numpy as np
import json
import bson
import time
import re
from datetime import datetime
import isodate
from textblob import TextBlob
from langdetect import detect
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

# ---------- YouTube API Management --------------- #
# ---------- Safely Call Youtube API ------------- #
# --------- Track API usage ---------- #
api_counter = {"calls": 0}
# def count_api_call():
#     api_counter["calls"] += 1

# --- Cached API Wrapper with Rate Limiting ---- #
def safe_api_call(request_func, *args, **kwargs):
    try:
        api_counter["calls"] += 1
        return request_func(*args, **kwargs)
    except HttpError as e:
        error_reason = ''
        try:
            error_reason = e.error_details[0]['reason']
        except:
            pass

        if 'quotaExceeded' in str(e) or error_reason == "quotaExceeded":
            st.error("⚠️ Quota Exceeded: API limit may have been reached.")
            # Optional: Log to MongoDB
            mg_yth_db["audit_logs"].insert_one({
                "error": "quotaExceeded",
                "timestamp": datetime.now().isoformat(),
                "function": request_func.__name__,
                "args": str(args)
            })
            return None
        else:
            st.error(f"❌ API call failed: {e}")
            return None
    # except Exception as e:
    #     if "quota" in str(e).lower():
    #         st.error("⚠️ Quota Exceeded: API limit may have been reached.")
    #     raise

# YOUTUBE_API_KEYS1 = ["AIzaSyBazo7xhteXVNcyvIPe6CUe168J0msX5TM","AIzaSyA6Wdt3qNFOLrSvonskzHkyEYUlLDj8goY",
#                     "AIzaSyAeodEWTg_RhDwOVn_p0CZJ482Ero2uQQ4", "AIzaSyDFWDGYi9U5UJJn_KvrvG8t55Q-qSzolEs"]

YOUTUBE_API_KEYS = ["AIzaSyD4DcvQD6AM1otR5-Z0j4WSY3r6tJ8Lx0o", "AIzaSyAvW2AzCjeOeu79Vzlz_h3RUUuX0kdMIkI"]
api_index = 0

@st.cache_resource
def get_youtube_api():

    try:
        # "AIzaSyDFWDGYi9U5UJJn_KvrvG8t55Q-qSzolEs"
        # api_key = "AIzaSyAeodEWTg_RhDwOVn_p0CZJ482Ero2uQQ4"
        global api_index
        api_key = YOUTUBE_API_KEYS[api_index]
        api_index = (api_index + 1) % len(YOUTUBE_API_KEYS)
        youtube_api_call = build('youtube', 'v3', developerKey=api_key)
        return youtube_api_call
    except HttpError as e:
        if e.resp.status == 403:
            st.error("🔒 Quota exceeded or API key invalid.")
        else:
            st.error(f"❌ YouTube API error: {e}")
        st.stop()

youtube_api = get_youtube_api()

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
    # return psycopg2.connect(**st.secrets["postgres"])
    return psycopg2.connect(host="localhost", user="postgres", password="Post@2025", port=5434, dbname="ythdb")
# conn = init_connection()

def parse_duration_to_hms(duration_str):
    try:
        duration = isodate.parse_duration(duration_str)
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    except Exception:
        return None

# ---------------- PostgreSQL - DB Operations ---------------- #
def create_postgrsql_tables(conn):
    with st.spinner("🔧 Creating PostgreSQL tables..."):
        try:
            with conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS channel_table (channel_id VARCHAR(50) PRIMARY KEY, 
                    channel_name VARCHAR(50), subscribers INT, channel_views INT, total_videos INT, 
                    harvested_time TIMESTAMP);""")

                cur.execute("""CREATE TABLE IF NOT EXISTS channel_playlist (playlist_id VARCHAR(255) PRIMARY KEY, 
                    playlist_name VARCHAR(100), channel_name VARCHAR(255), channel_id VARCHAR(255), description TEXT,
                    item_count INT, privacy_status VARCHAR(50), published_at TIMESTAMP, 
                    harvested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""")

                cur.execute("""CREATE TABLE IF NOT EXISTS channel_videos (video_id VARCHAR(50) PRIMARY KEY, 
                    playlist_id VARCHAR(50), video_name VARCHAR(120), video_description TEXT, published_date TIMESTAMP,
                    category_id INT, duration TIME, video_quality VARCHAR(20), licensed VARCHAR(10), 
                    view_count INT, like_count INT, dislike_count INT, favorite_count INT, comments_count INT,  
                    thumbnail VARCHAR(120), caption_status VARCHAR(150));""")

                cur.execute("""CREATE TABLE IF NOT EXISTS channel_comments (comment_id VARCHAR(50) PRIMARY KEY, video_id VARCHAR(50), 
                    channel_name VARCHAR(50), comment_text TEXT, comment_date TIMESTAMP, comment_author VARCHAR(255), 
                    comment_like INT DEFAULT 0, reply_count INT DEFAULT 0, is_pinned BOOLEAN DEFAULT FALSE, 
                    is_hearted BOOLEAN DEFAULT FALSE, language VARCHAR(10), sentiment_score FLOAT, 
                    harvested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""")

            conn.commit()
        except Exception as e:
            conn.rollback()
            st.error(f"❌ Table creation failed: {e}")

# ---------- Insert Channel meta - PostgreSQL------------- #
def migrate_to_postgresql(conn, selected_channel, mg_yth_db):
    try:
        progress_bar = st.progress(0, text="📤 Starting migration...")
        create_postgrsql_tables(conn)

        # ------ Get MonngoDB Data ------ #
        meta = mg_yth_db[f"{selected_channel}_meta"].find_one()
        # st.dataframe(meta)
        if not meta:
            st.error(f"⚠️ No meta data found for the channel: {selected_channel}, Check the ID and try again.")
            return
        # playlists = list(mg_yth_db[f"{selected_channel}_playlists"].find()) if (f"{selected_channel}_playlists" in
        #                             mg_yth_db.list_collection_names()) else []
        playlists = list(mg_yth_db[f"{selected_channel}_playlist"].find())
        st.dataframe(playlists)
        for p in playlists:
            p.pop("_id", None)
        videos = list(mg_yth_db[f"{selected_channel}_videos"].find())

        comments = list(mg_yth_db[f"{selected_channel}_comments"].find())

        # Remove ObjectId (_id) fields
        # for item in [meta] + videos + comments:
        #     if isinstance(item, dict):
        #         item.pop('_id', None)

        def remove_mongo_ids(docs):
            for d in docs:
                if isinstance(d, dict):
                    d.pop('_id', None)
            return docs

        meta = remove_mongo_ids([meta])[0]  # single dict
        playlists = remove_mongo_ids(playlists)
        videos = remove_mongo_ids(videos)
        comments = remove_mongo_ids(comments)

        # ------------ Insert Channel meta - PostgreSQL ---------------- #
        progress_bar.progress(0.2, "📦 Preparing metadata for insert...")
        # meta_rows = [(selected_channel, meta.get('Channel_id'), meta.get('Subscribers', 0), meta.get('Views', 0),
        #               meta.get('Total_videos', 0), meta.get('Harvested_at', datetime.now()))]
        # assert all([meta.get("Channel_Id"), meta.get("Channel_name"),
        #             meta.get("Subscribers") is not None]), "Missing essential metadata fields"
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO channel_table (channel_id, channel_name, subscribers, channel_views, 
                                                      total_videos, harvested_time)
                       VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (channel_id) DO UPDATE SET 
                                channel_id = EXCLUDED.channel_id, channel_name = EXCLUDED.channel_name, 
                               subscribers = EXCLUDED.subscribers, channel_views = EXCLUDED.channel_views, 
                               total_videos = EXCLUDED.total_videos, harvested_time = EXCLUDED.harvested_time;""",
                        (meta.get("Channel_Id"), meta.get("Channel_name"),meta.get("Subscribers"), meta.get("channel_views"),
                         meta.get("total_videos"), meta.get("harvested_time"), datetime.now()))
            assert all([meta.get("Channel_Id"), meta.get("Channel_name"),
                        meta.get("Subscribers") is not None]), "Missing essential metadata fields"


        # --------------- Insert into channel_playlist ---------------- #
        progress_bar.progress(0.3, "🎞 Inserting video records...")
        if playlists:
            playlist_rows = [(p.get("playlist_id"), p.get("playlist_title"), selected_channel, p.get("Channel_id"),
                    p.get("description", ""), int(p.get("item_count", 0)), p.get("privacy_status", ""),
                    p.get("published_at"), datetime.now()) for p in playlists]
            with conn.cursor() as cur:
                execute_values(cur, """INSERT INTO channel_playlist (playlist_id, playlist_Name, channel_name,
                                channel_id, description, item_count, privacy_status, published_at, harvested_at)
                                    VALUES %s ON CONFLICT (playlist_id) DO NOTHING;""", playlist_rows)

        # ---------- Insert Channel Videos - PostgreSQL------------- #
        progress_bar.progress(0.45, "🎞 Inserting video records...")
        if videos:
            video_rows = [(v.get("video_id"), v.get("playlist_id"), v.get("video_title"), v.get("description", ""),
                    v.get("published_at"), int(v.get("category_id", 0)), parse_duration_to_hms(v.get("duration")),
                    v.get("definition", "hd"), v.get("licensed_content", "No"), int(v.get("view_count", 0)),
                    int(v.get("like_count", 0)), int(v.get("dislike_count", 0)), int(v.get("favorite_count", 0)),
                    int(v.get("comment_count", 0)), v.get("thumbnail", ""), v.get("caption_status", "Unknown"))
                           for v in videos]
            with conn.cursor() as cur:
                execute_values(cur, """INSERT INTO channel_videos (video_id, playlist_id, video_name, 
                        video_description, published_date, category_id, duration, video_quality, 
                        licensed, view_count, like_count, dislike_count, favorite_count, comments_count,
                        thumbnail, caption_status) VALUES %s ON CONFLICT (video_id) DO NOTHING;""", video_rows)

        if not videos:
            st.info("No videos to migrate.")

        # ---------- Insert Channel comments - PostgreSQL------------- #
        progress_bar.progress(0.7, "💬 Inserting comment records...")
        if comments:
            comment_rows = []
            for c in comments:
                text = c.get("comment_text", "")
                try:
                    sentiment = TextBlob(text).sentiment.polarity
                    lang = detect(text)
                except:
                    sentiment = None
                    lang = "en"
                comment_rows.append((c.get("comment_id"), c.get("video_id"), selected_channel, c.get("comment_text"),
                    c.get("comment_date"), c.get("author"), int(c.get("like_count", 0)), int(c.get("reply_count", 0)),
                    c.get("is_pinned", False), c.get("is_hearted", False), lang, sentiment, datetime.now()))
            with conn.cursor() as cur:
                execute_values(cur, """INSERT INTO channel_comments (comment_id, video_id, channel_name, comment_text, 
                        comment_date, comment_author, comment_like, reply_count, is_pinned, is_hearted,
                        language, sentiment_score, harvested_at) VALUES %s ON CONFLICT (comment_id) DO NOTHING;""", comment_rows)

        if not comments:
            st.info("No comments to migrate.")

        conn.commit()
        # ----- Migration Completed ------ #
        progress_bar.progress(1.0, "✅ Migration complete!")
        st.success(f"✅ Channel '{selected_channel}' migrated to PostgreSQL")
        st.write(f"📦 Migrated {len(videos)} videos and {len(comments)} comments for '{selected_channel}'")

    except Exception as e:
        conn.rollback()
        st.error(f"❌ Migration failed: {e}")

# ------------------- Function to get Channel Stats ------------------- #
@st.cache_data
def get_channel_stats(_youtube_api, channel_id):
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
          API_Calls=api_counter["calls"],
          playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
        return data
    except KeyError:
        return False

# --------------------- Function to get Playlist Info ------------------------ #
@st.cache_data
def get_playlist_info(_youtube_api, playlist_id, channel_name, channel_id):
    try:
        request = youtube_api.playlists().list(
            part='snippet,contentDetails,status',
            id=playlist_id
        )
        response = safe_api_call(request.execute)

        if response['items']:
            item = response['items'][0]
            playlist = {
                "playlist_id": playlist_id,
                "playlist_name": item['snippet']['title'],
                "channel_name": channel_name,
                "channel_id": channel_id,
                "description": item['snippet'].get('description', ''),
                "item_count": item['contentDetails'].get('itemCount', 0),
                "privacy_status": item['status'].get('privacyStatus', 'public'),
                "published_at": item['snippet'].get('publishedAt'),
                "harvested_at": datetime.now().isoformat()
            } # "API Calls": api_counter["calls"],
            return playlist
        else:
            return None
    except Exception as e:
        st.error(f"❌ Playlist fetching failed: {e}")
        return None

@st.cache_data
def get_all_playlists_for_channel(_youtube_api, channel_name, channel_id):
    playlists = []
    next_page_token = None
    while True:
        try:
            request = youtube_api.playlists().list(
                part='snippet,contentDetails,status',
                channelId=channel_id,
                maxResults=50, pageToken=next_page_token
            )
            # response = safe_api_call(request.execute)
            response = request.execute()
            for item in response.get('items', []):
                playlists.append({"playlist_id": item['id'], "playlist_name": item['snippet']['title'],
                                  "channel_name": channel_name, "channel_id": channel_id,
                                  "description": item['snippet'].get('description', ''),
                                  "item_count": item['contentDetails'].get('itemCount', 0),
                                  "privacy_status": item['status'].get('privacyStatus', 'public'),
                                  "published_at": item['snippet'].get('publishedAt'),
                                  "harvested_at": datetime.now().isoformat()
                })
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

        except Exception as e:
            st.error(f"❌ Playlist fetching failed: {e}")
            break
    return playlists


# --------------------- Function to get Video Stats -------------------------- #
@st.cache_data
def get_video_stats(_youtube_api, playlist_id, max_results=50):
    videos = []
    next_page_token = None
    count = 0

    while True:
        playlist_response = youtube_api.playlistItems().list(part="contentDetails",playlistId=playlist_id,
            maxResults=min(max_results - count, 50),pageToken=next_page_token).execute()

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


# --- Fetch all playlists from channel ---
@st.cache_data
def get_all_playlists(_youtube_api, channel_id):
    playlists = []
    request = youtube_api.playlists().list(part="snippet,contentDetails", channelId=channel_id, maxResults=50)
    while request:
        response = safe_api_call(request.execute)
        for item in response.get('items', []):
            playlists.append({
                "playlist_id": item["id"],
                "playlist_name": item["snippet"]["title"],
                "description": item["snippet"].get("description", ""),
                "item_count": item["contentDetails"].get("itemCount", 0),
                "privacy_status": item.get("status", {}).get("privacyStatus", "public"),
                "published_at": item["snippet"]["publishedAt"]
            })
        request = youtube_api.playlists().list_next(request, response)
    return playlists

# --- Fetch all videos from all playlists ---
@st.cache_data
def get_all_playlist_videos(_youtube_api, channel_id):
    all_videos = []
    all_playlists = get_all_playlists(youtube_api, channel_id)
    for pl in all_playlists:
        pl_id = pl["playlist_id"]
        videos = get_video_stats(youtube_api, pl_id)
        for v in videos:
            v["playlist_id"] = pl_id
        all_videos.extend(videos)
    return all_videos, all_playlists

# --------------------- Function to get Comments Stats --------------------- #
@st.cache_data
def update_comment_stats(_youtube_api, video_ids, channel_name, max_comments_per_video=10):
    all_comments = []

    for video_id in video_ids:
        try:
            next_page_token = None
            count = 0

            while True:
                comment_response = youtube_api.commentThreads().list(part="snippet", videoId=video_id,
                    maxResults=min(max_comments_per_video - count, 10), pageToken=next_page_token,
                    textFormat="plainText").execute()

                for item in comment_response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comment_data = {
                        "video_id": video_id, "comment_id": item['id'],
                        "author": comment.get('authorDisplayName'), "text": comment.get('textDisplay'),
                        "like_count": comment.get('likeCount', 0), "published_at": comment.get('publishedAt')
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
@st.cache_data(ttl=3600, show_spinner=False)
def extract_channel_all_details(_youtube_api, channel_id):
    # ---- Getting Channel Info ---- #
    progress_bar_extract = st.progress(0, text="📤 Starting Youtube channel Harvesting...")
    with st.spinner('Fetching channel statistics...'):
        # ----- channel_statistics = get_channel_stats(youtube_api, channel_id)
        channel_statistics = safe_api_call(get_channel_stats, _youtube_api, channel_id)
        if not channel_statistics:
            st.warning("⚠️ Channel statistics not available.")
            return None
        channel_name = channel_statistics.get('Channel_name')
    # ----- Old method ---- #
    # # ---- Getting Playlist Info ---- #
    # progress_bar_extract.progress(0.1, "")
    # with st.spinner('Fetching playlists statistics...'):
    #     # playlist_id = channel_statistics.get('playlist_id')
    #     channel_name = channel_statistics.get('Channel_name')
    #     # ------- playlist_id_statistics = get_playlist_info(youtube_api, playlist_id, channel_name, channel_id)
    #     playlist_id_statistics = safe_api_call(get_all_playlists_for_channel, _youtube_api, channel_name, channel_id)
    #     if not playlist_id_statistics:
    #         st.warning("⚠️ Playlist info retrieval failed.")
    #         return None
    #
    # # ---- Getting all videos ---- #
    # progress_bar_extract.progress(0.25, "")
    # with st.spinner('Fetching video statistics for all playlists...'):
    #
    #     # video_statistics = safe_api_call(get_video_stats, _youtube_api, playlist_id)
    #     # # Extract video IDs
    #     # video_ids = [video.get('video_id') for video in video_statistics if video.get('video_id')]
    #     video_statistics = []
    #     video_ids = []
    #
    #     for i, playlist in enumerate(playlist_id_statistics):
    #         pid = playlist.get("playlist_id")
    #         if pid:
    #             videos = safe_api_call(get_video_stats, _youtube_api, pid)
    #             if videos:
    #                 video_statistics.extend(videos)
    #                 video_ids.extend([video.get('video_id') for video in videos if video.get('video_id')])
    #         progress_bar_extract.progress(0.25 + (i + 1) / (len(playlist_id_statistics) * 4), text=f"Fetched from playlist {i+1}/{len(playlist_id_statistics)}")

    # ---- New method ---- #
    with st.spinner('📂 Fetching all playlists and videos...'):
        all_videos, all_playlists = get_all_playlist_videos(youtube_api, channel_id)
        video_ids = [v.get("video_id") for v in all_videos if v.get("video_id")]

    # Get all comments
    comment_statistics = []

    # Showing Progress Indication for Each video / comments
    progress_bar_extract.progress(0.6, text="Fetching comments for each videos...")
    for i, vid in enumerate(video_ids):
        # ------- comment_statistics += update_comment_stats(youtube_api, [vid], channel_statistics.get("Channel_name", "Unknown"))
        comment_statistics += safe_api_call(update_comment_stats, _youtube_api, vid, channel_statistics.get("Channel_name", "Unknown"), max_comments_per_video=50)
        progress_bar_extract.progress((i + 1) / len(video_ids), text=f"Fetched comments for video {i + 1}/{len(video_ids)}")

    # Pack into a single dictionary
    channel_data = {
        'Channel_info': channel_statistics, 'playlist_info': all_playlists , 'Video_info': all_videos,
        'Comment_info': comment_statistics,
        'Meta': {
            'Total Videos': len(all_videos), 'Total Comments': len(comment_statistics)},
        "API Calls": api_counter["calls"],
        'last_updated': datetime.now().isoformat() # Added Timestamps for Tracking Updates
    }
    # ------ Log success to MongoDB -------#
    mg_yth_db["audit_logs"].insert_one({
        "channel_id": channel_id,
        "channel_name": channel_name,
        "status": "success",
        "api_calls": api_counter["calls"],
        "video_count": len(all_videos),
        "comment_count": len(comment_statistics),
        "timestamp": datetime.now().isoformat()
    })
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
        options=["YT Channel Extractor", "Mongo Manager", "Postgres Manager", "YT Channel Analyzer"],
        icons=["database", "database-add", "gear"], menu_icon="database-gear",
        default_index=0, orientation="horizontal")

    # ----------------- Search and Extract Youtube Channel ---------------- #
    if selected == "YT Channel Extractor":
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

        # with st.form("channel_input_form"):
        #     channel_id = st.text_input("🔎 Enter Channel ID")
        #
        #     col1, col2 = st.columns([1, 1])
        #     with col1:
        #         Search = st.form_submit_button("Search Youtube Channel")
        #     with col2:
        #         Extract = st.form_submit_button("Extract Youtube Channel")
        #
        #     if Search and not channel_id:
        #         st.warning("⚠️ Please enter a Channel ID to continue.")
        #
        #     if Extract and not channel_id:
        #         st.warning("⚠️ Please enter a Channel ID to continue.")

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
            # extracted_data = extract_channel_all_details(youtube_api, channel_id)
            extracted_data = safe_api_call(extract_channel_all_details, youtube_api, channel_id)

            if extracted_data:
                channel_name = extracted_data.get('Channel_info', {}).get('Channel_name')
                if not channel_name:
                    st.error("❌ Channel name is missing in extracted data. Cannot proceed with storage.")
                    st.stop()

                # ---- Separate MongoDB Collections for channel, playlist, videos, comments ---- #
                # ---- Save metadata with timestamp ---- #
                channel_info = extracted_data.get('Channel_info', {})
                mg_yth_db[f"{channel_name}_meta"].delete_many({})
                mg_yth_db[f"{channel_name}_meta"].insert_one({
                    "Channel_name": channel_info.get('Channel_name'), "Subscribers": channel_info.get('Subscribers'),
                    "Views": channel_info.get('Views'), "Total_videos": channel_info.get('Total_videos'),
                    "Harvested_at": datetime.now().isoformat()})

                # ---- Save separate playlist collection ---- #
                playlist_info = extracted_data.get('playlist_info')
                if playlist_info:
                    mg_yth_db[f"{channel_name}_playlist"].delete_many({})
                    if isinstance(playlist_info, list):
                        mg_yth_db[f"{channel_name}_playlist"].insert_many(playlist_info)
                    elif isinstance(playlist_info, dict):
                        mg_yth_db[f"{channel_name}_playlist"].insert_one(playlist_info)
                    else:
                        st.warning("⚠️ Playlist info is in an unexpected format.")
                #     mg_yth_db[f"{channel_name}_playlist"].insert_many(playlist_info)
                # else:
                #     st.warning("⚠️ Playlist info not available for this channel.")

                # mg_yth_db[f"{channel_name}_playlist"].delete_many({})
                # mg_yth_db[f"{channel_name}_playlist"].insert_one(extracted_data['playlist_info'])

                # ---- Save separate videos collection ---- #
                # mg_yth_db[f"{channel_name}_videos"].delete_many({})
                # if 'Video_info' in extracted_data and extracted_data['Video_info']:
                #     mg_yth_db[f"{channel_name}_videos"].insert_many(extracted_data['Video_info'])
                mg_yth_db[f"{channel_name}_videos"].delete_many({})
                videos = extracted_data.get('Video_info')
                if videos:
                    if isinstance(videos, list):
                        mg_yth_db[f"{channel_name}_videos"].insert_many(videos)
                    elif isinstance(videos, dict):
                        mg_yth_db[f"{channel_name}_videos"].insert_one(videos)
                    else:
                        st.warning("⚠️ Unexpected format in Video_info.")

                # ---- Save separate comments collection ---- #
                # mg_yth_db[f"{channel_name}_comments"].delete_many({})
                # if 'Comment_info' in extracted_data and extracted_data['Comment_info']:
                #     mg_yth_db[f"{channel_name}_comments"].insert_many(extracted_data['Comment_info'])

                mg_yth_db[f"{channel_name}_comments"].delete_many({})
                comments = extracted_data.get('Comment_info')
                if comments:
                    if isinstance(comments, list):
                        mg_yth_db[f"{channel_name}_comments"].insert_many(comments)
                    elif isinstance(comments, dict):
                        mg_yth_db[f"{channel_name}_comments"].insert_one(comments)
                    else:
                        st.warning("⚠️ Unexpected format in Comment_info.")
                st.success("✅ Harvest complete and saved to MongoDB")

                # ---- Summary ---- #
                st.write(f"📺 Channel: {channel_name}")
                st.info(f"📊 Total API Calls Used: {api_counter['calls']}")
                # st.write(f"🎞️ Videos: {len(videos.get('Video_info', []))}")
                # st.write(f"💬 Comments: {len(comments.get('Comment_info', []))}")
                st.write(f"🎞️ Videos: {len(videos)}")
                st.write(f"💬 Comments: {len(comments)}")


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
                st.stop()

# ---------- Manage Harvested Youtube channels in MongoDB ----------- #
    if selected == "Mongo Manager":
        st.markdown("### Manage MongoDB ")
        col1, col2, col3 = st.columns([4, 1, 2])
        with col1:
            collection_names = mg_yth_db.list_collection_names()
            user_channels = [c for c in mg_yth_db.list_collection_names() if c.endswith('_meta')]
            selected_channel_mongodb = st.selectbox("Select a Youtube channel", user_channels)

        col4, col5, col6, _ = st.columns([1, 1, 3, 0.2])
        with col4:
            view_basic_channel_details = st.button("View Basic Detail")

        with col5:
            view_detailed_channel_details = st.button("View Complete Details")

        with col6:
            delete_selected_channel = st.button("Delete Youtube Channel")

        if view_detailed_channel_details:

            doc = mg_yth_db[selected_channel_mongodb].find_one()
            # st.subheader("Channel Info")
            # st.json(doc)

            video_collection = selected_channel_mongodb.replace("_meta", "_videos")
            if video_collection in mg_yth_db.list_collection_names():
                videos_df = pd.DataFrame(mg_yth_db[video_collection].find())
                st.subheader("Video Data")
                st.dataframe(videos_df)

            comment_collection = selected_channel_mongodb.replace("_meta", "_comments")
            if comment_collection in mg_yth_db.list_collection_names():
                comments_df = pd.DataFrame(mg_yth_db[comment_collection].find())
                st.subheader("Comment Data")
                st.dataframe(comments_df)

    # ----------------- Migrate Channels to PostgreSQL ---------------- #
    if selected == "Postgres Manager":
        st.header("🛠️PostgreSQL Manager")
        st.markdown("### 🔍 Migrate Channel to PostgreSQL")
        saved_collections = [c for c in mg_yth_db.list_collection_names() if c.endswith('_meta')]
        channel_names = sorted([c.replace('_meta', '') for c in saved_collections])
        selected_channel = st.selectbox("Select a Youtube channel", channel_names)
        migrate_to_sql = st.button("Migrate YTC")
        if migrate_to_sql:
            with init_connection() as conn:
                migrate_to_postgresql(conn, selected_channel, mg_yth_db)


    # ----------------- Analyse Youtube Channel ---------------- #
    if selected == "YT Channel Analyzer":
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

