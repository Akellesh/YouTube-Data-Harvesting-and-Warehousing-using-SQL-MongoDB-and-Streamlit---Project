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
import traceback
from itertools import cycle

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
# ---- API Keys ---- from .streamlit/secrets.toml #
YOUTUBE_API_KEYS = st.secrets["youtube"]["api_keys"]
api_key_index = 0
api_key_cycle = cycle(YOUTUBE_API_KEYS)

# ---- Session-level API quota usage tracker ---- #
if "quota_used" not in st.session_state:
    st.session_state.quota_used = 0

# ---- Quota per endpoint (approx. official) ---- #
API_COST_MAP = {"channels().list": 1, "search().list": 100, "videos().list": 1,
                "commentThreads().list": 1, "playlistItems().list": 1, "playlists().list": 1}


# ---- API Key Rotation ---- #
@st.cache_resource
def get_youtube_api(api_key):
    # key = next(api_key_cycle)
    return build("youtube", "v3", developerKey=api_key)

# --------- Track API usage and display ---------- #
def show_quota_usage():
    total_quota = 10000
    used = st.session_state.quota_used
    remaining = total_quota - used

    st.markdown("### 📊 API Quota Usage")
    st.progress(min(used / total_quota, 1.0))
    st.markdown(f"""
    - **Used:** `{used} units`  
    - **Remaining:** `{remaining} units`  
    - **Limit:** `{total_quota} units/day`  
    """)
    if st.button("🔄 Reset Quota Counter"):
        st.session_state.quota_used = 0


# --- Pre-Wrapped Helper : Quota Tracking and Error Handling ---- #
def safe_api_call(service_function, cost_key=None):
    """Wrap a YouTube API call with key rotation, quota tracking, and error handling."""
    global api_key_cycle

    for _ in range(len(YOUTUBE_API_KEYS)):
        try:
            # Get current API key and build service
            api_key = next(api_key_cycle)
            youtube_api = get_youtube_api(api_key)

            # Execute the provided service function (already created with `.list(...).execute`)
            response = service_function(youtube_api)

            # Update quota
            cost = API_COST_MAP.get(cost_key, 1)
            st.session_state.quota_used += cost

            return response

        except HttpError as e:
            error_reason = ''
            try:
                error_reason = e.error_details[0]['reason']
            except:
                pass

            if 'quotaExceeded' in str(e) or error_reason == 'quotaExceeded':
                st.warning(f"🔁 Quota exceeded for current key `{api_key}`. Trying next key...")
                continue  # Try next key
            else:
                st.error(f"❌ API Error: {e}")
                return None

    st.error("🚫 All API keys exhausted or failed.")
    return None

# ----------- MongoDB Setup -------------- #
# ---- Refers Connection with MongoDB ---- #
@st.cache_resource
def get_mongo_client():
    mongo_url = st.secrets["mongodb"]["connection_url"]
    # Creating Client Object for connection based on pymongo and refers connection link
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')  # test Connection
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
def sanitize(name):
    return re.sub(r'[.$]', '_', name)

# -------- Initialize Postgres DataBase connection. ----------- #
# -- Uses st.cache_resource to only run once, for models, connection, tools. -- #
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])

# conn = init_connection()

def is_valid_video_id(video_id):
    """Validate YouTube video ID: exactly 11 characters (letters, digits, - or _)."""
    return bool(re.fullmatch(r"[a-zA-Z0-9_-]{11}", video_id))

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
# ---- Direct Postgresql Table during Harvest ---- #
def store_postgresql_direct(conn, data):
    with st.spinner("🔧 Creating PostgreSQL Channel Info Basic table for direct Information Storing..."):
        try:
            with conn.cursor() as cur:
                # Insert into channel table
                ch_basic = data.get('Channel_info', {})
                # Debug: log the channel info before insert
                st.write("🛠️ Debug - Channel Info:", ch_basic)
                channel_id = ch_basic.get("Channel_Id")
                st.write("🛠️ Debug - Channel Info:", channel_id)
                if not channel_id:
                    st.error("❌ Channel_Id is missing. Skipping database insert.")
                    return
                cur.execute("""CREATE TABLE IF NOT EXISTS channel_table_direct (channel_id VARCHAR(50) PRIMARY KEY, 
                    channel_name VARCHAR(50), subscribers INT, channel_views INT, total_videos INT,
                    harvested_time TIMESTAMP);""")
                # Insert record
                cur.execute("""INSERT INTO channel_table_direct (channel_id, channel_name, subscribers, channel_views, 
                    total_videos, harvested_time) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (channel_id) DO UPDATE SET 
                    channel_name = EXCLUDED.channel_name, subscribers = EXCLUDED.subscribers, 
                    channel_views = EXCLUDED.channel_views, total_videos = EXCLUDED.total_videos, 
                    harvested_time = EXCLUDED.harvested_time""", (channel_id, ch_basic.get("Channel_name"),
                    ch_basic.get("Subscribers"), ch_basic.get("Views"), ch_basic.get("Total_videos"), datetime.now()))
            conn.commit()
            st.success("✅ Basic Channel Data stored in PostgreSQL")
        except Exception as e:
            conn.rollback()
            st.error(f"❌ Table creation failed: {e}")
# ---- Create PostgreSQL Tables for Data Migration ---- #
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
                                channel_name = EXCLUDED.channel_name, 
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
def get_channel_stats(_channel_id):
    # request = _youtube_api.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    response = safe_api_call(lambda yt: yt.channels().list(part='snippet,contentDetails,statistics', id=channel_id).execute(),
                             cost_key="channels.list")
    # response = request.execute()
    try:
        data = dict(Channel_Id=channel_id, Channel_name=response['items'][0]['snippet']['title'],
          Subscribers=response['items'][0]['statistics']['subscriberCount'],
          Views=response['items'][0]['statistics']['viewCount'], Total_videos=response['items'][0]['statistics']['videoCount'],
          playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
        return data
    except KeyError:
        return False

# --------------------- Function to get Playlist Info ------------------------ #
@st.cache_data
# def get_playlist_info(_youtube_api, playlist_id, channel_name, channel_id):
#     try:
#         request = youtube_api.playlists().list(part='snippet,contentDetails,status', id=playlist_id)
#         response = safe_api_call(request.execute)
#
#         if response['items']:
#             item = response['items'][0]
#             playlist = {"playlist_id": playlist_id, "playlist_name": item['snippet']['title'],
#                 "channel_name": channel_name, "channel_id": channel_id,
#                 "description": item['snippet'].get('description', ''),
#                 "item_count": item['contentDetails'].get('itemCount', 0),
#                 "privacy_status": item['status'].get('privacyStatus', 'public'),
#                 "published_at": item['snippet'].get('publishedAt'),
#                 "harvested_at": datetime.now().isoformat()
#             }
#             return playlist
#         else:
#             return None
#     except Exception as e:
#         st.error(f"❌ Playlist fetching failed: {e}")
#         return None
def get_playlist_info(_channel_id, channel_name, playlist_id):
    try:
        response = safe_api_call(lambda yt: yt.playlists().list(part='snippet,contentDetails,status',
                id=playlist_id).execute(), cost_key="playlists().list")

        if response and response.get('items'):
            item = response['items'][0]
            playlist = {"playlist_id": playlist_id, "playlist_name": item['snippet']['title'],
                        "channel_name": channel_name, "channel_id": channel_id,
                        "description": item['snippet'].get('description', ''),
                        "item_count": item['contentDetails'].get('itemCount', 0),
                        "privacy_status": item['status'].get('privacyStatus', 'public'),
                        "published_at": item['snippet'].get('publishedAt'),
                        "harvested_at": datetime.now().isoformat()}
            return playlist
        else:
            return None

    except Exception as e:
        st.error(f"❌ Playlist fetching failed: {e}")
        return None

@st.cache_data
def get_all_playlists_for_channel(_channel_id, channel_name):
    playlists = []
    next_page_token = None
    while True:
        try:
            def api_fn(yt):
                return yt.playlists().list(part='snippet,contentDetails,status', channelId=channel_id,
                    maxResults=50, pageToken=next_page_token)
            response = safe_api_call(api_fn, cost_key="playlists.list")

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
def get_video_stats(_playlist_id, max_results=50):

    videos = []
    next_page_token = None
    count = 0

    while True:
        # PlaylistItems List
        # def playlist_items_call(yt):
        #     return lambda yt: yt.playlistItems().list(part="contentDetails",playlistId=playlist_id,
        #         maxResults=min(max_results - count, 50),pageToken=next_page_token).execute()

        # playlist_response = safe_api_call(playlist_items_call, cost_key="playlistItems.list")
        playlist_response = safe_api_call(lambda yt: yt.playlistItems().list(
                            part="snippet,contentDetails", playlistId=_playlist_id, maxResults=min(max_results - count, 50),
                            pageToken=next_page_token).execute(), cost_key="playlistItems.list")

        video_ids = [item['contentDetails']['videoId'] for item in playlist_response['items']]
        count += len(video_ids)

        if not video_ids:
            break

        # Video List
        # def videos_call(yt):
        #     return lambda yt: yt.videos().list(part="snippet,contentDetails,statistics", id=",".join(video_ids)).execute()

        video_response = safe_api_call(lambda yt: yt.videos().list(part="snippet,contentDetails,statistics",
                                        id=",".join(video_ids)).execute(), cost_key="videos.list")

        for item in video_response.get('items', []):
            video_data = {
                "video_id": item['id'], "video_title": item['snippet']['title'],
                "published_at": item['snippet']['publishedAt'], "view_count": item['statistics'].get('viewCount', 0),
                "like_count": item['statistics'].get('likeCount', 0), "comment_count": item['statistics'].get('commentCount', 0),
                "duration": item['contentDetails']['duration'], "description": item['snippet'].get('description', '')
            }
            videos.append(video_data)

        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token:
            break

    return videos


# --- Fetch all playlists from channel ---
@st.cache_data
def get_all_playlists(_channel_id):
    playlists = []
    def initial_request():
        return lambda yt: yt.playlists().list(part="snippet,contentDetails", channelId=channel_id, maxResults=50).execute()
    request = initial_request()

    while request:
        response = safe_api_call(request, cost_key="playlists.list")
        if not response:
            break

        for item in response.get('items', []):
            playlists.append({"playlist_id": item["id"], "playlist_name": item["snippet"]["title"],
                              "description": item["snippet"].get("description", ""),
                              "item_count": item["contentDetails"].get("itemCount", 0),
                              "privacy_status": item.get("status", {}).get("privacyStatus", "public"),
                              "published_at": item["snippet"]["publishedAt"]})

        # request = lambda yt: yt.playlists().list_next(request, response)
    return playlists

# --- Fetch all videos from all playlists ---
@st.cache_data
def get_all_playlist_videos(_channel_id):
    all_videos = []
    all_playlists = get_all_playlists(channel_id)
    for pl in all_playlists:
        pl_id = pl["playlist_id"]
        videos = get_video_stats(pl_id)
        for v in videos:
            v["playlist_id"] = pl_id
        all_videos.extend(videos)
    return all_videos, all_playlists

# --------------------- Function to get Comments Stats --------------------- #
@st.cache_data
def update_comment_stats(_channel_name, video_ids, max_comments_per_video=50):
    all_comments = []

    for video_id in video_ids:
        try:
            next_page_token = None
            count = 0

            while True:
                def fetch_comments(yt):
                    return lambda yt: yt.commentThreads().list(part="snippet", videoId=video_id,
                        maxResults=min(max_comments_per_video - count, 50), pageToken=next_page_token,
                        textFormat="plainText").execute()
                comment_response = safe_api_call(fetch_comments, cost_key="commentThreads.list")
                if not comment_response:
                    break

                for item in comment_response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comment_data = {"video_id": video_id, "comment_id": item['id'],
                                    "author": comment.get('authorDisplayName'), "text": comment.get('textDisplay'),
                                    "like_count": comment.get('likeCount', 0), "published_at": comment.get('publishedAt')}
                    all_comments.append(comment_data)

                count += len(comment_response['items'])

                next_page_token = comment_response.get('nextPageToken')
                if not next_page_token:
                    break

        except Exception as e:
            print(f"Failed to fetch comments for video {video_id}: {e}")
            continue

    return all_comments

# ----------- Function to get all the channel video details -------------- #
@st.cache_data(ttl=3600, show_spinner=False)
def extract_channel_all_details(_channel_id, use_uploaded_playlist_only=True):
    # ---- 1. Getting Channel Statistics ---- #
    progress_bar_extract = st.progress(0.0, text="📤 Starting Youtube channel Harvesting...")
    with st.spinner('Fetching channel statistics...'):
        # channel_statistics = safe_api_call(lambda yt: get_channel_stats(yt, channel_id), "channels().list")
        channel_statistics = get_channel_stats(channel_id)
        if not channel_statistics:
            st.warning("⚠️ Channel statistics not available.")
            return None
        # uploads_playlist_id = channel_statistics.get('playlist_id') # This is the uploads playlist
        channel_name = channel_statistics.get("Channel_name", "Unknown")
        if not channel_name:
            st.error("❌ Channel name missing. Cannot continue.")
            return None
    progress_bar_extract.progress(0.05, "✅ Channel stats fetched...")

    # ---- 2. Fetch Playlist and Videos ---- #
    # all_playlists = []
    # all_videos = []
    #
    # if use_uploaded_playlist_only:
    #     with st.spinner("📂 Fetching videos from uploads playlist..."):
    #         progress_bar_extract.progress(0.10, "Fetching all uploaded videos...")
    #         uploads_playlist_id = channel_statistics.get("playlist_id")
    #         all_playlists = [{"playlist_id": uploads_playlist_id, "playlist_name": "Uploads",
    #                           "description": "Default upload playlist", "item_count": channel_statistics.get("Total_videos", 0),
    #                           "privacy_status": "public", "published_at": channel_statistics.get("published_at", "")}]
    #
    #         # videos = safe_api_call(lambda yt: get_video_stats(yt, uploads_playlist_id), "playlistItems.list")
    #         videos = get_video_stats(uploads_playlist_id)
    #         if videos:
    #             all_videos.extend(videos)
    #     progress_bar_extract.progress(0.40, "✅ All Uploaded videos fetched.")
    #
    # else:
    #     with st.spinner("📂 Fetching all playlists and their videos..."):
    #         progress_bar_extract.progress(0.10, "Fetching all playlists and their videos...")
    #         # playlists = safe_api_call(lambda yt: get_all_playlists_for_channel(yt, channel_name, channel_id), "playlists.list")
    #         playlists = get_all_playlists_for_channel(channel_id, channel_name)
    #         if not playlists:
    #             st.warning("⚠️ No playlists found.")
    #             return None
    #
    #         all_playlists.extend(playlists)
    #         total_playlists = len(playlists)
    #
    #         for idx, playlist in enumerate(playlists):
    #             pid = playlist.get("playlist_id")
    #
    #             # videos = safe_api_call(lambda yt: get_video_stats(yt, pid), "playlistItems.list")
    #             videos = get_video_stats(pid)
    #             if videos:
    #                 all_videos.extend(videos)
    #             pct = 0.11 + (0.20 * ((idx + 1) / total_playlists))
    #             progress_bar_extract.progress(pct, f"Fetched videos from playlist {idx + 1}/{total_playlists}")
    #     progress_bar_extract.progress(0.40, "✅ All playlists and their videos fetched.")
    with st.spinner('📂 Fetching all playlists and videos...'):
        result = get_all_playlist_videos(_channel_id)
        # result = safe_api_call(lambda yt: get_all_playlist_videos(yt, channel_id), cost_key="playlists().list")
        if not result:
            return None
        all_videos, all_playlists = result
        video_ids = [v.get("video_id") for v in all_videos if v.get("video_id")]

    with st.spinner('💬 Fetching all comments for each video...'):
        all_comments = []
        for vid in video_ids:
            comment_data = update_comment_stats(_channel_id, video_ids)
            # comment_data = safe_api_call(lambda yt: update_comment_stats(yt, vid), cost_key="commentThreads().list")
            if comment_data:
                for comment in comment_data:
                    all_comments.append(comment)

    # ----3. Fetch comments for each Video ------ #
    # video_ids = [v.get("video_id") for v in all_videos if v.get("video_id") and v.get("video_id") != "-" and v.get("video_id") != "_"]
    # # video_ids = []
    # invalid_video_ids = []
    #
    # for v in all_videos:
    #     vid = v.get("video_id", "")
    #     if is_valid_video_id(vid):
    #         video_ids.append(vid)
    #     else:
    #         invalid_video_ids.append(vid)
    #
    # if invalid_video_ids:
    #     st.warning(f"⚠️ {len(invalid_video_ids)} invalid video IDs were skipped.")
    #     mg_yth_db["invalid_video_ids"].insert_one({"channel_id": channel_id, "channel_name": channel_name,
    #                                                "invalid_ids": invalid_video_ids, "timestamp": datetime.now().isoformat()})
    # st.write("🧪 Video IDs for comment extraction:", video_ids)
    # st.write("🎞️ Total videos:", len(all_videos))
    # comment_statistics = []
    #
    # if video_ids:
    #     with st.spinner("💬 Fetching comments for each video..."):
    #         for i, vid in enumerate(video_ids):
    #             # comments = safe_api_call(lambda yt: update_comment_stats(yt, vid, channel_name, max_comments_per_video=50), "commentThreads.list")
    #             comments = update_comment_stats(channel_name, vid, max_comments_per_video=50)
    #             if comments:
    #                 comment_statistics.extend(comments)
    #             progress = 0.40 + (0.30 * ((i + 1) / len(video_ids)))
    #             progress_bar_extract.progress(progress, f"Fetched comments for video {i + 1}/{len(video_ids)}")
    #     progress_bar_extract.progress(0.90, "✅ Comments for all videos fetched.")
    # else:
    #     st.warning("⚠️ No videos found.")

    # ----- Pack into a single dictionary ----- #
    channel_data = {
        'Channel_info': channel_statistics, 'playlist_info': all_playlists , 'Video_info': all_videos,
        'Comment_info': all_comments,
        'Meta': {
            'Total Videos': len(all_videos), 'Total Comments': len(all_comments)},
        'last_updated': datetime.now().isoformat() # Added Timestamps for Tracking Updates
    }
    # ------ Log success to MongoDB -------#
    mg_yth_db["audit_logs"].insert_one({
        "channel_id": channel_id,
        "channel_name": channel_name,
        "status": "success",
        "video_count": len(all_videos),
        "comment_count": len(all_comments),
        "timestamp": datetime.now().isoformat()
    })

    progress_bar_extract.progress(1.00, "✅ Channel Data created and log stored successfully in MonngoDB.")
    return channel_data

# ---------------------------------- Streamlit UI -------------------------------------- #
# -------------------------------------------------------------------------------------- #
# ------------------------ Streamlit Sidebar Menu and YDH_DB --------------------------- #
st.set_page_config(page_title="Youtube_Data_Harvesting", layout="wide")

with st.sidebar:
    selected = option_menu(menu_title="Youtube_Data_Harvesting Menu", options=["Home","---", "YDH_DB", "---","Contact"],
        icons=["house", "upload", "envelope"], # "gear",
        menu_icon="cast", default_index=0,
        orientation="vertical",
        styles={"container": {"padding": "0!important", "background-color": "#AFBFAB"},
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

# Set default value for api_status on first run
if "api_status" not in st.session_state:
    st.session_state.api_status = "🕒 Waiting for Test YouTube API Connection response..."

# Initialize session state variable on first run
if "tested_channel_name" not in st.session_state:
    st.session_state.tested_channel_name = "No Channel to Display"
# Initialize quota tracking on first run
if "quota_used" not in st.session_state:
    st.session_state.quota_used = 0
def update_quota(units_used, endpoint=""):
    st.session_state.quota_used += units_used
    # print(f"🧮 Used {units_used} units for {endpoint}. Total quota used: {st.session_state.quota_used}")

# ------------- YouTube Data Harvessting DataBase Section -------------- #
if selected == "YDH_DB":
    selected = option_menu(menu_title="Youtube_Data_Harvesting_DataBase Menu",
        options=["YT Channel Extractor", "Mongo Manager", "Postgres Manager", "YT Channel Analyzer"],
        icons=["database", "database-add", "gear"], menu_icon="database-gear",
        default_index=0, orientation="horizontal")

# ----------------- Search and Extract Youtube Channel ---------------- #
    if selected == "YT Channel Extractor":
        st.markdown("## API Key Check")
        # api_status = "Waiting for Test YouTube API Connection response..."
        col1, col2 = st.columns([1,3])
        with col1:
            if st.button("Test YouTube API Connection Status"):
                try:
                    # Try a basic call using Google Developers Channel ID
                    test_channel_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw"
                    # response = youtube_api.channels().list(part="snippet", id=test_channel_id).execute()
                    response = safe_api_call(lambda yt: yt.channels().list(part="snippet", id=test_channel_id).execute(),
                        cost_key="channels().list")

                    if response and response.get('items'):
                        channel_info = response['items'][0]['snippet']
                        # test_channel_name = st.write(f"**Channel Name:** {channel_info['title']}")
                        test_channel_name = channel_info.get('title')
                        # st.success("✅ YouTube API key is valid!")
                        st.session_state.api_status = "✅ YouTube API key is valid, API connection is established!, Good to Process..."
                        st.session_state.tested_channel_name = test_channel_name  # ✅ Store tested channel name
                    else:
                        st.session_state.api_status = "⚠️ API call returned no items."
                        st.session_state.tested_channel_name = "No Channel to Display"
                except Exception as e:
                    st.session_state.api_status = st.error(f"❌ YouTube API test failed: {e}")
                    st.session_state.tested_channel_name = "No Channel to Display"  # Clear it if failed
        with col2:
            if st.button("Reset YouTube API Connection Status"):
                st.session_state.api_status = "🕒 Waiting for Test YouTube API Connection response..."
                st.session_state.tested_channel_name = "No Channel to Display"
        st.markdown(st.session_state.api_status)
        # Show tested channel name if available
        if "tested_channel_name" in st.session_state and st.session_state.tested_channel_name:
            st.markdown(f"**Tested Channel Name:** `{st.session_state.tested_channel_name}`")

        # ---- API Quota Usage ---- #
        show_quota_usage()

        col1, col2, col3 = st.columns([4, 1, 2])
        with col1:
            st.subheader("🔍 Search and Extract Youtube Channel")
            channel_id = st.text_input("Enter Channel Id:")

        col4, col5, col6, col7 = st.columns([4, 4, 3, 3])
        with col4:
            Search = st.button("Search Youtube Channel")

        with col5:
            Extract = st.button("Extract Youtube Channel")
            store_pgsql = st.checkbox("📦 Store extracted data in PostgreSQL")
            export_json = st.checkbox("🗃️ Export data as JSON")

        if Search and channel_id:
            # view_data = safe_api_call(get_channel_stats,youtube_api, channel_id)
            view_data = get_channel_stats(channel_id)
            if view_data:
                view_data_df = pd.DataFrame([view_data]).T
                st.success("Found Youtube Channel")
                st.dataframe(view_data_df[1:-1].style.background_gradient())
                # st.table(view_data_df)
            else:
                st.error("Youtube Channel not found. Check the ID and try again.", icon="🚨")
        elif Search and not channel_id:
            st.warning("⚠️ Please enter a Channel ID to search.")

        if Extract and channel_id:
            # extracted_data = safe_api_call(extract_channel_all_details,youtube_api, channel_id)
            # extracted_data = extract_channel_all_details(youtube_api, channel_id)
            extracted_data = extract_channel_all_details(channel_id)

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
                # Debug: log the channel info before insert
                st.write("🛠️ Debug - Comments:", comments)
                if comments:
                    if isinstance(comments, list):
                        mg_yth_db[f"{channel_name}_comments"].insert_many(comments)
                    elif isinstance(comments, dict):
                        # mg_yth_db[f"{channel_name}_comments"].insert_one(comments)
                        mg_yth_db[f"{channel_name}_comments"].insert_many(comments)
                    else:
                        st.warning("⚠️ Unexpected format in Comment_info.")
                st.success("✅ Harvest complete and saved to MongoDB")

                # ---- Summary ---- #
                st.write(f"📺 Channel: {channel_name}")
                # st.info(f"📊 Total API Calls Used: {api_counter['calls']}")
                st.write(f"🎞️ Videos: {len(extracted_data.get('Video_info', []))}")
                st.write(f"💬 Comments: {len(extracted_data.get('Comment_info', []))}")
                # st.write(f"🎞️ Videos: {len(videos)}")
                # st.write(f"💬 Comments: {len(comments)}")

                # --- Direct PostgreSQL storage - Basic Channel Info Option ---- #
                if store_pgsql:
                    try:
                        with init_connection() as conn:
                            store_postgresql_direct(conn, extracted_data)

                        # st.success("✅ Basic Channel Data stored in PostgreSQL")
                    except Exception as e:
                        st.error(f"❌ PostgreSQL storage failed: {e}")

                # ----  Plot Preview Option ---- #
                st.markdown("📋 Plot Charts for Top 10 Videos by Views ")
                display_plot = st.button("Display Plots")
                if display_plot:
                    video_df = pd.DataFrame(extracted_data.get('Video_info', []))
                    if not video_df.empty and 'view_count' in video_df.columns:
                        video_df['view_count'] = pd.to_numeric(video_df.get('view_count', 0), errors='coerce')
                        top_videos = video_df.sort_values(by='view_count', ascending=False).head(10)
                        fig = px.bar(top_videos, x='video_title', y='view_count', title='Top 10 Videos by Views')
                        fig.update_layout(xaxis_tickangle=45)
                        st.plotly_chart(fig)
                    else:
                        st.warning("⚠️ No valid video data available.")

                # ---- Channel Preview Option - Json and Dataframe ---- #
                st.markdown("📋 Channel Info")
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1:
                    display_json = st.button("Display Extracted Json Channel Info for Reference")
                with col3:
                    display_dataframe = st.button("Display Extracted Channel Info as DataFrame for Reference")
                if display_json:
                    st.json(extracted_data)  # or use st.dataframe if tabular
                if display_dataframe:
                    st.dataframe(extracted_data)

                # ---- Download JSON File Option ---- #
                def convert_bson(obj):
                    if isinstance(obj, bson.ObjectId):
                        return str(obj)
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    raise TypeError(f"Type {type(obj)} not serializable")

                if export_json:
                    try:
                        json_data = json.dumps(extracted_data, indent=2, default=str)
                        st.download_button(label="📥 Download Extracted Data (JSON)", data=json_data,
                                           file_name=f"{channel_id}_youtube_data.json", mime="application/json")
                    except Exception as e:
                        st.error(f"❌ JSON export failed: {e}")
                # st.download_button("Download JSON", json.dumps(extracted_data, indent=2, default=convert_bson), f"{channel_name}_data.json")

            else:
                st.error("Youtube Channel not found. Check the ID and try again. or failed to retrieve.", icon="🚨")
                st.stop()
        elif Extract and not channel_id:
            st.warning("⚠️ Please enter a Channel ID to extract.")

# ----------- Manage Harvested YouTube channels in MongoDB ------------ #
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

# ----------------- Migrate Channels to PostgreSQL ------------------- #
    if selected == "Postgres Manager":
        st.header("🛠️ PostgreSQL Manager")
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
        st.markdown("### YouTube Channel Analyzer")

        # cur = conn.cursor()

        # Refactored FAQ Display Block with Reusability, Decorators, and Quota Tracking

        # Sample function for quota tracking decorator
        def track_quota(units=1, endpoint=""):
            def decorator(func):
                def wrapper(*args, **kwargs):
                    if "quota_used" not in st.session_state:
                        st.session_state.quota_used = 0
                    st.session_state.quota_used += units
                    return func(*args, **kwargs)

                return wrapper

            return decorator

        # FAQ Handler
        # def render_faq():
        #
        #     st.markdown('__<p style="text-align:left; font-size: 30px; color: #FAA026">Top 10 FAQs</P>__',
        #                 unsafe_allow_html=True)
        #
        #     # Helper function to display a query result
        #     @track_quota(units=0, endpoint="SQL Query")
        #     def show_query_result(query, columns, index=None, width=1000):
        #         cur.execute(query)
        #         result = cur.fetchall()
        #         df = pd.DataFrame(result, columns=columns)
        #         if index:
        #             df = df.set_index(index)
        #         st.dataframe(df, width=width)
        #         return df
        #
        #     with st.expander("Q1. What are the names of all the videos and their corresponding channels?"):
        #         st.write("Here you can find a comprehensive list of channels and the associated videos within them:")
        #         # query = """
        #                 SELECT channel_table.channel_name, video_table.title
        #                 FROM video_table
        #                          JOIN channel_table ON video_table.channel_id = channel_table.channel_id
        #                 ORDER BY channel_table.channel_name \
        #                 """
        #         # show_query_result(query, ['Channel Name', 'Video Title'], index='Channel Name')
        #
        #     with st.expander("Q2. Which channels have the most number of videos and how many videos do they have?"):
        #         query = """
        #                 SELECT channel_name, total_videos AS Videos
        #                 FROM channel_table
        #                 ORDER BY total_videos DESC LIMIT 3 \
        #                 """
        #         df = show_query_result(query, ['Channel Name', 'Total Video'])
        #         channel_name, total_video = df.iloc[0]
        #         st.write(
        #             f"'{channel_name}' channel has the most videos with a total count of {total_video}. Below are the top 3 channels in the list")
        #
        #     with st.expander("Q3. What are the top 10 most viewed videos and their respective channels?"):
        #         query = """
        #                 SELECT channel_table.channel_name, video_table.title, video_table.view_count
        #                 FROM video_table
        #                          JOIN channel_table ON video_table.channel_id = channel_table.channel_id
        #                 ORDER BY video_table.view_count DESC LIMIT 10 \
        #                 """
        #         df = show_query_result(query, ['Channel Name', 'Video Title', 'View Count'], index='Channel Name')
        #         st.write(
        #             f"{df.iloc[0, 0]} channel is on the top of the list for the video '{df.iloc[0, 1]}' with {df.iloc[0, 2]} views.")
        #
        #     with st.expander(
        #             "Q4. How many comments were made on each video and what are their corresponding video names?"):
        #         query = "SELECT title, comment_count FROM video_table ORDER BY comment_count DESC"
        #         df = show_query_result(query, ['Video Name', 'Total Comment'], index='Video Name', width=700)
        #         st.write(f"{df.index[0]} received {df.iloc[0, 0]} comments.")
        #
        #     with st.expander(
        #             "Q5. Which videos have the highest number of likes and what are their corresponding channel names?"):
        #         query = """
        #                 SELECT video_table.like_count, video_table.title, channel_table.channel_name
        #                 FROM video_table
        #                          JOIN channel_table ON video_table.channel_id = channel_table.channel_id
        #                 ORDER BY video_table.like_count DESC LIMIT 10 \
        #                 """
        #         df = show_query_result(query, ['Like Count', 'Video Name', 'Channel Name'], index='Like Count')
        #         st.write("Below are the top 10 liked videos and their channel name:")
        #
        #     with st.expander("Q6. What is the total number of likes and dislikes for each video?"):
        #         query = "SELECT title, like_count, dislike_count FROM video_table ORDER BY like_count DESC"
        #         df = show_query_result(query, ['Video Name', 'Like Count', 'Dislike Count'], index='Video Name')
        #         st.write("Note: YouTube no longer shows public dislike counts as per their 2021 update.")
        #
        #     with st.expander("Q7. What is the total and average number of views for each channel?"):
        #         query = """
        #                 SELECT channel_table.channel_name, \
        #                        SUM(video_table.view_count), \
        #                        ROUND(AVG(video_table.view_count), 2)
        #                 FROM video_table
        #                          JOIN channel_table ON video_table.channel_id = channel_table.channel_id
        #                 GROUP BY channel_table.channel_name
        #                 ORDER BY SUM(video_table.view_count) DESC \
        #                 """
        #         show_query_result(query, ['Channel Name', 'View Count', 'Avg View/Video'], index='Channel Name')
        #
        #     with st.expander("Q8. Which channels published videos in the year 2022?"):
        #         query = """
        #                 SELECT channel_table.channel_name, COUNT(video_table.title), SUM(video_table.view_count)
        #                 FROM video_table
        #                          JOIN channel_table ON video_table.channel_id = channel_table.channel_id
        #                 WHERE EXTRACT(YEAR FROM video_table.published_date) = 2022
        #                 GROUP BY channel_table.channel_name
        #                 ORDER BY COUNT(video_table.title) DESC \
        #                 """
        #         show_query_result(query, ['Channel Name', 'Total Videos', 'Total Views'], index='Channel Name')
        #
        #     with st.expander("Q9. What is the average duration of all videos in each channel?"):
        #         query = """
        #                 SELECT channel_name,
        #                        EXTRACT(MINUTE FROM duration) || ' mins ' || ROUND(EXTRACT(SECOND FROM duration)) || \
        #                        ' secs' AS avg_duration
        #                 FROM (SELECT channel_table.channel_name, AVG(video_table.duration) AS duration \
        #                       FROM video_table \
        #                                JOIN channel_table ON video_table.channel_id = channel_table.channel_id \
        #                       GROUP BY channel_table.channel_name) AS subq \
        #                 """
        #         show_query_result(query, ['Channel Name', 'Average Duration'], index='Channel Name')
        #
        #     with st.expander(
        #             "Q10. Which videos have the highest number of comments and what are their corresponding channel names?"):
        #         query = """SELECT video_table.comment_count, video_table.title, channel_table.channel_name
        #                 FROM video_table JOIN channel_table ON video_table.channel_id = channel_table.channel_id
        #                 ORDER BY comment_count DESC LIMIT 10"""
        #         df = show_query_result(query, ['Total Comment', 'Video Name', 'Channel Name'], index='Total Comment')
        #         st.write(
        #             f"'{df.iloc[0, 1]}' by {df.iloc[0, 2]} received {df.iloc[0, 0]} comments and holds the top position.")
        #
        #     st.write("Note: The above insights are based on the scraped dataset and may not represent real-time data.")
        #
        # st.markdown('__<p style="text-align:left; font-size: 30px; color: #FAA026">Top 10 FAQs</P>__',
        #                 unsafe_allow_html=True)
        #
        # display_faq = st.button("Display FAQ")
        # if display_faq:
        #     with init_connection() as conn:
        #         cur = conn.cursor()
        #         render_faq()


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
