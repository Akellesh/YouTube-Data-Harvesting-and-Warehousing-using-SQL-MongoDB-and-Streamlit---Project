# ----- Import Basic Packages ----- #
import pandas as pd
import json
import re
from datetime import datetime
import isodate
from textblob import TextBlob
from langdetect import detect
import streamlit as st
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

# ---------- Complete YouTube API Management --------------- #
# ---- API Keys ---- from .streamlit/secrets.toml #
YOUTUBE_API_KEYS_P1 = st.secrets["youtube1"]["api_keys"]  # Project 1 — 10,000 quota
YOUTUBE_API_KEYS_P2 = st.secrets["youtube2"]["api_keys"]  # Project 2 — 10,000 quota
YOUTUBE_API_KEYS = [
    key for pair in zip(YOUTUBE_API_KEYS_P1, YOUTUBE_API_KEYS_P2)
    for key in pair
]
# Cycle through the interleaved keys
api_key_cycle = cycle(YOUTUBE_API_KEYS)

# ---- Session-level API quota usage tracker ---- #
if "quota_used" not in st.session_state:
    st.session_state.quota_used = 0

# ---- Quota per endpoint (approx. official) ---- #
API_COST_MAP = {
    "channels().list": 1,
    "search().list": 100,
    "videos().list": 1,
    "commentThreads().list": 1,
    "playlistItems().list": 1,
    "playlists().list": 1,
}

# ---- Build API Key Service with Given Key ---- #
@st.cache_resource
def get_youtube_api(api_key):
    return build("youtube", "v3", developerKey=api_key)

# ---- Get API Key and build service - Rotation Helper ---- #
def get_next_youtube_service():
    """Cycles through API keys and returns (key, service)."""
    try:
        api_key = next(api_key_cycle)
        youtube_service = get_youtube_api(api_key)
        return api_key, youtube_service
    except Exception as e:
        st.error(f"❌ Failed to create YouTube service: {e}")
        return None, None

# ---- Quota Increment Helper ---- #
def increment_quota(cost_key):
    cost = API_COST_MAP.get(cost_key, 1)
    st.session_state.quota_used += cost

# --------- Track YouTube API usage and display ---------- #
def show_quota_usage():
    total_quota = 20000
    per_project = 10000
    used = st.session_state.get("quota_used", 0)
    remaining = total_quota - used
    # ── Metric cards ──
    m1, m2, m3 = st.columns(3)
    m1.metric("🔢 Total Quota", f"{total_quota:,} units")
    m2.metric("✅ Total Used", f"{used:,} units")
    m3.metric("🟢 Remaining", f"{remaining:,} units",
              delta=f"-{used} used", delta_color="inverse")
    n1, n2 = st.columns(2)
    n1.metric("📁 Project 1 Est.", f"{min(used, per_project)} / {per_project}")
    n2.metric("📁 Project 2 Est.", f"{max(0, used - per_project):,} / {per_project:,}")

    # ── Progress bar with colour warning ──
    pct = min(used / total_quota, 1.0)
    st.progress(pct)
    if used >= 10000:
        st.warning("⚠️ Project 1 quota likely exhausted. Running on Project 2 keys.")
    elif used >= 20000:
        st.error("🚫 All quota exhausted for today. Resets at midnight Pacific Time.")
    elif pct >= 0.9:
        st.error("🚫 Over 90% quota used. Resets at midnight Pacific Time.")
    elif pct >= 0.7:
        st.warning("⚠️ Over 70% quota used. Only few extraction possible, Consider pausing extraction for today.")
    elif used > 0:
        st.success("✅ Quota healthy.")
    else:
        st.info("🕒 No quota used yet today.")

    # ── Action button - Reset ── #
    if st.button("🔄 Reset Quota Counter", use_container_width=True):
        st.session_state.quota_used = 0
        st.rerun()

# ---- Pre-Wrapped Helper: Quota Tracking and Error Handling ---- #
def safe_api_call(service_function, cost_key=None):
    """Wrap a YouTube API call with key rotation, quota tracking, and error handling.
    service_function must accept a youtube service object and return the response:
        lambda yt: yt.channels().list(...).execute()
    """
    for _ in range(len(YOUTUBE_API_KEYS)):
        api_key, youtube_api = get_next_youtube_service()
        if not youtube_api:
            continue
        try:
            response = service_function(youtube_api)
            increment_quota(cost_key)
            return response
        except HttpError as e:
            error_reason = ""
            try:
                error_reason = e.error_details[0]["reason"]
            except Exception:
                pass

            if "quotaExceeded" in str(e) or error_reason == "quotaExceeded":
                st.warning(f"🔁 Quota exceeded for key `{api_key}`. Trying next key...")
                continue
            else:
                # 403 on commentThreads is expected — don't alarm the user
                if e.resp.status not in [403, 400]:
                    st.error(f"❌ API Error: {e}")
                return None

    st.error("🚫 All API keys exhausted or failed.")
    return None

# ----------- MongoDB Setup -------------- #
@st.cache_resource
def get_mongo_client():
    mongo_url = st.secrets["mongodb"]["connection_url"]
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        return client
    except errors.ServerSelectionTimeoutError as e:
        st.error(f"❌ Failed to connect to MongoDB: {e}")
        st.stop()

client = get_mongo_client()
mg_yth_db = client["YouTubeHarvest"]
collection_list = mg_yth_db.list_collection_names()

# ---- Helper: sanitize collection names ---- #
# def sanitize(name):
#     return re.sub(r"[.$]", "_", name)

# -------- Initialize Postgres connection --------- #
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


# ---- Video ID validator ---- #
def is_valid_video_id(video_id):
    """Validate YouTube video ID: exactly 11 characters (letters, digits, - or _)."""
    return bool(re.fullmatch(r"[a-zA-Z0-9_-]{11}", video_id))


# ---- ISO 8601 duration → HH:MM:SS ---- #
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

# ============================================================
# PostgreSQL - DB Operations
# ============================================================
def store_postgresql_direct(conn, data):
    """Store basic channel info directly to PostgreSQL right after harvest."""
    with st.spinner("🔧 Storing basic channel info in PostgreSQL..."):
        try:
            ch_basic = data.get("Channel_info", {})
            channel_id = ch_basic.get("Channel_Id")
            if not channel_id:
                st.error("❌ Channel_Id is missing. Skipping database insert.")
                return
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS channel_table_direct (
                        channel_id    VARCHAR(50) PRIMARY KEY,
                        channel_name  VARCHAR(255),
                        subscribers   BIGINT,
                        channel_views BIGINT,
                        total_videos  BIGINT,
                        harvested_time TIMESTAMP
                    );
                """)
                cur.execute("""
                    INSERT INTO channel_table_direct
                        (channel_id, channel_name, subscribers, channel_views, total_videos, harvested_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (channel_id) DO UPDATE SET
                        channel_name   = EXCLUDED.channel_name,
                        subscribers    = EXCLUDED.subscribers,
                        channel_views  = EXCLUDED.channel_views,
                        total_videos   = EXCLUDED.total_videos,
                        harvested_time = EXCLUDED.harvested_time;
                """, (
                    channel_id,
                    ch_basic.get("Channel_name"),
                    int(ch_basic.get("Subscribers", 0)),
                    int(ch_basic.get("Views", 0)),
                    int(ch_basic.get("Total_videos", 0)),
                    datetime.now(),
                ))
            conn.commit()
            st.success("✅ Basic Channel Data stored in PostgreSQL")
        except Exception as e:
            conn.rollback()
            st.error(f"❌ Direct PostgreSQL store failed: {e}")


def create_postgresql_tables(conn):
    """Create all four normalised tables if they do not already exist."""
    with st.spinner("🔧 Creating PostgreSQL tables..."):
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS channel_table (
                        channel_id    VARCHAR(50) PRIMARY KEY,
                        channel_name  VARCHAR(255),
                        subscribers   BIGINT,
                        channel_views BIGINT,
                        total_videos  BIGINT,
                        harvested_time TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS channel_playlist (
                        playlist_id    VARCHAR(255) PRIMARY KEY,
                        playlist_name  VARCHAR(255),
                        channel_name   VARCHAR(255),
                        channel_id     VARCHAR(255),
                        description    TEXT,
                        item_count     INT,
                        privacy_status VARCHAR(50),
                        published_at   TIMESTAMP,
                        harvested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS channel_videos (
                        video_id          VARCHAR(50) PRIMARY KEY,
                        playlist_id       VARCHAR(50),
                        video_name        VARCHAR(500),
                        video_description TEXT,
                        published_date    TIMESTAMP,
                        category_id       INT,
                        duration          TIME,
                        video_quality     VARCHAR(20),
                        licensed          VARCHAR(10),
                        view_count        BIGINT,
                        like_count        INT,
                        dislike_count     INT,
                        favorite_count    INT,
                        comments_count    INT,
                        thumbnail         VARCHAR(500),
                        caption_status    VARCHAR(150)
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS channel_comments (
                        comment_id      VARCHAR(50) PRIMARY KEY,
                        video_id        VARCHAR(50),
                        channel_name    VARCHAR(255),
                        comment_text    TEXT,
                        comment_date    TIMESTAMP,
                        comment_author  VARCHAR(255),
                        comment_like    INT     DEFAULT 0,
                        reply_count     INT     DEFAULT 0,
                        is_pinned       BOOLEAN DEFAULT FALSE,
                        is_hearted      BOOLEAN DEFAULT FALSE,
                        language        VARCHAR(10),
                        sentiment_score FLOAT,
                        harvested_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            st.error(f"❌ Table creation failed: {e}")

def migrate_to_postgresql(conn, selected_channel, mg_yth_db):
    """Migrate a harvested channel from MongoDB to PostgreSQL."""
    try:
        progress_bar = st.progress(0, text="📤 Starting migration...")
        create_postgresql_tables(conn)

        # ------ Fetch from MongoDB ------ #
        meta = mg_yth_db[f"{selected_channel}_meta"].find_one()
        if not meta:
            st.error(f"⚠️ No meta data found for: {selected_channel}")
            return

        playlists = list(mg_yth_db[f"{selected_channel}_playlist"].find())
        videos    = list(mg_yth_db[f"{selected_channel}_videos"].find())
        comments  = list(mg_yth_db[f"{selected_channel}_comments"].find())

        # Remove MongoDB ObjectIds
        def strip_ids(docs):
            for d in docs:
                if isinstance(d, dict):
                    d.pop("_id", None)
            return docs

        meta      = strip_ids([meta])[0]
        playlists = strip_ids(playlists)
        videos    = strip_ids(videos)
        comments  = strip_ids(comments)

        # ---- Validate required fields before any DB write ---- #
        channel_id_val   = meta.get("Channel_Id")
        channel_name_val = meta.get("Channel_name")
        subscribers_val  = meta.get("Subscribers")
        if not all([channel_id_val, channel_name_val, subscribers_val is not None]):
            st.error("❌ Missing essential metadata fields (Channel_Id / Channel_name / Subscribers). Aborting.")
            return

        # ---- Insert channel row ---- #
        progress_bar.progress(0.2, "📦 Inserting channel metadata...")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO channel_table
                    (channel_id, channel_name, subscribers, channel_views, total_videos, harvested_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_id) DO UPDATE SET
                    channel_name   = EXCLUDED.channel_name,
                    subscribers    = EXCLUDED.subscribers,
                    channel_views  = EXCLUDED.channel_views,
                    total_videos   = EXCLUDED.total_videos,
                    harvested_time = EXCLUDED.harvested_time;
            """, (
                channel_id_val,
                channel_name_val,
                int(subscribers_val),
                int(meta.get("Views", 0)),
                int(meta.get("Total_videos", 0)),
                datetime.now(),
            ))

        # ---- Insert playlists ---- #
        progress_bar.progress(0.3, "🎞 Inserting playlist records...")
        if playlists:
            playlist_rows = [
                (
                    p.get("playlist_id"),
                    p.get("playlist_name") or p.get("playlist_title"),
                    selected_channel,
                    p.get("channel_id") or channel_id_val,
                    p.get("description", ""),
                    int(p.get("item_count", 0)),
                    p.get("privacy_status", ""),
                    p.get("published_at"),
                    datetime.now(),
                )
                for p in playlists
            ]
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO channel_playlist
                        (playlist_id, playlist_name, channel_name, channel_id, description,
                         item_count, privacy_status, published_at, harvested_at)
                    VALUES %s ON CONFLICT (playlist_id) DO NOTHING;
                """, playlist_rows)

        # ---- Insert videos ---- #
        progress_bar.progress(0.45, "🎞 Inserting video records...")
        if videos:
            video_rows = [
                (
                    v.get("video_id"),
                    v.get("playlist_id"),
                    v.get("video_title"),
                    v.get("description", ""),
                    v.get("published_at"),
                    int(v.get("category_id", 0)),
                    parse_duration_to_hms(v.get("duration")),
                    v.get("definition", "hd"),
                    v.get("licensed_content", "No"),
                    int(v.get("view_count", 0)),
                    int(v.get("like_count", 0)),
                    int(v.get("dislike_count", 0)),
                    int(v.get("favorite_count", 0)),
                    int(v.get("comment_count", 0)),
                    v.get("thumbnail", ""),
                    v.get("caption_status", "Unknown"),
                )
                for v in videos
            ]
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO channel_videos
                        (video_id, playlist_id, video_name, video_description, published_date,
                         category_id, duration, video_quality, licensed, view_count, like_count,
                         dislike_count, favorite_count, comments_count, thumbnail, caption_status)
                    VALUES %s ON CONFLICT (video_id) DO NOTHING;
                """, video_rows)
        else:
            st.info("No videos to migrate.")

        # ---- Insert comments ---- #
        progress_bar.progress(0.7, "💬 Inserting comment records...")
        if comments:
            comment_rows = []
            for c in comments:
                text = c.get("comment_text", "") or ""
                try:
                    sentiment = TextBlob(text).sentiment.polarity
                    lang = detect(text) if text.strip() else "en"
                except Exception:
                    sentiment = None
                    lang = "en"
                comment_rows.append((
                    c.get("comment_id"),
                    c.get("video_id"),
                    selected_channel,
                    text,
                    c.get("comment_date"),
                    c.get("author"),
                    int(c.get("like_count", 0)),
                    int(c.get("reply_count", 0)),
                    bool(c.get("is_pinned", False)),
                    bool(c.get("is_hearted", False)),
                    lang,
                    sentiment,
                    datetime.now(),
                ))
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO channel_comments
                        (comment_id, video_id, channel_name, comment_text, comment_date,
                         comment_author, comment_like, reply_count, is_pinned, is_hearted,
                         language, sentiment_score, harvested_at)
                    VALUES %s ON CONFLICT (comment_id) DO NOTHING;
                """, comment_rows)
        else:
            st.info("No comments to migrate.")

        conn.commit()
        progress_bar.progress(1.0, "✅ Migration complete!")
        st.success(f"✅ Channel '{selected_channel}' migrated to PostgreSQL")
        st.write(f"📦 {len(videos)} videos · {len(comments)} comments migrated.")

    except Exception as e:
        conn.rollback()
        st.error(f"❌ Migration failed: {e}")
        traceback.print_exc()
# ============================================================
# YouTube API Data Fetching Functions
# All functions use the _param (underscore-prefix) convention
# for @st.cache_data and pass it explicitly into every lambda
# to avoid closure / global-variable bugs.
# ============================================================
@st.cache_data
def get_channel_stats(_channel_id):
    """Fetch top-level channel statistics and the uploads playlist ID."""
    response = safe_api_call(
        lambda yt: yt.channels().list(
            part="snippet,contentDetails,statistics",
            id=_channel_id,
        ).execute(),
        cost_key="channels().list",
    )
    if not response or not response.get("items"):
        return None
    try:
        item = response["items"][0]
        return {
            "Channel_Id":    _channel_id,
            "Channel_name":  item["snippet"]["title"],
            "Subscribers":   item["statistics"].get("subscriberCount", 0),
            "Views":         item["statistics"].get("viewCount", 0),
            "Total_videos":  item["statistics"].get("videoCount", 0),
            # uploads playlist — contains EVERY uploaded video including non-playlist ones
            "playlist_id":   item["contentDetails"]["relatedPlaylists"]["uploads"],
        }
    except KeyError:
        return None

@st.cache_data
def get_all_playlists_for_channel(_channel_id, channel_name):
    """Fetch all public playlists the channel has created."""
    playlists = []
    next_page_token = None
    while True:
        # Capture _channel_id and next_page_token in default args to avoid closure bugs
        response = safe_api_call(
            lambda yt, cid=_channel_id, tok=next_page_token: yt.playlists().list(
                part="snippet,contentDetails,status",
                channelId=cid,
                maxResults=50,
                pageToken=tok,
            ).execute(),
            cost_key="playlists().list",
        )
        if not response:
            break
        for item in response.get("items", []):
            playlists.append({
                "playlist_id":    item["id"],
                "playlist_name":  item["snippet"]["title"],
                "channel_name":   channel_name,
                "channel_id":     _channel_id,
                "description":    item["snippet"].get("description", ""),
                "item_count":     item["contentDetails"].get("itemCount", 0),
                "privacy_status": item["status"].get("privacyStatus", "public"),
                "published_at":   item["snippet"].get("publishedAt"),
                "harvested_at":   datetime.now().isoformat(),
            })
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return playlists

# @st.cache_data
def get_videos_from_playlist(_playlist_id, max_results=500):
    """
    Fetch all video details from a single playlist (or the uploads playlist).
    Works for both named playlists AND the hidden 'uploads' playlist that
    contains every video a channel has ever uploaded — including videos that
    were never added to any named playlist.
    """
    videos = []
    next_page_token = None

    while True:
        # Step 1: get a page of video IDs from the playlist
        playlist_response = safe_api_call(
            lambda yt, pid=_playlist_id, tok=next_page_token: yt.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=pid,
                maxResults=50,
                pageToken=tok,
            ).execute(),
            cost_key="playlistItems().list",
        )
        if not playlist_response:
            break

        video_ids = [
            item["contentDetails"]["videoId"]
            for item in playlist_response.get("items", [])
        ]
        if not video_ids:
            break

        # Step 2: batch-fetch full video details for those IDs
        video_response = safe_api_call(
            lambda yt, ids=",".join(video_ids): yt.videos().list(
                part="snippet,contentDetails,statistics",
                id=ids,
            ).execute(),
            cost_key="videos().list",
        )
        if not video_response:
            break

        for item in video_response.get("items", []):
            snippet    = item.get("snippet", {})
            stats      = item.get("statistics", {})
            content    = item.get("contentDetails", {})
            videos.append({
                "video_id":       item["id"],
                "playlist_id":    _playlist_id,
                "video_title":    snippet.get("title", ""),
                "description":    snippet.get("description", ""),
                "published_at":   snippet.get("publishedAt"),
                "category_id":    snippet.get("categoryId", 0),
                "thumbnail":      snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                "duration":       content.get("duration", "PT0S"),
                "definition":     content.get("definition", "hd"),
                "caption_status": content.get("caption", "false"),
                "licensed_content": str(content.get("licensedContent", False)),
                "view_count":     int(stats.get("viewCount", 0)),
                "like_count":     int(stats.get("likeCount", 0)),
                "dislike_count":  int(stats.get("dislikeCount", 0)),
                "favorite_count": int(stats.get("favoriteCount", 0)),
                "comment_count":  int(stats.get("commentCount", 0)),
            })

        if len(videos) >= max_results:
            break

        next_page_token = playlist_response.get("nextPageToken")
        if not next_page_token:
            break

    return videos

# @st.cache_data
def get_comments_for_video(_video_id, max_comments=20):
    """
    Fetch up to max_comments top-level comments for a single video.
    Returns [] silently if comments are disabled on the video.
    """
    comments = []
    next_page_token = None

    while len(comments) < max_comments:
        remaining = max_comments - len(comments)
        try:
            response = safe_api_call(
                lambda yt, vid=_video_id, tok=next_page_token, n=min(remaining, 20): yt.commentThreads().list(
                    part="snippet",
                    videoId=vid,
                    maxResults=n,
                    pageToken=tok,
                    textFormat="plainText",
                ).execute(),
                cost_key="commentThreads().list",
            )
        except HttpError as e:
            # 403 = comments disabled/restricted — skip silently
            if e.resp.status in [403, 404]:
                break
            return None
        if not response:
            break

        for item in response.get("items", []):
            top = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "comment_id":   item["id"],
                "video_id":     _video_id,
                "comment_text": top.get("textDisplay", ""),
                "author":       top.get("authorDisplayName", ""),
                "like_count":   top.get("likeCount", 0),
                "reply_count":  item["snippet"].get("totalReplyCount", 0),
                "comment_date": top.get("publishedAt"),
                "is_pinned":    False,
                "is_hearted":   False,
            })

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return comments

# @st.cache_data(ttl=3600, show_spinner=False)
def extract_channel_all_details(_channel_id):
    """
    Full harvest pipeline for one channel.

    Video strategy
    ──────────────
    1. Fetch the 'uploads' playlist — this contains EVERY video the channel
       has ever uploaded, including videos not in any named playlist.
    2. Also fetch all named playlists (for playlist metadata / relationships).
    3. Deduplicate videos by video_id so a video appearing in both the uploads
       playlist and a named playlist is only stored once.
    """
    progress = st.progress(0.0, text="📤 Starting YouTube channel harvest...")

    # ── 1. Channel statistics ──────────────────────────────────────────────
    with st.spinner("Fetching channel statistics..."):
        channel_stats = get_channel_stats(_channel_id)
        if not channel_stats:
            st.warning("⚠️ Channel statistics not available.")
            return None
        channel_name     = channel_stats.get("Channel_name", "Unknown")
        uploads_pl_id    = channel_stats.get("playlist_id")   # hidden uploads playlist
    progress.progress(0.05, "✅ Channel stats fetched.")

    # ── 2. Named playlists ────────────────────────────────────────────────
    with st.spinner("📂 Fetching all named playlists..."):
        named_playlists = get_all_playlists_for_channel(_channel_id, channel_name)
    progress.progress(0.15, f"✅ {len(named_playlists)} named playlist(s) found.")

    # ── 3. Videos from ALL named playlists ───────────────────────────────
    all_videos_map = {}   # keyed by video_id for deduplication

    total_named = len(named_playlists)
    for idx, pl in enumerate(named_playlists):
        pid = pl["playlist_id"]
        with st.spinner(f"📹 Fetching videos from playlist {idx + 1}/{total_named}: {pl['playlist_name']}"):
            pl_videos = get_videos_from_playlist(pid)
            for v in pl_videos:
                all_videos_map[v["video_id"]] = v
        pct = 0.15 + 0.25 * ((idx + 1) / max(total_named, 1))
        progress.progress(pct, f"Playlist {idx + 1}/{total_named} done.")

    # ── 4. Videos from uploads playlist (catches non-playlist uploads) ────
    with st.spinner("📹 Fetching all uploaded videos (including those not in any playlist)..."):
        if uploads_pl_id:
            upload_videos = get_videos_from_playlist(uploads_pl_id)
            new_count = 0
            for v in upload_videos:
                if v["video_id"] not in all_videos_map:
                    # Mark that this video has no named playlist
                    v["playlist_id"] = None
                    all_videos_map[v["video_id"]] = v
                    new_count += 1
            if new_count:
                st.info(f"ℹ️ {new_count} additional video(s) found outside named playlists.")
    progress.progress(0.45, f"✅ {len(all_videos_map)} unique video(s) collected.")

    all_videos  = list(all_videos_map.values())
    video_ids   = [v["video_id"] for v in all_videos]

    # Validate video IDs
    valid_ids   = [vid for vid in video_ids if is_valid_video_id(vid)]
    skipped     = len(video_ids) - len(valid_ids)
    if skipped:
        st.warning(f"⚠️ {skipped} invalid video ID(s) skipped.")
    video_ids = valid_ids

    # ── 5. Comments for each video ────────────────────────────────────────
    all_comments = []
    total_vids   = len(video_ids)
    with st.spinner("💬 Fetching comments..."):
        for i, vid in enumerate(video_ids):
            vid_comments = get_comments_for_video(vid, max_comments=50)
            all_comments.extend(vid_comments)
            pct = 0.45 + 0.45 * ((i + 1) / max(total_vids, 1))
            progress.progress(pct, f"Comments: video {i + 1}/{total_vids}")
    progress.progress(0.95, "✅ Comments fetched.")

    # ── 6. Pack result ────────────────────────────────────────────────────
    channel_data = {
        "Channel_info":  channel_stats,
        "playlist_info": named_playlists,
        "Video_info":    all_videos,
        "Comment_info":  all_comments,
        "Meta": {
            "Total_Videos":   len(all_videos),
            "Total_Comments": len(all_comments),
        },
        "last_updated": datetime.now().isoformat(),
    }

    # Audit log
    mg_yth_db["audit_logs"].insert_one({
        "channel_id":    _channel_id,
        "channel_name":  channel_name,
        "status":        "success",
        "video_count":   len(all_videos),
        "comment_count": len(all_comments),
        "timestamp":     datetime.now().isoformat(),
    })

    progress.progress(1.0, "✅ Harvest complete!")
    return channel_data

# ============================================================
# Streamlit UI
# ============================================================
st.set_page_config(page_title="YouTube Data Harvesting", layout="wide")

with st.sidebar:
    selected = option_menu(
        menu_title="YouTube Data Harvesting",
        options=["Home", "---", "YDH_DB", "---", "Contact"],
        icons=["house", "upload", "envelope"],
        menu_icon="cast",
        default_index=0,
        orientation="vertical",
        styles={
            "container":          {"padding": "0!important", "background-color": "#AFBFAB"},
            "icon":               {"color": "orange", "font-size": "15px"},
            "nav-link":           {"font-size": "15px", "text-align": "left", "margin": "5px", "--hover-color": "#eee"},
            "nav-link-selected":  {"background-color": "grey"},
        },
    )

# ---------- Session state defaults ------------ #
if "api_status" not in st.session_state:
    st.session_state.api_status = "🕒 Waiting for API connection test..."
if "tested_channel_name" not in st.session_state:
    st.session_state.tested_channel_name = "No Channel to Display"
if "quota_used" not in st.session_state:
    st.session_state.quota_used = 0
if "extracted_data" not in st.session_state:
    st.session_state.extracted_data = {}
if "extracted_channel_id" not in st.session_state:
    st.session_state.extracted_channel_id = None
if "delete_requested" not in st.session_state:
    st.session_state.delete_requested = None

# ==============================
# HOME
# ==============================
if selected == "Home":
    st.header("Project: YouTube Data Harvesting")
    st.markdown("""
    This application allows you to:
    - 🔍 Search and extract data from YouTube channels via the Google YouTube API
    - 🍃 Store harvested data in **MongoDB** (data lake)
    - 🐘 Migrate data to **PostgreSQL** (data warehouse)
    - 📊 Analyse channel data using pre-built SQL queries
    """)

# ==============================
# YDH_DB
# ==============================
if selected == "YDH_DB":
    selected = option_menu(
        menu_title="YouTube Data Harvesting DB",
        options=["YT Channel Extractor", "DB Manager", "YT Channel Analyzer"],
        icons=["cloud-download", "database-gear", "bar-chart"],
        menu_icon="database-gear",
        default_index=0,
        orientation="horizontal",
    )
    # ──────────────────────────────────────────────
    # TAB 1 — YT Channel Extractor
    # ──────────────────────────────────────────────
    if selected == "YT Channel Extractor":
        st.markdown("### 🔍 YouTube Channel Extractor")
        st.caption("Use this tab to test your API, monitor quota, search and harvest YouTube channels.")
        # ── Custom CSS for section cards ──────────────────────────────────────
        # ════════════════════════════════════════════
        # SECTION 1 — YouTube API Connection Test
        # ════════════════════════════════════════════
        with st.container(border=True):
            # Section header row
            sec1_col_icon, sec1_col_title = st.columns([0.05, 0.95])
            with sec1_col_icon:
                st.markdown("### 🔌")
            with sec1_col_title:
                st.markdown("#### Section 1 · YouTube API Connection Test")
                st.caption("Verify your YouTube API key is valid before starting extraction.")
            # st.divider()
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                test_btn = st.button(
                    "▶ Test YT API",
                    use_container_width=True,
                    help="Sends a test request to YouTube API using your configured key",
                )
            with col2:
                reset_btn = st.button(
                    "↺ Reset Status",
                    use_container_width=True,
                    help="Clear the current API status display",
                )

            if test_btn:
                with st.spinner("Testing YouTube API connection..."):
                    try:
                        test_resp = safe_api_call(
                            lambda yt: yt.channels().list(
                                part="snippet", id="UC_x5XG1OV2P6uZZ5FSM9Ttw"
                            ).execute(),
                            cost_key="channels().list",
                        )
                        if test_resp and test_resp.get("items"):
                            name = test_resp["items"][0]["snippet"]["title"]
                            st.session_state.api_status = "success"
                            st.session_state.tested_channel_name = name
                        else:
                            st.session_state.api_status = "warning"
                            st.session_state.tested_channel_name = "No Channel to Display"
                    except Exception as e:
                        st.session_state.api_status = "error"
                        st.session_state.tested_channel_name = "No Channel to Display"

            if reset_btn:
                st.session_state.api_status = "idle"
                st.session_state.tested_channel_name = "No Channel to Display"

            # Status display using native Streamlit callouts
            status = st.session_state.get("api_status", "idle")
            tested_name = st.session_state.get("tested_channel_name", "No Channel to Display")

            if status == "success":
                st.success(f"✅ API connection valid — Tested on channel: **{tested_name}**")
            elif status == "warning":
                st.warning("⚠️ API call returned no items. Check your key restrictions.")
            elif status == "error":
                st.error("❌ API test failed. Check your key in secrets.toml.")
            else:
                st.info("🕒 Click **Test YT API** to verify your connection before extracting.")
        # ════════════════════════════════════════════
        # SECTION 2 — YouTube API Quota Counter
        # ════════════════════════════════════════════
        with st.container(border=True):
            sec2_col_icon, sec2_col_title = st.columns([0.05, 0.95])
            with sec2_col_icon:
                st.markdown("#### 📊")
            with sec2_col_title:
                st.markdown("#### Section 2 · YouTube API Quota Monitor")
                st.caption("Track daily quota usage across both Google Cloud projects (20,000 units total).")
            show_quota_usage()
        # ════════════════════════════════════════════
        # SECTION 3 — Channel Search & Extraction
        # ════════════════════════════════════════════
        with st.container(border=True):
            sec3_col_icon, sec3_col_title = st.columns([0.05, 0.95])
            with sec3_col_icon:
                st.markdown("#### 🔍")
            with sec3_col_title:
                st.markdown("#### Section 3 · YouTube Channel Search & Extraction")
                st.caption("Enter a YouTube Channel ID to search or fully harvest it into MongoDB.")
            # st.divider()
            # Input
            channel_id = st.text_input(
                "YouTube Channel ID",
                placeholder="e.g. UCQhpnItclGAUn4NdGGcEyPQ",
                help="Find the Channel ID from the channel's About page or URL",
            )

            # Action buttons + options
            act_c1, act_c2 = st.columns([1, 2])
            with act_c1:
                Search = st.button("🔎 Search Channel", use_container_width=True)
            with act_c2:
                Extract = st.button("⬇️ Extract & Save to MongoDB", use_container_width=True)

            opt_c1, opt_c2, opt_c3 = st.columns([1, 1, 1])
            with opt_c1:
                store_pgsql = st.checkbox("📦 Also store in PostgreSQL")
            with opt_c2:
                export_json = st.checkbox("🗃️ Export as JSON")
            with opt_c3:
                if st.button("🧹 Clear Extraction Cache", use_container_width=True,
                             help="Clears cached API responses — forces a fresh fetch on next extraction"):
                    st.cache_data.clear()
                    st.success("✅ Cache cleared. Next extraction will fetch fresh data from YouTube.")
            st.info("💡 Clear Cache Before every New attempt / Channel Extraction")

            # ── Search ───────────────────────────────────────
            if Search and channel_id:
                with st.spinner("Searching channel..."):
                    view_data = get_channel_stats(channel_id)
                if view_data:
                    st.success("✅ Channel found!")
                    st.metric("📺 Channel", view_data.get("Channel_name", "—"))
                    s1, s2, s3 = st.columns(3)
                    # s1.metric("📺 Channel", view_data.get("Channel_name", "—"))
                    s1.metric("👥 Subscribers", f"{int(view_data.get('Subscribers', 0)):,}")
                    s2.metric("👁️ Total Views", f"{int(view_data.get('Views', 0)):,}")
                    s3.metric("🎞️ Videos", view_data.get("Total_videos", "—"))
                else:
                    st.error("❌ Channel not found. Check the ID and try again.", icon="🚨")
            elif Search:
                st.warning("⚠️ Please enter a Channel ID to search.")

            # ── Extract ──────────────────────────────────────
            if Extract and channel_id:
                extracted_data = extract_channel_all_details(channel_id)

                if extracted_data:
                    # Save to session state
                    st.session_state.extracted_data = extracted_data
                    st.session_state.extracted_channel_id = channel_id

                    channel_name = extracted_data.get("Channel_info", {}).get("Channel_name")
                    if not channel_name:
                        st.error("❌ Channel name missing. Cannot store.")
                        st.stop()

                    channel_info = extracted_data["Channel_info"]

                    # MongoDB saves
                    mg_yth_db[f"{channel_name}_meta"].delete_many({})
                    mg_yth_db[f"{channel_name}_meta"].insert_one({
                        "Channel_Id": channel_info.get("Channel_Id"),
                        "Channel_name": channel_info.get("Channel_name"),
                        "Subscribers": channel_info.get("Subscribers"),
                        "Views": channel_info.get("Views"),
                        "Total_videos": channel_info.get("Total_videos"),
                        "Harvested_at": datetime.now().isoformat(),
                    })
                    playlist_info = extracted_data.get("playlist_info", [])
                    mg_yth_db[f"{channel_name}_playlist"].delete_many({})
                    if playlist_info:
                        mg_yth_db[f"{channel_name}_playlist"].insert_many(playlist_info)

                    videos = extracted_data.get("Video_info", [])
                    mg_yth_db[f"{channel_name}_videos"].delete_many({})
                    if videos:
                        mg_yth_db[f"{channel_name}_videos"].insert_many(videos)

                    comments = extracted_data.get("Comment_info", [])
                    mg_yth_db[f"{channel_name}_comments"].delete_many({})
                    if comments:
                        mg_yth_db[f"{channel_name}_comments"].insert_many(comments)

                    # Result summary metrics
                    st.success("✅ Harvest complete and saved to MongoDB!")
                    r1, r2, r3 = st.columns(3)
                    r1.metric("📺 Channel", channel_name)
                    r2.metric("🎞️ Videos Harvested", len(videos),
                              help="Includes videos outside named playlists")
                    r3.metric("💬 Comments Harvested", len(comments))

                    # Optional direct storage to PostgreSQL - Basic info
                    if store_pgsql:
                        try:
                            with init_connection() as conn:
                                store_postgresql_direct(conn, extracted_data)
                        except Exception as e:
                            st.error(f"❌ PostgreSQL storage failed: {e}")

                else:
                    st.error("❌ Extraction failed. Check the Channel ID and try again.", icon="🚨")

            elif Extract and not channel_id:
                st.warning("⚠️ Please enter a Channel ID to extract.")

            #------- JSON download — always visible if data exists in session state ---- #
            _dl_data = st.session_state.get("extracted_data", {})
            _dl_channel_id = st.session_state.get("extracted_channel_id", "channel")
            if _dl_data:
                if export_json:
                    json_data = json.dumps(_dl_data, indent=2, default=str)
                    st.download_button(
                        label="📥 Download Extracted Data as JSON",
                        data=json_data,
                        file_name=f"{_dl_channel_id}_youtube_data.json",
                        mime="application/json",
                        use_container_width=True,
                    )
                    # st.info("💡 Go to **DB Manager → MongoDB Manager** to view videos, comments and charts.")
                else:
                    st.caption("💡 Tick the 'Export data as JSON' checkbox above to enable download.")
            else:
                st.info("💡 Extract a channel first to enable JSON download.")
                st.info("💡 Go to **DB Manager → MongoDB Manager** to view videos, comments and charts.")
    # ──────────────────────────────────────────────
    # TAB 2 — Mongo Manager
    # ──────────────────────────────────────────────
    if selected == "DB Manager":
        st.markdown("### 🗄️ Database Manager")
        st.caption("Manage harvested channel data in MongoDB and migrate it to PostgreSQL.")

        # Shared channel list — used by both sub-tabs
        all_harvested = [
            c.replace("_meta", "")
            for c in mg_yth_db.list_collection_names()
            if c.endswith("_meta")
        ]
        if not all_harvested:
            st.info("ℹ️ No harvested channels found in MongoDB. Go to YT Channel Extractor first.")
        else:
            mongo_tab, pg_tab = st.tabs(["🍃 MongoDB Manager", "🐘 PostgreSQL Manager"])
            # ── MongoDB Manager ──────────────────────────────
            with mongo_tab:
                # st.markdown("#### Manage Harvested Channels in MongoDB")
                st.markdown("#### 🍃 MongoDB Manager")
                st.caption("View and manage harvested channel data stored in MongoDB.")

                # ── Channel Selector Section ──────────────────────
                with st.container(border=True):
                    sec_icon, sec_title = st.columns([0.05, 0.95])
                    with sec_icon:
                        st.markdown("#### 📋")
                    with sec_title:
                        st.markdown("#### Channel Selection")
                        st.caption("Choose a harvested channel to inspect or delete.")

                    # st.divider()

                    selected_channel_mg = st.selectbox(
                        "Harvested Channels", all_harvested, key="mg_select",
                        help="Only channels already extracted and saved to MongoDB appear here",
                    )

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        view_basic = st.button("📄 View Basic Info", use_container_width=True)
                    with col2:
                        view_full = st.button("📊 View Full Data", use_container_width=True)
                    with col3:
                        delete_ch = st.button("🗑️ Delete Channel", use_container_width=True,
                                              help="Permanently removes all collections for this channel from MongoDB")

                # ── Channel Basic Info ────────────────────
                if view_basic and selected_channel_mg:
                    with st.container(border=True):
                        sec_icon, sec_title = st.columns([0.05, 0.95])
                        with sec_icon:
                            st.markdown("### 📄")
                        with sec_title:
                            st.markdown(f"### Basic Info — {selected_channel_mg}")
                            st.caption("Channel metadata and collection counts from MongoDB.")

                        doc = mg_yth_db[f"{selected_channel_mg}_meta"].find_one()
                        if doc:
                            doc.pop("_id", None)

                            # Metadata as metrics
                            m1, m2, m3 = st.columns(3)
                            # m1.metric("📺 Channel", doc.get("Channel_name", ""))
                            m1.metric("👥 Subscribers", f"{int(doc.get('Subscribers', 0)):,}")
                            m2.metric("👁️ Total Views", f"{int(doc.get('Views', 0)):,}")
                            m3.metric("🎞️ Videos", doc.get("Total_videos", "—"))

                            st.caption(f"🕒 Harvested at: {doc.get('Harvested_at', '—')}")
                            st.divider()

                            # Collection size summary
                            st.markdown("**MongoDB Collection Counts**")
                            summary = {}
                            for suffix in ["_playlist", "_videos", "_comments"]:
                                col_name = f"{selected_channel_mg}{suffix}"
                                if col_name in mg_yth_db.list_collection_names():
                                    summary[suffix.replace("_", "").capitalize()] = mg_yth_db[
                                        col_name].count_documents({})
                            if summary:
                                s_cols = st.columns(len(summary))
                                for idx, (label, count) in enumerate(summary.items()):
                                    s_cols[idx].metric(f"📁 {label}", f"{count:,} docs")
                        else:
                            st.warning("⚠️ No metadata found for this channel.")
                # ── Full Data View ─────────────────────────
                if view_full and selected_channel_mg:
                    with st.container(border=True):
                        sec_icon, sec_title = st.columns([0.05, 0.95])
                        with sec_icon:
                            st.markdown("### 📊")
                        with sec_title:
                            st.markdown(f"### Full Data — {selected_channel_mg}")
                            st.caption("Videos, comments and playlists loaded directly from MongoDB.")

                        videos_data = list(mg_yth_db[f"{selected_channel_mg}_videos"].find())
                        comments_data = list(mg_yth_db[f"{selected_channel_mg}_comments"].find())
                        playlist_data = list(mg_yth_db[f"{selected_channel_mg}_playlist"].find())

                        videos_df = pd.DataFrame(videos_data).drop(columns=["_id"], errors="ignore")
                        comments_df = pd.DataFrame(comments_data).drop(columns=["_id"], errors="ignore")
                        playlist_df = pd.DataFrame(playlist_data).drop(columns=["_id"], errors="ignore")

                        # Top 10 chart
                        if not videos_df.empty and "view_count" in videos_df.columns:
                            st.markdown("#### 📈 Top 10 Videos by Views")
                            videos_df["view_count"] = pd.to_numeric(videos_df["view_count"],
                                                                    errors="coerce")
                            top10 = videos_df.nlargest(10, "view_count")
                            fig = px.bar(
                                top10, x="view_count", y="video_title", orientation="h",
                                title=f"Top 10 Videos — {selected_channel_mg}",
                                color="view_count", color_continuous_scale="reds",
                                labels={"view_count": "Views", "video_title": ""},
                            )

                            fig.update_layout(
                                yaxis=dict(autorange="reversed"),  # highest at top
                                xaxis_title="View Count",
                                yaxis_title="",
                                height=450,
                                margin=dict(l=10, r=10, t=40, b=10),
                                coloraxis_showscale=False,  # hide colour bar
                            )
                            fig.update_traces(
                                texttemplate="%{x:,.0f}",  # show count on bar
                                textposition="outside",
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            st.divider()
                        # Videos dataframe
                        st.markdown("#### 🎞️ Videos")
                        if not videos_df.empty:
                            st.dataframe(videos_df, use_container_width=True)
                            st.caption(f"Total: {len(videos_df):,} videos")
                        else:
                            st.info("No videos found.")
                        st.divider()

                        # Comments dataframe
                        st.markdown("#### 💬 Comments")
                        if not comments_df.empty:
                            st.dataframe(comments_df, use_container_width=True)
                            st.caption(f"Total: {len(comments_df):,} comments")
                        else:
                            st.info("No comments found.")
                        st.divider()

                        # Playlists dataframe
                        st.markdown("#### 📂 Playlists")
                        if not playlist_df.empty:
                            st.dataframe(playlist_df, use_container_width=True)
                            st.caption(f"Total: {len(playlist_df):,} playlists")
                        else:
                            st.info("No playlists found.")

                        # MongoDB JSON export
                        st.divider()
                        if not videos_df.empty:
                            mongo_json = json.dumps({
                                "channel": selected_channel_mg,
                                "videos": videos_df.to_dict(orient="records"),
                                "comments": comments_df.to_dict(orient="records"),
                                "playlists": playlist_df.to_dict(orient="records"),
                            }, indent=2, default=str)
                            st.download_button(
                                label="📥 Download MongoDB Data as JSON",
                                data=mongo_json,
                                file_name=f"{selected_channel_mg}_mongodb_export.json",
                                mime="application/json",
                                use_container_width=True,
                            )
                # ── Delete button ─────────────────────────────────────
                if delete_ch and selected_channel_mg:
                    st.session_state.delete_requested = selected_channel_mg
                # ── Delete confirmation ───────────────────────────────
                if st.session_state.delete_requested:
                    channel_to_delete = st.session_state.delete_requested
                    with st.container(border=True):
                        st.warning(f"⚠️ This will permanently delete all MongoDB data for **{selected_channel_mg}**.")
                        confirm_c1, confirm_c2, _ = st.columns([1, 1, 2])
                        with confirm_c1:
                            if st.button("✅ Yes, Delete", use_container_width=True, key="confirm_delete"):
                                for suffix in ["_meta", "_playlist", "_videos", "_comments"]:
                                    mg_yth_db[f"{selected_channel_mg}{suffix}"].drop()
                                # st.success(f"✅ '{selected_channel_mg}' deleted from MongoDB.")
                                # Clear session state and rerun
                                st.session_state.delete_requested = None
                                st.success(f"✅ '{selected_channel_mg}' deleted from MongoDB.")
                                st.rerun()
                        with confirm_c2:
                            if st.button("❌ Cancel", use_container_width=True, key="cancel_delete"):
                                st.session_state.delete_requested = None
                                st.info("Deletion cancelled.")
                                st.rerun()

                # if view_full and selected_channel_mg:
                #     playlist_col = f"{selected_channel_mg}_playlist"
                #     video_col = f"{selected_channel_mg}_videos"
                #     comment_col = f"{selected_channel_mg}_comments"
                #
                #     if playlist_col in mg_yth_db.list_collection_names():
                #         st.subheader("📂 Playlists")
                #         st.dataframe(
                #             pd.DataFrame(mg_yth_db[playlist_col].find()).drop(columns=["_id"], errors="ignore"),
                #             use_container_width=True,
                #         )
                #     if video_col in mg_yth_db.list_collection_names():
                #         st.subheader("🎞️ Videos")
                #         st.dataframe(
                #             pd.DataFrame(mg_yth_db[video_col].find()).drop(columns=["_id"], errors="ignore"),
                #             use_container_width=True,
                #         )
                #     if comment_col in mg_yth_db.list_collection_names():
                #         st.subheader("💬 Comments")
                #         st.dataframe(
                #             pd.DataFrame(mg_yth_db[comment_col].find()).drop(columns=["_id"], errors="ignore"),
                #             use_container_width=True,
                #         )
                #
                # if delete_ch and selected_channel_mg:
                #     for suffix in ["_meta", "_playlist", "_videos", "_comments"]:
                #         mg_yth_db[f"{selected_channel_mg}{suffix}"].drop()
                #     st.success(f"✅ '{selected_channel_mg}' deleted from MongoDB.")

            # ── PostgreSQL Manager ───────────────────────────
            with pg_tab:
                st.markdown("#### 🐘 PostgreSQL Manager")
                st.caption("Migrate harvested channel data from MongoDB into the PostgreSQL data warehouse.")
                # st.markdown("#### Migrate Channel from MongoDB → PostgreSQL")

                # ──  Migration ──────────────────────────────
                with st.container(border=True):
                    sec_icon, sec_title = st.columns([0.05, 0.95])
                    with sec_icon:
                        st.markdown("### 🚀")
                    with sec_title:
                        st.markdown("#### Migrate Channel from MongoDB → PostgreSQL")
                        st.caption("Populates channel_table, channel_playlist, channel_videos and channel_comments.")
                    selected_channel_pg = st.selectbox(
                        "Select a channel to migrate", all_harvested, key="pg_select"
                    )
                    mg_col1, mg_col2, mg_col3 = st.columns(3)
                    with mg_col1:
                        playlists_count = mg_yth_db[f"{selected_channel_pg}_playlist"].count_documents(
                            {}) if f"{selected_channel_pg}_playlist" in mg_yth_db.list_collection_names() else 0
                        st.metric("📂 Playlists in MongoDB", f"{playlists_count:,}")
                    with mg_col2:
                        videos_count = mg_yth_db[f"{selected_channel_pg}_videos"].count_documents(
                            {}) if f"{selected_channel_pg}_videos" in mg_yth_db.list_collection_names() else 0
                        st.metric("🎞️ Videos in MongoDB", f"{videos_count:,}")
                    with mg_col3:
                        comments_count = mg_yth_db[f"{selected_channel_pg}_comments"].count_documents(
                            {}) if f"{selected_channel_pg}_comments" in mg_yth_db.list_collection_names() else 0
                        st.metric("💬 Comments in MongoDB", f"{comments_count:,}")

                    st.divider()

                    # if st.button("🚀 Migrate to PostgreSQL", use_container_width=True):
                    #     with init_connection() as conn:
                    #         migrate_to_postgresql(conn, selected_channel_pg, mg_yth_db)

                    col_a, col_b = st.columns([1, 2])
                    with col_a:
                        migrate_btn = st.button("🚀 Migrate to PostgreSQL")
                    with col_b:
                        st.caption(
                            "This will create the channel_table, channel_playlist, "
                            "channel_videos and channel_comments tables in PostgreSQL "
                            "and populate them from MongoDB."
                        )

                    if migrate_btn and selected_channel_pg:
                        with init_connection() as conn:
                            migrate_to_postgresql(conn, selected_channel_pg, mg_yth_db)

                # ── Section 2: Direct Store Table ────────────────────
                with st.container(border=True):
                    st.markdown("##### 🗃️ Direct Store — Basic Channel Info")
                    st.caption("Shows channels stored directly during extraction via 'Also store in PostgreSQL'.")
                    st.markdown('<hr style="margin:0.5rem 0; border-color:#333">', unsafe_allow_html=True)

                    try:
                        conn = init_connection()
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT channel_name, subscribers, channel_views,
                                       total_videos, harvested_time
                                FROM channel_table_direct
                                ORDER BY harvested_time DESC;
                            """)
                            rows = cur.fetchall()

                        if rows:
                            df_direct = pd.DataFrame(rows, columns=[
                                "Channel Name", "Subscribers",
                                "Channel Views", "Total Videos", "Harvested Time"
                            ])
                            # Summary metrics
                            m1, m2, m3 = st.columns(3)
                            m1.metric("📺 Channels Stored", len(df_direct))
                            m2.metric("🎞️ Total Videos",
                                      f"{df_direct['Total Videos'].astype(int).sum():,}")
                            m3.metric("👥 Total Subscribers",
                                      f"{df_direct['Subscribers'].astype(int).sum():,}")
                            st.markdown('<hr style="margin:0.5rem 0; border-color:#333">',
                                        unsafe_allow_html=True)
                            st.dataframe(df_direct, use_container_width=True)
                        else:
                            st.info("ℹ️ No direct store data found. Extract a channel with "
                                    "'Also store in PostgreSQL' checked.")

                    except Exception as e:
                        st.error(f"❌ Could not load direct store table: {e}")

    # ──────────────────────────────────────────────
    # TAB 4 — YT Channel Analyzer (10 SQL queries)
    # ──────────────────────────────────────────────
    if selected == "YT Channel Analyzer":
        st.markdown("#### 📊 YouTube Channel Analyzer")
        st.caption("Run the pre-built SQL queries against your PostgreSQL data warehouse.")

        def run_query(query, columns, index_col=None):
            """Execute a SQL query and return a styled dataframe."""
            try:
                conn = init_connection()
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                if index_col and index_col in df.columns:
                    df = df.set_index(index_col)
                return df
            except Exception as e:
                st.error(f"❌ Query failed: {e}")
                return pd.DataFrame()

        # Q1
        with st.expander("Q1 · Names of all videos and their corresponding channels"):
            df = run_query(
                """
                SELECT ch.channel_name, v.video_name
                FROM channel_videos v
                JOIN channel_playlist p ON v.playlist_id = p.playlist_id
                JOIN channel_table   ch ON p.channel_id  = ch.channel_id
                ORDER BY ch.channel_name
                """,
                ["Channel Name", "Video Title"],
                index_col="Channel Name",
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)

        # Q2
        with st.expander("Q2 · Channels with the most number of videos"):
            df = run_query(
                """
                SELECT ch.channel_name, ch.total_videos AS video_count
                FROM channel_table ch
                ORDER BY ch.total_videos DESC
                LIMIT 10
                """,
                ["Channel Name", "Video Count"],
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                fig = px.bar(df, x="Channel Name", y="Video Count", title="Channels by Video Count")
                st.plotly_chart(fig)

        # Q3
        with st.expander("Q3 · Top 10 most viewed videos and their channels"):
            df = run_query(
                """
                SELECT ch.channel_name, v.video_name, v.view_count
                FROM channel_videos v
                LEFT JOIN channel_playlist p ON v.playlist_id = p.playlist_id
                LEFT JOIN channel_table   ch ON p.channel_id  = ch.channel_id
                ORDER BY v.view_count DESC
                LIMIT 10
                """,
                ["Channel Name", "Video Title", "View Count"],
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                fig = px.bar(df, x="Video Title", y="View Count", color="Channel Name",
                             title="Top 10 Most Viewed Videos")
                fig.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig)

        # Q4
        with st.expander("Q4 · Number of comments on each video"):
            df = run_query(
                """
                SELECT v.video_name, v.comments_count
                FROM channel_videos v
                ORDER BY v.comments_count DESC
                """,
                ["Video Name", "Comment Count"],
                index_col="Video Name",
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)

        # Q5
        with st.expander("Q5 · Videos with the highest number of likes"):
            df = run_query(
                """
                SELECT ch.channel_name, v.video_name, v.like_count
                FROM channel_videos v
                LEFT JOIN channel_playlist p ON v.playlist_id = p.playlist_id
                LEFT JOIN channel_table   ch ON p.channel_id  = ch.channel_id
                ORDER BY v.like_count DESC
                LIMIT 10
                """,
                ["Channel Name", "Video Title", "Like Count"],
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)

        # Q6
        with st.expander("Q6 · Total likes and dislikes for each video"):
            df = run_query(
                """
                SELECT video_name, like_count, dislike_count
                FROM channel_videos
                ORDER BY like_count DESC
                """,
                ["Video Name", "Like Count", "Dislike Count"],
                index_col="Video Name",
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                st.caption("Note: YouTube removed public dislike counts in 2021; dislike_count will be 0.")

        # Q7
        with st.expander("Q7 · Total views for each channel"):
            df = run_query(
                """
                SELECT channel_name, channel_views AS total_views
                FROM channel_table
                ORDER BY total_views DESC
                """,
                ["Channel Name", "Total Views"],
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                fig = px.bar(df, x="Channel Name", y="Total Views", title="Total Views per Channel")
                st.plotly_chart(fig)

        # Q8
        with st.expander("Q8 · Channels that published videos in 2022"):
            df = run_query(
                """
                SELECT DISTINCT ch.channel_name,
                       COUNT(v.video_id)  AS videos_in_2022,
                       SUM(v.view_count)  AS total_views
                FROM channel_videos v
                LEFT JOIN channel_playlist p ON v.playlist_id = p.playlist_id
                LEFT JOIN channel_table   ch ON p.channel_id  = ch.channel_id
                WHERE EXTRACT(YEAR FROM v.published_date) = 2022
                GROUP BY ch.channel_name
                ORDER BY videos_in_2022 DESC
                """,
                ["Channel Name", "Videos in 2022", "Total Views"],
                index_col="Channel Name",
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No videos published in 2022 found in the warehouse.")

        # Q9
        with st.expander("Q9 · Average duration of videos per channel"):
            df = run_query(
                """
                SELECT ch.channel_name,
                       TO_CHAR(AVG(v.duration::interval), 'HH24:MI:SS') AS avg_duration
                FROM channel_videos v
                LEFT JOIN channel_playlist p ON v.playlist_id = p.playlist_id
                LEFT JOIN channel_table   ch ON p.channel_id  = ch.channel_id
                GROUP BY ch.channel_name
                ORDER BY ch.channel_name
                """,
                ["Channel Name", "Avg Duration (HH:MM:SS)"],
                index_col="Channel Name",
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)

        # Q10
        with st.expander("Q10 · Videos with the highest number of comments"):
            df = run_query(
                """
                SELECT ch.channel_name, v.video_name, v.comments_count
                FROM channel_videos v
                LEFT JOIN channel_playlist p ON v.playlist_id = p.playlist_id
                LEFT JOIN channel_table   ch ON p.channel_id  = ch.channel_id
                ORDER BY v.comments_count DESC
                LIMIT 10
                """,
                ["Channel Name", "Video Title", "Comment Count"],
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True)

# ==============================
# CONTACT
# ==============================
if selected == "Contact":
    st.header("Project: YouTube Data Harvesting")
    st.subheader("My Contact Details")
    st.write("Created by: Akellesh Vasudevan")
    st.write("LinkedIn Profile:")
    st.markdown("https://www.linkedin.com/in/akellesh/")
    st.write("GitHub Profile:")
    st.markdown(
        "https://github.com/Akellesh/YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit---Project"
    )
