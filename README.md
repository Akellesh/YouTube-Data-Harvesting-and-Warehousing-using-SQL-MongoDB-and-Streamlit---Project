# 📺 YouTube Data Harvesting & Warehousing

A full-stack data engineering project that extracts YouTube channel data via the YouTube Data API v3, stages it in MongoDB (data lake), migrates it to PostgreSQL (data warehouse), and surfaces insights through an interactive Streamlit dashboard.

> Built as a placement-ready portfolio project demonstrating real-world data pipeline design.

---

## 🚀 Features

- **Multi-channel extraction** — Fetch channel metadata, playlists, videos, and comments in one click
- **Uploads playlist approach** — Captures all videos via `contentDetails.relatedPlaylists.uploads`, not just named playlists
- **API quota management** — Rotates across multiple Google Cloud API keys (~20,000 units/day combined)
- **Two-tier storage** — MongoDB as a flexible data lake; PostgreSQL as a structured data warehouse
- **10 analytical SQL queries** — Pre-built queries exposed via the Streamlit UI for instant insights
- **DB Manager** — Unified tab to inspect, migrate, and delete data across both databases

---

## 🏗️ Architecture

```
YouTube Data API v3
        │
        ▼
  [Harvest Layer]
  Python + google-api-python-client
        │
        ▼
   MongoDB Atlas          ◄──── Data Lake (raw JSON documents)
        │
        ▼  (Migrate)
   PostgreSQL             ◄──── Data Warehouse (normalized relational schema)
        │
        ▼
  Streamlit App           ◄──── Interactive Dashboard & SQL Explorer
```

---

## 🗂️ Project Structure

```
youtube-data-harvesting/
├── app.py                  # Streamlit entry point
├── harvest.py              # YouTube API extraction logic
├── mongo_db.py             # MongoDB staging functions
├── postgres_db.py          # PostgreSQL migration & schema
├── sql_queries.py          # 10 analytical queries
├── .streamlit/
│   └── secrets.toml        # API keys & DB credentials (gitignored)
├── requirements.txt
└── README.md
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Extraction | YouTube Data API v3 |
| Data Lake | MongoDB |
| Data Warehouse | PostgreSQL |
| Frontend | Streamlit |
| Language | Python 3.10+ |
| IDE | PyCharm |

---

## ⚙️ Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/Akellesh/youtube-data-harvesting.git
cd youtube-data-harvesting
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure secrets

Create `.streamlit/secrets.toml` (never commit this file):

```toml
[youtube1]
api_key_1 = "YOUR_KEY_1"
api_key_2 = "YOUR_KEY_2"
api_key_3 = "YOUR_KEY_3"

[youtube2]
api_key_1 = "YOUR_KEY_4"
api_key_2 = "YOUR_KEY_5"
api_key_3 = "YOUR_KEY_6"

[mongo]
uri = "mongodb+srv://<user>:<password>@cluster.mongodb.net/"

[postgres]
host     = "localhost"
port     = 5432
database = "youtube_dw"
user     = "your_user"
password = "your_password"
```

### 5. Run the app

```bash
streamlit run app.py
```

---

## 📊 Analytical Queries

The SQL Explorer tab exposes 10 pre-built queries, including:

1. Videos and their channels
2. Channels with the most videos
3. Top 10 most-viewed videos
4. Videos with the most comments
5. Videos with the highest likes
6. Total views per channel
7. Channels published in a given year
8. Average video duration per channel
9. Videos with the most comments (ranked)
10. Channels ranked by total view count

---

## 🗄️ Database Schema

**PostgreSQL tables:**

| Table | Key Columns |
|---|---|
| `channels` | `channel_id`, `channel_name`, `subscribers`, `total_views` (BIGINT) |
| `playlists` | `playlist_id`, `channel_id`, `playlist_name` |
| `videos` | `video_id`, `channel_id`, `title`, `views`, `likes`, `duration` |
| `comments` | `comment_id`, `video_id`, `author`, `text`, `published_at` |

---

## 📌 Key Design Decisions

- **BIGINT for view counts** — Channels with billions of views exceed INT limits
- **Deduplication by `video_id`** — Prevents duplicate records on repeated migrations
- **Session state persistence** — Extracted data survives Streamlit reruns without re-fetching
- **API key rotation** — Keys interleaved across two Google Cloud projects to maximize daily quota
- **403/400 errors suppressed** — Quota errors handled gracefully; users see friendly messages

---

## 🔮 Roadmap

- [ ] Trend analysis with time-series charts
- [ ] Sentiment analysis on comments (NLP)
- [ ] Scheduled auto-harvesting with APScheduler
- [ ] Docker containerization

---

## 👤 Author

**Akellesh**
- GitHub: [@Akellesh](https://github.com/Akellesh)
- LinkedIn: [linkedin.com/in/akellesh](https://linkedin.com/in/akellesh)

---

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).
