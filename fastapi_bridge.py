import threading
import random
import asyncio
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import time
import os
import json
import logging
import socket
import subprocess
import uuid
from pathlib import Path
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_local_env(env_path: str = ".env") -> None:
    """Load simple KEY=VALUE pairs from the project .env without requiring python-dotenv."""
    path = Path(__file__).resolve().with_name(env_path)
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        logger.warning("Invalid integer for %s; using default %s", name, default)
        return default


def env_csv(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


load_local_env()

# --- Configuration ---
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = env_int("API_PORT", 8000)
FRONTEND_ORIGINS = env_csv("FRONTEND_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")

KAFKA_SERVERS = env_csv("KAFKA_BOOTSTRAP_SERVERS", "10.204.131.11:9092,10.84.128.10:9092")
KAFKA_TOPIC_PLAY = os.getenv("KAFKA_TOPIC_PLAY", "music.events.play")
KAFKA_TOPIC_SKIP = os.getenv("KAFKA_TOPIC_SKIP", "music.events.skip")
KAFKA_TOPIC_LIKE = os.getenv("KAFKA_TOPIC_LIKE", "music.events.like")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
KAFKA_CREATE_TOPICS = env_bool("KAFKA_CREATE_TOPICS", False)
KAFKA_START_WS_CONSUMER = env_bool("KAFKA_START_WS_CONSUMER", True)
KAFKA_LOCAL_FORWARD_ENABLED = env_bool("KAFKA_LOCAL_FORWARD_ENABLED", False)
KAFKA_LOCAL_FORWARD_BIND = os.getenv("KAFKA_LOCAL_FORWARD_BIND", "127.0.0.1:9092")
KAFKA_LOCAL_FORWARD_TARGET = os.getenv("KAFKA_LOCAL_FORWARD_TARGET", "10.204.131.11:9092")
KAFKA_LOCAL_FORWARD_REQUIRED = env_bool("KAFKA_LOCAL_FORWARD_REQUIRED", True)
KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS = env_int("KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS", 10)

HIVE_SERVER2_IP = os.getenv("HIVE_SERVER2_IP", "10.84.128.48")
HIVE_SERVER2_PORT = env_int("HIVE_SERVER2_PORT", 10000)
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "francisco_jose_simoes")
HIVE_USERNAME = os.getenv("HIVE_USERNAME", "g3")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = env_int("REDIS_PORT", 6379)
APP_CACHE_ENABLED = env_bool("APP_CACHE_ENABLED", False)
ENABLE_NOTIFICATION_POLL = env_bool("ENABLE_NOTIFICATION_POLL", False)


def split_host_port(value: str) -> tuple[str, int]:
    host, port = value.rsplit(":", 1)
    return host, int(port)


def is_tcp_open(host: str, port: int, timeout_seconds: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def wait_for_tcp(host: str, port: int, timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if is_tcp_open(host, port):
            return True
        time.sleep(0.25)
    return is_tcp_open(host, port)


def maybe_start_kafka_local_forward() -> bool:
    if not KAFKA_LOCAL_FORWARD_ENABLED:
        return True

    try:
        bind_host, bind_port = split_host_port(KAFKA_LOCAL_FORWARD_BIND)
        target_host, target_port = split_host_port(KAFKA_LOCAL_FORWARD_TARGET)
    except ValueError:
        logger.warning(
            "Invalid Kafka local forward config: bind=%s target=%s",
            KAFKA_LOCAL_FORWARD_BIND,
            KAFKA_LOCAL_FORWARD_TARGET,
        )
        return False

    if is_tcp_open(bind_host, bind_port):
        logger.info("Kafka local forward already available at %s", KAFKA_LOCAL_FORWARD_BIND)
        return True

    command = [
        "socat",
        f"TCP4-LISTEN:{bind_port},bind={bind_host},reuseaddr,fork",
        f"TCP4:{target_host}:{target_port}",
    ]
    try:
        subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if wait_for_tcp(bind_host, bind_port, KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS):
            logger.info(
                "Started Kafka local forward %s -> %s",
                KAFKA_LOCAL_FORWARD_BIND,
                KAFKA_LOCAL_FORWARD_TARGET,
            )
            return True
        else:
            logger.error(
                "Kafka local forward did not become ready at %s within %s seconds",
                KAFKA_LOCAL_FORWARD_BIND,
                KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS,
            )
            return False
    except FileNotFoundError:
        logger.error("KAFKA_LOCAL_FORWARD_ENABLED=true but socat is not installed in this environment.")
        return False
    except Exception as e:
        logger.error("Failed to start Kafka local forward: %s", e)
        return False


if not maybe_start_kafka_local_forward() and KAFKA_LOCAL_FORWARD_REQUIRED:
    raise RuntimeError(
        "Kafka local forward is required but unavailable. "
        "Install socat or set KAFKA_LOCAL_FORWARD_REQUIRED=false."
    )

# --- WebSocket Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        dead_connections = []
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")
                dead_connections.append(connection)

        for connection in dead_connections:
            self.disconnect(connection)

manager = ConnectionManager()

# --- Kafka Consumer Thread ---
def kafka_worker(loop, servers, topic):
    try:
        from kafka import KafkaConsumer
        logger.info(f"Starting background Kafka Consumer for {topic}...")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            api_version=(0, 10, 1),
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET
        )
        for message in consumer:
            data = message.value
            logger.info(f"Kafka Worker Received: {data}")
            asyncio.run_coroutine_threadsafe(
                manager.broadcast({"type": "kafka_event", "data": data}),  # ← tipo correto
                loop
            )
            logger.info(f"Kafka Worker Received: {data}")
    except Exception as e:
        logger.error(f"Kafka Worker FAILED: {e}")

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    from pyhive import hive
    HIVE_AVAILABLE = True
except ImportError:
    HIVE_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

app = FastAPI(title="Caveman Music API")

@app.middleware("http")
async def log_requests(request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    logger.info(f"Response status: {response.status_code}")
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Kafka: ensure topics exist ---
KAFKA_TOPICS = [KAFKA_TOPIC_PLAY, KAFKA_TOPIC_SKIP, KAFKA_TOPIC_LIKE]

if KAFKA_AVAILABLE and KAFKA_CREATE_TOPICS:
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10, 1))
        existing = admin.list_topics()
        new_topics = [NewTopic(t, 1, 1) for t in KAFKA_TOPICS if t not in existing]
        if new_topics:
            admin.create_topics(new_topics)
            logger.info(f"Created Kafka topics: {[t.name for t in new_topics]}")
        admin.close()
    except Exception as e:
        logger.warning(f"Kafka Admin Error: {e}")
elif KAFKA_AVAILABLE:
    logger.info("Kafka topic creation disabled. Set KAFKA_CREATE_TOPICS=true to enable it.")

# --- Kafka Producer ---
producer = None
if KAFKA_AVAILABLE:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1),
            retries=5
        )
        logger.info("Kafka Producer initialized.")
    except Exception as e:
        logger.error(f"Kafka Producer FAILED: {e}")
else:
    logger.error("kafka-python NOT INSTALLED.")

# --- Redis ---
r_cache = None
if REDIS_AVAILABLE:
    for host in [REDIS_HOST, "host.docker.internal"]:
        try:
            r_cache = redis.Redis(host=host, port=REDIS_PORT, db=0, socket_timeout=1)
            r_cache.ping()
            logger.info(f"Connected to Redis at {host}")
            break
        except Exception:
            r_cache = None

if not r_cache:
    logger.warning("Redis not available. Caching disabled.")

# --- Hive helper ---
def hive_conn():
    return hive.Connection(
        host=HIVE_SERVER2_IP,
        port=HIVE_SERVER2_PORT,
        username=HIVE_USERNAME,
        database=HIVE_DATABASE
    )

# NOTE: correct artist JOIN path in MusicBrainz schema:
# curated_recording.artist_credit
#   → curated_artist_credit_name.artist_credit (intermediary)
#   → curated_artist_credit_name.artist
#   → curated_artist.id
ARTIST_JOIN = """
    JOIN curated_artist_credit_name acn ON r.artist_credit = acn.artist_credit
    JOIN curated_artist a ON acn.artist = a.id
"""


def merge_song_artist_rows(rows, has_score: bool = False, limit: Optional[int] = None) -> List[dict]:
    merged = {}
    for row in rows:
        key = (int(row[0]), str(row[1]))
        artist = str(row[2])
        if key not in merged:
            merged[key] = {
                "id": key[0],
                "name": key[1],
                "artists": [],
            }
            if has_score:
                merged[key]["score"] = float(row[3])
        if artist not in merged[key]["artists"]:
            merged[key]["artists"].append(artist)
        if has_score:
            merged[key]["score"] = max(float(merged[key]["score"]), float(row[3]))

    songs = []
    for song in merged.values():
        item = {
            "id": song["id"],
            "name": song["name"],
            "artist": ", ".join(song["artists"]),
        }
        if has_score:
            item["score"] = song["score"]
        songs.append(item)

    return songs[:limit] if limit else songs

# --- Pydantic Models ---
class PlayEvent(BaseModel):
    user_id: int
    recording_id: int
    duration_ms: Optional[int] = 30000

class SkipEvent(BaseModel):
    user_id: int
    recording_id: int
    position_ms: Optional[int] = 0

class LikeEvent(BaseModel):
    user_id: int
    recording_id: int

class SongInfo(BaseModel):
    id: int
    name: str
    artist: str

class SongInfoWithScore(BaseModel):
    id: int
    name: str
    artist: str
    score: Optional[float] = None

class TrackInfo(SongInfoWithScore):
    position: int
    number: str

class RecordingAlbumResponse(BaseModel):
    release_id: int
    release_name: str
    tracks: List[TrackInfo]

class UserProfile(BaseModel):
    id: int
    username: str
    age: int
    country: str

class UserTopTracksResponse(BaseModel):
    user: UserProfile
    songs: List[SongInfoWithScore]

class RecommendationResponse(BaseModel):
    source: str
    songs: List[SongInfo]

class PlaylistResponse(BaseModel):
    user_id: int
    time_of_day: str
    songs: List[SongInfoWithScore]

class NotificationItem(BaseModel):
    artist_id: int
    artist_name: str
    release_id: int
    release_name: str
    notified_at: str

class SearchResponse(BaseModel):
    query: str
    results: List[SongInfoWithScore]

# ============================================================
# LOGIC
# ============================================================

FALLBACK_USERNAMES = {"guest", "mockuser", "manual"}


def is_real_hive_user(user: UserProfile) -> bool:
    return (
        user.id > 0
        and bool(user.username)
        and user.username.strip().lower() not in FALLBACK_USERNAMES
    )


def get_hive_users(n: int = 20, use_cache: bool = True) -> List[UserProfile]:
    if APP_CACHE_ENABLED and use_cache and r_cache:
        cached = r_cache.get("users:list")
        if cached:
            try:
                logger.info("Users fetched from Redis cache")
                cached_rows = json.loads(cached)
                users = []
                for row in cached_rows:
                    users.append(UserProfile(
                        id=int(row.get("id", row.get("user_id"))),
                        username=str(row["username"]),
                        age=int(row.get("age") or 0),
                        country=str(row.get("country") or "")
                    ))
                if users and all(is_real_hive_user(user) for user in users):
                    return users
                logger.warning("Ignoring fallback or empty users found in Redis cache")
            except Exception as e:
                logger.warning(f"Ignoring invalid Redis users cache: {e}")
            r_cache.delete("users:list")
    if not HIVE_AVAILABLE:
        return [UserProfile(id=1, username="MockUser", age=25, country="PT")]
    try:
        logger.info("Users fetched from Hive")
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT user_id, username, age, country
            FROM curated_users
            WHERE username != 'username' AND user_id IS NOT NULL
            LIMIT {n}
        """)
        users = []
        for r in cursor.fetchall():
            try:
                users.append(UserProfile(id=int(r[0]), username=str(r[1]),
                                         age=int(r[2]) if r[2] else 0, country=str(r[3])))
            except: continue
        if APP_CACHE_ENABLED and r_cache and users and all(is_real_hive_user(user) for user in users):
            r_cache.setex("users:list", 600, json.dumps([u.dict() for u in users]))
        return users
    except Exception as e:
        logger.error(f"Hive user fetch failed: {e}")
        return []


def get_batch_recommendations(user_id: int, n: int = 8) -> dict:
    cache_key = f"recs:{user_id}"

    # 1. Redis (speed layer — ML recs from Spark Streaming)
    if r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            logger.info(f"User {user_id} recs from Redis ML cache")
            return {"source": "redis_ml_cache", "songs": json.loads(cached)}

    if not HIVE_AVAILABLE:
        return {"source": "mock", "songs": [{"id": 101, "name": "Mock Song", "artist": "Mock"}]}

    try:
        conn = hive_conn()
        cursor = conn.cursor()

        # 2. Hive batch layer — top played songs for this user
        cursor.execute(f"""
            SELECT r.id, r.name, a.name
            FROM curated_history h
            JOIN curated_recording r ON h.recording_id = r.id
            {ARTIST_JOIN}
            WHERE h.user_id = {user_id}
            GROUP BY r.id, r.name, a.name
            ORDER BY count(*) DESC
            LIMIT {n * 4}
        """)
        songs = merge_song_artist_rows(cursor.fetchall(), limit=n)

        # 3. Cold start — user has no history
        if not songs:
            logger.info(f"Cold start: user {user_id}, serving popular songs")
            cursor.execute(f"""
                SELECT r.id, r.name, a.name
                FROM curated_recording r
                {ARTIST_JOIN}
                LIMIT {n * 4}
            """)
            songs = merge_song_artist_rows(cursor.fetchall(), limit=n)

        if APP_CACHE_ENABLED and r_cache and songs:
            r_cache.setex(cache_key, 600, json.dumps(songs))

        return {"source": "hive_batch", "songs": songs}
    except Exception as e:
        logger.error(f"Hive recs failed: {e}")
        return {"source": "error", "songs": []}


def get_playlist_by_time(user_id: int, time_of_day: str, n: int = 10) -> List[dict]:
    """
    Uses curated_tod_profile — pre-computed play counts per user/song/time_of_day.
    time_of_day values: morning | afternoon | night  (as stored in feature-engineering)
    """
    cache_key = f"playlist:{user_id}:{time_of_day}"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return json.loads(cached)

    if not HIVE_AVAILABLE:
        return []

    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT r.id, r.name, a.name, tp.play_count
            FROM curated_tod_profile tp
            JOIN curated_recording r ON tp.recording_id = r.id
            {ARTIST_JOIN}
            WHERE tp.user_id = {user_id}
              AND lower(tp.time_of_day) = lower('{time_of_day}')
            ORDER BY tp.play_count DESC
            LIMIT {n * 4}
        """)
        songs = merge_song_artist_rows(cursor.fetchall(), has_score=True, limit=n)
        if APP_CACHE_ENABLED and r_cache and songs:
            r_cache.setex(cache_key, 600, json.dumps(songs))
        return songs
    except Exception as e:
        logger.error(f"Playlist fetch failed: {e}")
        return []


def get_user_friends(user_id: int) -> List[UserProfile]:
    cache_key = f"friends_list:{user_id}"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return [UserProfile(**u) for u in json.loads(cached)]
    if not HIVE_AVAILABLE:
        return []
    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT u.user_id, u.username, u.age, u.country
            FROM curated_friends f
            JOIN curated_users u ON f.friend_id = u.user_id
            WHERE f.user_id = {user_id}
            LIMIT 20
        """)
        friends = []
        for r in cursor.fetchall():
            try:
                friends.append(UserProfile(id=int(r[0]), username=str(r[1]),
                                         age=int(r[2]) if r[2] else 0, country=str(r[3])))
            except: continue
        if APP_CACHE_ENABLED and r_cache and friends:
            r_cache.setex(cache_key, 600, json.dumps([f.dict() for f in friends]))
        return friends
    except Exception as e:
        logger.error(f"Friends list fetch failed: {e}")
        return []

def get_friend_history(user_id: int, n: int = 20) -> List[dict]:
    """
    Uses curated_friend_history — songs played by friends, aggregated.
    """
    cache_key = f"friends:{user_id}"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return json.loads(cached)

    if not HIVE_AVAILABLE:
        return []

    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT r.id, r.name, a.name, fh.friend_plays
            FROM curated_friend_history fh
            JOIN curated_recording r ON fh.recording_id = r.id
            {ARTIST_JOIN}
            WHERE fh.user_id = {user_id}
            ORDER BY fh.friend_plays DESC
            LIMIT {n * 4}
        """)
        songs = merge_song_artist_rows(cursor.fetchall(), has_score=True, limit=n)
        if APP_CACHE_ENABLED and r_cache and songs:
            r_cache.setex(cache_key, 300, json.dumps(songs))
        return songs
    except Exception as e:
        logger.error(f"Friend history fetch failed: {e}")
        return []


def get_notifications(user_id: int) -> List[dict]:
    """
    Uses curated_notifications — new releases from favourite artists.
    """
    cache_key = f"notif:{user_id}"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return json.loads(cached)

    if not HIVE_AVAILABLE:
        return []

    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT n.artist_id, a.name, n.release_id, n.release_name, n.notified_at
            FROM curated_notifications n
            JOIN curated_artist a ON n.artist_id = a.id
            WHERE n.user_id = {user_id}
            ORDER BY n.notified_at DESC
            LIMIT 20
        """)
        notifs = [{"artist_id": int(r[0]), "artist_name": str(r[1]),
                   "release_id": int(r[2]), "release_name": str(r[3]),
                   "notified_at": str(r[4])}
                  for r in cursor.fetchall()]
        if APP_CACHE_ENABLED and r_cache and notifs:
            r_cache.setex(cache_key, 300, json.dumps(notifs))
        return notifs
    except Exception as e:
        logger.error(f"Notifications fetch failed: {e}")
        return []


def search_tracks(query: str, n: int = 20) -> List[dict]:
    """
    Semantic-style search over recording name, artist name, and tags.
    Scoring:
      - exact name match    → weight 3
      - partial name match  → weight 2
      - artist name match   → weight 1
      - tag match           → weight 1
    Results ranked by total score descending.
    For true vector semantic search: replace with FAISS/pgvector + song2vec embeddings.
    """
    cache_key = f"search:{query.lower().strip()}"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return json.loads(cached)

    if not HIVE_AVAILABLE:
        return []

    q = query.lower().strip().replace("'", "''")  # sanitise

    try:
        conn = hive_conn()
        cursor = conn.cursor()

        # Search by track name (exact + partial) and artist name
        cursor.execute(f"""
            SELECT
                r.id,
                r.name,
                a.name AS artist_name,
                CASE
                    WHEN lower(r.name) = '{q}'          THEN 3
                    WHEN lower(r.name) LIKE '{q}%'      THEN 2
                    WHEN lower(r.name) LIKE '%{q}%'     THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN lower(a.name) LIKE '%{q}%'     THEN 1
                    ELSE 0
                END AS score
            FROM curated_recording r
            {ARTIST_JOIN}
            WHERE lower(r.name) LIKE '%{q}%'
               OR lower(a.name) LIKE '%{q}%'
            ORDER BY score DESC
            LIMIT {n * 4}
        """)
        results = merge_song_artist_rows(cursor.fetchall(), has_score=True, limit=n)

        # Enrich with tag matches if results < n
        if len(results) < n:
            existing_ids = {r["id"] for r in results}
            cursor.execute(f"""
                SELECT r.id, r.name, a.name, 1 AS score
                FROM curated_recording_tag rt
                JOIN curated_tag t ON rt.tag = t.id
                JOIN curated_recording r ON rt.recording = r.id
                {ARTIST_JOIN}
                WHERE lower(t.name) LIKE '%{q}%'
                LIMIT {n * 4}
            """)
            for r in merge_song_artist_rows(cursor.fetchall(), has_score=True, limit=n):
                rid = int(r["id"])
                if rid not in existing_ids:
                    results.append(r)
                    existing_ids.add(rid)
                if len(results) >= n:
                    break

        results = sorted(results, key=lambda x: x["score"], reverse=True)[:n]

        if APP_CACHE_ENABLED and r_cache and results:
            r_cache.setex(cache_key, 300, json.dumps(results))

        return results
    except Exception as e:
        logger.error(f"Search failed: {e}")
        return []


def send_kafka_event(topic: str, data: dict):
    if producer:
        try:
            producer.send(topic, value=data)
            producer.flush()
            logger.info(f"Kafka → {topic}: {data}")
        except Exception as e:
            logger.error(f"Kafka send FAILED ({topic}): {e}")
            raise HTTPException(status_code=500, detail="Kafka error")
    else:
        logger.warning(f"Kafka Producer NOT AVAILABLE. Event not sent to {topic}.")


def new_event_id(event_type: str, user_id: int, recording_id: int) -> str:
    return f"{event_type}:{user_id}:{recording_id}:{uuid.uuid4().hex}"


def get_artist_songs(artist_id: int, n: int = 10) -> List[dict]:
    cache_key = f"artist:{artist_id}:songs"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached: return json.loads(cached)
    
    if not HIVE_AVAILABLE: return []

    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT r.id, r.name, a.name, count(h.user_id) as popularity
            FROM curated_recording r
            JOIN curated_artist_credit_name acn ON r.artist_credit = acn.artist_credit
            JOIN curated_artist a ON acn.artist = a.id
            LEFT JOIN curated_history h ON r.id = h.recording_id
            WHERE a.id = {artist_id}
            GROUP BY r.id, r.name, a.name
            ORDER BY popularity DESC
            LIMIT {n * 4}
        """)
        songs = merge_song_artist_rows(cursor.fetchall(), has_score=True, limit=n)
        if APP_CACHE_ENABLED and r_cache and songs:
            r_cache.setex(cache_key, 600, json.dumps(songs))
        return songs
    except Exception as e:
        logger.error(f"Artist songs fetch failed: {e}")
        return []

def get_user_top_tracks(user_id: int, n: int = 5) -> List[dict]:
    cache_key = f"user:{user_id}:top_tracks:{n}"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return json.loads(cached)

    if not HIVE_AVAILABLE:
        return []

    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT r.id, r.name, a.name, count(*) as plays
            FROM curated_history h
            JOIN curated_recording r ON h.recording_id = r.id
            {ARTIST_JOIN}
            WHERE h.user_id = {user_id}
            GROUP BY r.id, r.name, a.name
            ORDER BY plays DESC
            LIMIT {n * 4}
        """)
        songs = merge_song_artist_rows(cursor.fetchall(), has_score=True, limit=n)
        if APP_CACHE_ENABLED and r_cache and songs:
            r_cache.setex(cache_key, 600, json.dumps(songs))
        return songs
    except Exception as e:
        logger.error(f"User top tracks fetch failed: {e}")
        return []

def get_recording_album_tracks(recording_id: int) -> Optional[dict]:
    cache_key = f"recording:{recording_id}:album"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return json.loads(cached)

    if not HIVE_AVAILABLE:
        return None

    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT rel.id, rel.name
            FROM curated_track t
            JOIN curated_medium m ON t.medium = m.id
            JOIN curated_release rel ON m.release = rel.id
            WHERE t.recording = {recording_id}
            ORDER BY rel.id
            LIMIT 1
        """)
        release = cursor.fetchone()
        if not release:
            return None

        release_id = int(release[0])
        cursor.execute(f"""
            SELECT
                r.id,
                COALESCE(t.name, r.name) AS track_name,
                a.name AS artist_name,
                COALESCE(m.position, 0) AS medium_position,
                COALESCE(t.position, 0) AS track_position,
                COALESCE(t.number, CAST(t.position AS STRING)) AS track_number,
                count(h.user_id) AS plays
            FROM curated_track t
            JOIN curated_medium m ON t.medium = m.id
            JOIN curated_recording r ON t.recording = r.id
            {ARTIST_JOIN}
            LEFT JOIN curated_history h ON r.id = h.recording_id
            WHERE m.release = {release_id}
            GROUP BY r.id, COALESCE(t.name, r.name), a.name, COALESCE(m.position, 0),
                     COALESCE(t.position, 0), COALESCE(t.number, CAST(t.position AS STRING))
            ORDER BY medium_position, track_position
            LIMIT 200
        """)

        merged = {}
        ordering = {}
        for row in cursor.fetchall():
            rid = int(row[0])
            name = str(row[1])
            artist = str(row[2])
            medium_position = int(row[3] or 0)
            track_position = int(row[4] or 0)
            track_number = str(row[5] or track_position)
            plays = float(row[6] or 0)
            key = (rid, name)
            ordering[key] = (medium_position, track_position)
            if key not in merged:
                merged[key] = {
                    "id": rid,
                    "name": name,
                    "artist": artist,
                    "position": track_position,
                    "number": track_number,
                    "score": plays,
                }
            elif artist not in merged[key]["artist"].split(", "):
                merged[key]["artist"] = f'{merged[key]["artist"]}, {artist}'

        tracks = [merged[key] for key in sorted(merged, key=lambda item: ordering[item])]
        album = {"release_id": release_id, "release_name": str(release[1]), "tracks": tracks}
        if APP_CACHE_ENABLED and r_cache and tracks:
            r_cache.setex(cache_key, 600, json.dumps(album))
        return album
    except Exception as e:
        logger.error(f"Recording album fetch failed: {e}")
        return None

def build_recommended_queue(user_id: int, current_recording_id: int, n: int = 10) -> List[dict]:
    result = get_batch_recommendations(user_id, max(n + 8, 20))
    seen = {current_recording_id}
    queue = []
    for song in result.get("songs", []):
        sid = int(song["id"])
        if sid in seen:
            continue
        seen.add(sid)
        queue.append(song)
        if len(queue) >= n:
            break
    set_user_queue(user_id, queue)
    return queue

def get_user_queue(user_id: int) -> List[dict]:
    queue_key = f"queue:{user_id}"
    if r_cache:
        items = r_cache.lrange(queue_key, 0, -1)
        if items:
            return [json.loads(i) for i in items]
    return []

def set_user_queue(user_id: int, songs: List[dict]):
    queue_key = f"queue:{user_id}"
    if not r_cache:
        return
    pipe = r_cache.pipeline()
    pipe.delete(queue_key)
    for song in songs[:10]:
        pipe.rpush(queue_key, json.dumps(song))
    pipe.expire(queue_key, 3600)  # expira em 1h
    pipe.execute()

def pop_next_from_queue(user_id: int) -> Optional[dict]:
    queue_key = f"queue:{user_id}"
    if r_cache:
        item = r_cache.lpop(queue_key)
        if item:
            return json.loads(item)
    return None

def get_user_history_home(user_id: int, n: int = 8) -> List[dict]:
    """Histórico recente do user para a Home — Redis primeiro, Hive como fallback."""
    cache_key = f"home:{user_id}"
    if APP_CACHE_ENABLED and r_cache:
        cached = r_cache.get(cache_key)
        if cached:
            return json.loads(cached)

    if not HIVE_AVAILABLE:
        return []

    try:
        conn = hive_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT r.id, r.name, a.name
            FROM curated_history h
            JOIN curated_recording r ON h.recording_id = r.id
            {ARTIST_JOIN}
            WHERE h.user_id = {user_id}
            GROUP BY r.id, r.name, a.name
            ORDER BY count(*) DESC
            LIMIT {n * 4}
        """)
        songs = merge_song_artist_rows(cursor.fetchall(), limit=n)
        if APP_CACHE_ENABLED and r_cache and songs:
            r_cache.setex(cache_key, 600, json.dumps(songs))
        return songs
    except Exception as e:
        logger.error(f"Home history fetch failed: {e}")
        return []
# ============================================================
# ROUTES
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "kafka": producer is not None,
        "hive": HIVE_AVAILABLE,
        "redis": r_cache is not None
    }

@app.get("/users", response_model=List[UserProfile])
def list_users(use_cache: bool = False):
    return get_hive_users(use_cache=use_cache)

# --- Recommendations ---
@app.get("/recommend/{user_id}", response_model=RecommendationResponse)
def get_recommendations(user_id: int, n: int = 8):
    result = get_batch_recommendations(user_id, n)
    return RecommendationResponse(**result)

# --- Playlist by time of day ---
@app.get("/playlist/{user_id}", response_model=PlaylistResponse)
def get_playlist(user_id: int, time_of_day: str = "morning", n: int = 10):
    valid = {"morning", "afternoon", "night"}
    if time_of_day.lower() not in valid:
        raise HTTPException(status_code=400, detail=f"time_of_day must be one of {valid}")
    songs = get_playlist_by_time(user_id, time_of_day, n)
    return PlaylistResponse(user_id=user_id, time_of_day=time_of_day,
                            songs=[SongInfoWithScore(**s) for s in songs])

# --- Friend history ---
@app.get("/friends/{user_id}/history", response_model=List[SongInfoWithScore])
def friend_history(user_id: int, n: int = 20):
    songs = get_friend_history(user_id, n)
    return [SongInfoWithScore(**s) for s in songs]

# --- Friend list ---
@app.get("/friends/{user_id}/list", response_model=List[UserProfile])
def list_friends(user_id: int):
    return get_user_friends(user_id)

# --- Notifications ---
@app.get("/notifications/{user_id}", response_model=List[NotificationItem])
def notifications(user_id: int):
    notifs = get_notifications(user_id)
    return [NotificationItem(**n) for n in notifs]

@app.get("/artist/{artist_id}/songs", response_model=List[SongInfoWithScore])
def artist_songs(artist_id: int, n: int = 10):
    songs = get_artist_songs(artist_id, n)
    return [SongInfoWithScore(**s) for s in songs]

@app.get("/user/{user_id}/top-tracks", response_model=UserTopTracksResponse)
def user_top_tracks(user_id: int, n: int = 5):
    users = [u for u in get_hive_users(n=100) if u.id == user_id]
    if users:
        user = users[0]
    else:
        user = UserProfile(id=user_id, username=f"User {user_id}", age=0, country="")
    songs = get_user_top_tracks(user_id, n)
    return UserTopTracksResponse(user=user, songs=[SongInfoWithScore(**s) for s in songs])

@app.get("/recording/{recording_id}/album", response_model=RecordingAlbumResponse)
def recording_album(recording_id: int):
    album = get_recording_album_tracks(recording_id)
    if not album:
        raise HTTPException(status_code=404, detail="Album not found for recording")
    return RecordingAlbumResponse(**album)

# --- Semantic Search ---
@app.get("/search", response_model=SearchResponse)
def search(q: str, n: int = 20):
    if not q or len(q.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query too short")
    results = search_tracks(q, n)
    return SearchResponse(query=q, results=[SongInfoWithScore(**r) for r in results])

# Fallback for common frontend typo
@app.get("/searchq={q}")
def search_fallback(q: str, n: int = 20):
    logger.warning(f"Fallback search triggered for: {q}")
    return search(q, n)

@app.get("/queue/{user_id}")
def get_queue(user_id: int):
    return get_user_queue(user_id)

@app.post("/queue/{user_id}")
def set_queue(user_id: int, songs: List[SongInfo]):
    set_user_queue(user_id, [s.dict() for s in songs])
    return {"status": "ok", "queued": len(songs)}

@app.post("/queue/{user_id}/recommend-after/{recording_id}", response_model=List[SongInfo])
def recommended_queue_after_play(user_id: int, recording_id: int, n: int = 10):
    songs = build_recommended_queue(user_id, recording_id, n)
    return [SongInfo(**s) for s in songs]

@app.get("/queue/{user_id}/next")
def next_in_queue(user_id: int):
    song = pop_next_from_queue(user_id)
    if not song:
        raise HTTPException(status_code=404, detail="Queue empty")
    return song

@app.get("/home/{user_id}")
def home_feed(user_id: int, n: int = 8):
    return get_batch_recommendations(user_id, n)

# --- Events → Kafka ---
@app.post("/event/play")
def record_play(event: PlayEvent):
    send_kafka_event(KAFKA_TOPIC_PLAY, {
        "event_id": new_event_id("play", event.user_id, event.recording_id),
        "event_type": "play",
        "user_id": event.user_id,
        "recording_id": event.recording_id,
        "ts": time.time(),
        "duration_ms": event.duration_ms,
        "source": "frontend"
    })
    # Invalida cache home para forçar refresh na próxima visita
    if r_cache:
        r_cache.delete(f"home:{event.user_id}")
    return {"status": "event_recorded", "topic": KAFKA_TOPIC_PLAY}

@app.post("/event/skip")
def record_skip(event: SkipEvent):
    send_kafka_event(KAFKA_TOPIC_SKIP, {
        "event_id": new_event_id("skip", event.user_id, event.recording_id),
        "event_type": "skip",
        "user_id": event.user_id,
        "recording_id": event.recording_id,
        "ts": time.time(),
        "position_ms": event.position_ms,
        "source": "frontend"
    })
    return {"status": "event_recorded", "topic": KAFKA_TOPIC_SKIP}

@app.post("/event/like")
def record_like(event: LikeEvent):
    send_kafka_event(KAFKA_TOPIC_LIKE, {
        "event_id": new_event_id("like", event.user_id, event.recording_id),
        "event_type": "like",
        "user_id": event.user_id,
        "recording_id": event.recording_id,
        "ts": time.time(),
        "source": "frontend"
    })
    return {"status": "event_recorded", "topic": KAFKA_TOPIC_LIKE}

@app.post("/broadcast/play-random")
async def broadcast_random_song():
    if not HIVE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Hive not available")
    
    try:
        pool_key = "broadcast:song_pool"
        pool = None

        if APP_CACHE_ENABLED and r_cache:
            cached = r_cache.get(pool_key)
            if cached:
                pool = json.loads(cached)

        if not pool:
            conn = hive_conn()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT r.id, r.name, a.name
                FROM curated_recording r
                {ARTIST_JOIN}
                LIMIT 2000
            """)
            pool = merge_song_artist_rows(cursor.fetchall(), limit=500)
            if APP_CACHE_ENABLED and r_cache and pool:
                r_cache.setex(pool_key, 3600, json.dumps(pool))

        users = get_hive_users()

        # Música diferente para cada user
        assignments = {user.id: random.choice(pool) for user in users}

        for user in users:
            song = assignments[user.id]
            send_kafka_event(KAFKA_TOPIC_PLAY, {
                "event_id": new_event_id("play", user.id, song["id"]),
                "event_type": "play",
                "user_id": user.id,
                "recording_id": song["id"],
                "ts": time.time(),
                "duration_ms": 30000,
                "source": "broadcast"
            })

        
        await manager.broadcast({
            "type": "broadcast_play",
            "assignments": {str(k): v for k, v in assignments.items()}
        })

        return {"status": "broadcasting", "assignments": assignments, "users_notified": len(users)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Broadcast failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def poll_notifications(loop):
    """Verifica de 30 em 30s se há novas notificações para algum user."""
    await asyncio.sleep(15)  # espera inicial
    while True:
        try:
            if not manager.active_connections:
                await asyncio.sleep(30)
                continue

            users = get_hive_users()
            for user in users:
                cache_key = f"notif:{user.id}"
                old_cached = r_cache.get(cache_key) if r_cache else None
                old_ids = set()
                if old_cached:
                    old_ids = {n["release_id"] for n in json.loads(old_cached)}
                
                # Invalida cache para forçar re-fetch
                if r_cache:
                    r_cache.delete(cache_key)
                
                fresh = get_notifications(user.id)
                new_ones = [n for n in fresh if n["release_id"] not in old_ids]
                
                for notif in new_ones:
                    await manager.broadcast({
                        "type": "new_notification",
                        "user_id": user.id,
                        "artist_name": notif["artist_name"],
                        "release_name": notif["release_name"]
                    })
        except Exception as e:
            logger.error(f"Notification poll failed: {e}")
        
        await asyncio.sleep(30)  # verifica a cada 30s        

# --- WebSocket ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    if KAFKA_AVAILABLE and KAFKA_START_WS_CONSUMER:
        loop = asyncio.get_running_loop()
        thread = threading.Thread(
            target=kafka_worker,
            args=(loop, KAFKA_SERVERS, KAFKA_TOPIC_PLAY),
            daemon=True
        )
        thread.start()
    elif KAFKA_AVAILABLE:
        logger.info("Kafka WebSocket consumer disabled. Set KAFKA_START_WS_CONSUMER=true to enable it.")
    if ENABLE_NOTIFICATION_POLL:
        asyncio.create_task(poll_notifications(asyncio.get_running_loop()))
    else:
        logger.info("Notification poll disabled. Set ENABLE_NOTIFICATION_POLL=true to enable it.")


@app.post("/dev/simulate-notification")
async def simulate_notification(user_id: int, artist_name: str = "Radiohead", release_name: str = "New Album 2025"):
    await manager.broadcast({
        "type": "new_notification",
        "user_id": user_id,
        "artist_name": artist_name,
        "release_name": release_name
    })
    return {"status": "simulated", "user_id": user_id, "artist_name": artist_name}
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=API_HOST, port=API_PORT)
