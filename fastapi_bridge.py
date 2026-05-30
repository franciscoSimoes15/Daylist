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
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")

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
            auto_offset_reset='latest'   # ← era 'earliest', volta para 'latest'
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
    logger.info(f"Response status: {response.status_code}")
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Configuration ---
KAFKA_SERVERS        = ["10.204.131.11:9092", "10.84.128.10:9092"]
KAFKA_TOPIC_PLAY     = "music.events.play"
KAFKA_TOPIC_SKIP     = "music.events.skip"
KAFKA_TOPIC_LIKE     = "music.events.like"
HIVE_SERVER2_IP      = os.getenv("HIVE_SERVER2_IP", "10.84.128.48")
HIVE_SERVER2_PORT    = int(os.getenv("HIVE_SERVER2_PORT", 10000))
HIVE_DATABASE        = os.getenv("HIVE_DATABASE", "francisco_jose_simoes")
REDIS_HOST           = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT           = int(os.getenv("REDIS_PORT", 6379))

# --- Kafka: ensure topics exist ---
KAFKA_TOPICS = [KAFKA_TOPIC_PLAY, KAFKA_TOPIC_SKIP, KAFKA_TOPIC_LIKE]

if KAFKA_AVAILABLE:
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
        username='hadoop',
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

class UserProfile(BaseModel):
    id: int
    username: str
    age: int
    country: str

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

def get_hive_users(n: int = 20) -> List[UserProfile]:
    if r_cache:
        cached = r_cache.get("users:list")
        if cached:
            return [UserProfile(**u) for u in json.loads(cached)]
    if not HIVE_AVAILABLE:
        return [UserProfile(id=1, username="MockUser", age=25, country="PT")]
    try:
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
        if r_cache and users:
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
            LIMIT {n}
        """)
        songs = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2])} for r in cursor.fetchall()]

        # 3. Cold start — user has no history
        if not songs:
            logger.info(f"Cold start: user {user_id}, serving popular songs")
            cursor.execute(f"""
                SELECT r.id, r.name, a.name
                FROM curated_recording r
                {ARTIST_JOIN}
                LIMIT {n}
            """)
            songs = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2])} for r in cursor.fetchall()]

        if r_cache and songs:
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
    if r_cache:
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
            LIMIT {n}
        """)
        songs = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2]), "score": float(r[3])}
                 for r in cursor.fetchall()]
        if r_cache and songs:
            r_cache.setex(cache_key, 600, json.dumps(songs))
        return songs
    except Exception as e:
        logger.error(f"Playlist fetch failed: {e}")
        return []


def get_user_friends(user_id: int) -> List[UserProfile]:
    cache_key = f"friends_list:{user_id}"
    if r_cache:
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
        if r_cache and friends:
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
    if r_cache:
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
            LIMIT {n}
        """)
        songs = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2]), "score": float(r[3])}
                 for r in cursor.fetchall()]
        if r_cache and songs:
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
    if r_cache:
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
        if r_cache and notifs:
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
    if r_cache:
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
            LIMIT {n}
        """)
        results = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2]), "score": float(r[3])}
                   for r in cursor.fetchall()]

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
                LIMIT {n}
            """)
            for r in cursor.fetchall():
                rid = int(r[0])
                if rid not in existing_ids:
                    results.append({"id": rid, "name": str(r[1]), "artist": str(r[2]), "score": 1.0})
                    existing_ids.add(rid)
                if len(results) >= n:
                    break

        results = sorted(results, key=lambda x: x["score"], reverse=True)[:n]

        if r_cache and results:
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


def get_artist_songs(artist_id: int, n: int = 10) -> List[dict]:
    cache_key = f"artist:{artist_id}:songs"
    if r_cache:
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
            LIMIT {n}
        """)
        songs = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2]), "score": float(r[3])}
                 for r in cursor.fetchall()]
        if r_cache and songs:
            r_cache.setex(cache_key, 600, json.dumps(songs))
        return songs
    except Exception as e:
        logger.error(f"Artist songs fetch failed: {e}")
        return []

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
    if r_cache:
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
            LIMIT {n}
        """)
        songs = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2])}
                 for r in cursor.fetchall()]
        if r_cache and songs:
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
def list_users():
    return get_hive_users()

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

@app.get("/queue/{user_id}/next")
def next_in_queue(user_id: int):
    song = pop_next_from_queue(user_id)
    if not song:
        raise HTTPException(status_code=404, detail="Queue empty")
    return song

@app.get("/home/{user_id}")
def home_feed(user_id: int, n: int = 8):
    songs = get_user_history_home(user_id, n)
    # Quando um user toca uma música, a cache home é invalidada
    # e na próxima visita mostra o histórico atualizado
    return {"source": "redis_history" if r_cache else "hive", "songs": songs}

# --- Events → Kafka ---
@app.post("/event/play")
def record_play(event: PlayEvent):
    send_kafka_event(KAFKA_TOPIC_PLAY, {
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

        if r_cache:
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
                LIMIT 500
            """)
            pool = [{"id": int(r[0]), "name": str(r[1]), "artist": str(r[2])}
                    for r in cursor.fetchall()]
            if r_cache and pool:
                r_cache.setex(pool_key, 3600, json.dumps(pool))

        users = get_hive_users()

        # Música diferente para cada user
        assignments = {user.id: random.choice(pool) for user in users}

        for user in users:
            song = assignments[user.id]
            send_kafka_event(KAFKA_TOPIC_PLAY, {
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
    if KAFKA_AVAILABLE:
        loop = asyncio.get_running_loop()
        thread = threading.Thread(
            target=kafka_worker,
            args=(loop, KAFKA_SERVERS, KAFKA_TOPIC_PLAY),
            daemon=True
        )
        thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)