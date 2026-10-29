"""
RFID Queue Server  (FastAPI + uvicorn + MongoDB)

Flow:
  RFID hardware -> POST /api/scan {"rollNo": "..."} or {"uid": "..."}
  -> look up student in MongoDB -> add name to queue
  Website also supports manual roll-no search + add.
  Queue updates are pushed live to every open browser (Server-Sent Events).

Run:
  uvicorn main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import os
import hmac
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from bson import ObjectId
from bson.errors import InvalidId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Header, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pymongo import AsyncMongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from pydantic import BaseModel, Field

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "college")
STUDENTS_COLLECTION = os.getenv("STUDENTS_COLLECTION", "students")
ROLL_FIELD = os.getenv("ROLL_FIELD", "rollNo")
NAME_FIELD = os.getenv("NAME_FIELD", "name")
RFID_FIELD = os.getenv("RFID_FIELD", "rfid")  # only used if hardware sends card UID
DEVICE_API_KEY = os.getenv("DEVICE_API_KEY", "")
# Required shared secret for requests sent by the RC server.
RC_QUEUE_API_KEY = os.getenv("RC_QUEUE_API_KEY", "")
# Secret required by the OpenCV detection client when submitting darts (POST /api/games/darts).
DARTS_API_KEY = os.getenv("DARTS_API_KEY", "")

# Every participant plays each of these once (3 darts each).
ROUND_TYPES = ("static", "linear", "revolving")

BASE_DIR = Path(__file__).parent
db = {}  # filled at startup: students, queue, game_rounds, active_rounds
# Single-scanner station: an operator selects a student, then the next card tap
# registers that card to the selected student. This state is in-memory.
pending_registration = None
pending_registration_lock = asyncio.Lock()
game_state_lock = asyncio.Lock()  # one active dart round at a time


class RoundType(str, Enum):
    static = "static"
    linear = "linear"
    revolving = "revolving"


class RCScanRequest(BaseModel):
    rollNumber: str


class StartRoundRequest(BaseModel):
    rollNumber: str = Field(min_length=1, max_length=80)
    # Optional for compatibility; the backend chooses the next unfinished round.
    roundType: RoundType | None = None


class StartQueuedRoundRequest(BaseModel):
    queueEntryId: str = Field(min_length=1, max_length=80)
    # The backend enforces the participant's next round in sequence.
    roundType: RoundType | None = None


class DartEvent(BaseModel):
    """One detected dart. Its index is assigned by the backend from the active round."""
    canonical_x: float
    canonical_y: float
    radial_distance_px: float = Field(ge=0)
    score: int = Field(ge=0)


class ManualDartScore(BaseModel):
    """Score-only input for the next dart when automatic detection fails."""
    score: int = Field(ge=0)
# ---------------- startup / shutdown ----------------
@asynccontextmanager
async def lifespan(_app: FastAPI):
    client = AsyncMongoClient(MONGO_URI, tz_aware=True)
    database = client[DB_NAME]
    db["students"] = database[STUDENTS_COLLECTION]
    db["queue"] = database["queue"]
    db["game_rounds"] = database["game_rounds"]
    db["active_rounds"] = database["active_rounds"]

    # Indexes keep lookups fast even with a very large student collection
    await db["students"].create_index(ROLL_FIELD)
    await db["students"].create_index(RFID_FIELD, sparse=True)
    await db["queue"].create_index([("status", 1), ("addedAt", 1)])
    await db["queue"].create_index([("rollNo", 1), ("status", 1)])
    await db["game_rounds"].create_index([("rollNo", 1), ("status", 1), ("playedAt", -1)])
    await db["game_rounds"].create_index([("status", 1), ("playedAt", -1)])
    # Safety net: the database itself refuses a second COMPLETED round of the same
    # type for one participant. Legacy rounds (no roundType), in-progress and
    # cancelled rounds are not covered by this partial index.
    try:
        await db["game_rounds"].create_index(
            [("rollNo", 1), ("roundType", 1)],
            unique=True,
            name="uniq_completed_round_type",
            partialFilterExpression={"status": "completed", "roundType": {"$type": "string"}},
        )
    except Exception as exc:  # e.g. pre-existing duplicate data; app still runs with the lock-based check
        print(f"WARNING: could not create unique completed-round-type index: {exc}")
    print(f"Connected to MongoDB: {DB_NAME}.{STUDENTS_COLLECTION}")
    yield
    await client.close()


app = FastAPI(title="Robostrike Queue and Leaderboard API", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


# ---------------- helpers ----------------
def clean_roll(value) -> str:
    return str(value or "").strip().upper()


def now() -> datetime:
    return datetime.now(timezone.utc)


def serialize(doc: dict) -> dict:
    """Make a Mongo document JSON-safe (ObjectId / datetime -> str)."""
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, dict):
            out[k] = serialize(v)
        elif isinstance(v, list):
            out[k] = [serialize(item) if isinstance(item, dict) else item for item in v]
        else:
            out[k] = v
    return out


async def find_student(roll_no: str):
    students = db["students"]
    # 1) Fast exact match (uses index); also tries numeric roll numbers
    candidates = [roll_no, roll_no.lower()]
    if roll_no.isdigit():
        candidates.append(int(roll_no))
    doc = await students.find_one({ROLL_FIELD: {"$in": candidates}})

    # 2) Fallback: case-insensitive match (slower on huge collections)
    if not doc:
        doc = await students.find_one(
            {ROLL_FIELD: {"$regex": f"^{re.escape(roll_no)}$", "$options": "i"}}
        )
    if not doc:
        return None
    return {
        "rollNo": str(doc.get(ROLL_FIELD)),
        "name": doc.get(NAME_FIELD) or "(no name)",
        "raw": serialize(doc),
    }


async def completed_round_types(roll_no: str) -> list:
    """Round types this participant has completed (legacy untyped rounds are ignored)."""
    found = await db["game_rounds"].distinct("roundType", {"rollNo": roll_no, "status": "completed"})
    return [t for t in ROUND_TYPES if t in found]


async def completed_types_by_roll(rolls: list) -> dict:
    """{rollNo: [completed round types]} for many participants in one query."""
    if not rolls:
        return {}
    cursor = await db["game_rounds"].aggregate([
        {"$match": {"rollNo": {"$in": rolls}, "status": "completed"}},
        {"$group": {"_id": "$rollNo", "types": {"$addToSet": "$roundType"}}},
    ])
    out = {}
    for item in await cursor.to_list(length=None):
        types = item.get("types") or []
        out[item["_id"]] = [t for t in ROUND_TYPES if t in types]
    return out


def summarize_rounds(rounds: list) -> dict:
    """Totals for a participant's COMPLETED rounds.

    Legacy rounds without a valid roundType count towards the overall score and
    round/dart counts, but not towards the static/linear/revolving totals."""
    scores = {t: 0 for t in ROUND_TYPES}
    done = set()
    round_scores = []
    total_darts = 0
    for item in rounds:
        score = int(item.get("roundScore") or 0)
        round_scores.append(score)
        total_darts += int(item.get("dartsReceived") or 0)
        rtype = item.get("roundType")
        if rtype in scores:
            scores[rtype] += score
            done.add(rtype)
    total = sum(round_scores)
    completed = [t for t in ROUND_TYPES if t in done]
    return {
        "overallScore": total,
        "totalRounds": len(round_scores),
        "totalDarts": total_darts,
        "highestRoundScore": max(round_scores) if round_scores else 0,
        "averageRoundScore": round(total / len(round_scores), 2) if round_scores else 0,
        "staticScore": scores["static"],
        "linearScore": scores["linear"],
        "revolvingScore": scores["revolving"],
        "completedRoundTypes": completed,
        "remainingRoundTypes": [t for t in ROUND_TYPES if t not in done],
        "isComplete": len(completed) == len(ROUND_TYPES),
    }


async def get_queue() -> list:
    cursor = db["queue"].find({"status": "waiting"}).sort("addedAt", 1)
    items = [serialize(d) async for d in cursor]
    done = await completed_types_by_roll([i["rollNo"] for i in items if i.get("rollNo")])
    for item in items:
        completed = done.get(item.get("rollNo"), [])
        item["completedRoundTypes"] = completed
        item["remainingRoundTypes"] = [t for t in ROUND_TYPES if t not in completed]
        item["nextRoundType"] = next((t for t in ROUND_TYPES if t not in completed), None)
    return items


async def get_stats() -> dict:
    start = now().replace(hour=0, minute=0, second=0, microsecond=0)
    served = await db["queue"].count_documents({"status": "served", "doneAt": {"$gte": start}})
    scans = await db["queue"].count_documents({"addedAt": {"$gte": start}})
    return {"servedToday": served, "scansToday": scans}


# ---------------- live updates (SSE) ----------------
subscribers: set[asyncio.Queue] = set()


def sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


async def broadcast(event: str, data):
    msg = sse(event, data)
    for q in list(subscribers):
        q.put_nowait(msg)


async def broadcast_queue():
    await broadcast("queue", await get_queue())
    await broadcast("stats", await get_stats())


@app.get("/api/events")
async def events(request: Request):
    q: asyncio.Queue = asyncio.Queue()
    subscribers.add(q)

    async def stream():
        try:
            yield sse("queue", await get_queue())
            yield sse("stats", await get_stats())
            while True:
                if await request.is_disconnected():
                    break
                try:
                    yield await asyncio.wait_for(q.get(), timeout=20)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"  # keeps the connection alive
        finally:
            subscribers.discard(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------- core: add a roll no to the queue ----------------
async def add_to_queue(roll_input, source: str):
    roll_no = clean_roll(roll_input)
    if not roll_no:
        return 400, {"ok": False, "error": "Roll number is required"}

    student = await find_student(roll_no)
    if not student:
        await broadcast("scan", {"ok": False, "rollNo": roll_no, "source": source,
                                 "message": f"Roll no {roll_no} not found"})
        return 404, {"ok": False, "error": f"Roll no {roll_no} not found in database"}

    # Don't add the same student twice while still waiting
    existing = await db["queue"].find_one({"rollNo": student["rollNo"], "status": "waiting"})
    if existing:
        await broadcast("scan", {"ok": False, "rollNo": roll_no, "name": student["name"],
                                 "source": source, "message": f"{student['name']} is already in the queue"})
        return 409, {"ok": False, "error": "Already in queue", "entry": serialize(existing)}

    entry = {
        "rollNo": student["rollNo"],
        "name": student["name"],
        "personType": "student",
        "source": source,  # "rfid" or "manual"
        "status": "waiting",
        "nextRoundType": "static",
        "addedAt": now(),
    }
    result = await db["queue"].insert_one(entry)
    entry["_id"] = result.inserted_id

    await broadcast("scan", {"ok": True, "rollNo": entry["rollNo"], "name": entry["name"],
                             "source": source, "message": f"{entry['name']} added to queue"})
    await broadcast_queue()
    return 201, {"ok": True, "entry": serialize(entry)}


async def read_body(request: Request) -> dict:
    """Accept JSON or form data (some hardware sends form-encoded)."""
    ctype = request.headers.get("content-type", "")
    try:
        if "application/json" in ctype:
            return await request.json()
        form = await request.form()
        return dict(form)
    except Exception:
        return {}


# ---------------- routes ----------------
@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "static" / "index.html")


# 1) Called by the RFID hardware
async def register_student_rfid(roll_input: str, uid_input: str):
    """Associate a scanned card UID with an existing student safely."""
    roll_no = clean_roll(roll_input)
    uid = str(uid_input or "").strip().upper().replace(" ", "")
    if not roll_no or not uid:
        return 400, {"ok": False, "error": "Both roll number and RFID UID are required"}
    student = await find_student(roll_no)
    if not student:
        return 404, {"ok": False, "error": f"Roll no {roll_no} not found in database"}

    # A physical card must not be assigned to two different students.
    owner = await db["students"].find_one({RFID_FIELD: {"$in": [uid, uid.lower()]}})
    if owner and str(owner.get(ROLL_FIELD, "")).strip().upper() != student["rollNo"].upper():
        return 409, {"ok": False, "error": "This RFID UID is already registered to another student"}

    await db["students"].update_one(
        {ROLL_FIELD: {"$in": [student["rollNo"], student["rollNo"].lower()]}},
        {"$set": {RFID_FIELD: uid}},
    )
    return 200, {"ok": True, "rollNo": student["rollNo"], "name": student["name"], "rfid": uid}


@app.post("/api/scan")
@app.post("/api/rfid/scan")
@app.post("/api/arcade/scan")
async def scan(request: Request):
    global pending_registration
    if DEVICE_API_KEY and request.headers.get("x-api-key") != DEVICE_API_KEY:
        return JSONResponse({"ok": False, "error": "Invalid device key"}, status_code=401)

    body = await read_body(request)
    # Supported payloads: rollNumber/rollNo, uid/rfid, or roll number + UID together.
    roll = body.get("rollNo") or body.get("rollNumber") or body.get("roll_no") or body.get("roll")
    uid_value = body.get("uid") or body.get("rfid")

    # If the hardware supplies both fields, save the mapping and then queue the student.
    if roll and uid_value:
        status, registration = await register_student_rfid(roll, uid_value)
        if status != 200:
            return JSONResponse(registration, status_code=status)
        status, data = await add_to_queue(registration["rollNo"], "rfid")
        if status in (201, 409):
            data["registration"] = registration
        return JSONResponse(data, status_code=status)

    # If the UI armed a registration for a selected roll number, the next UID-only
    # card tap is bound to that student, then that student is added to the queue.
    if uid_value and not roll:
        uid = str(uid_value).strip().upper().replace(" ", "")
        async with pending_registration_lock:
            target_roll = pending_registration
            if target_roll:
                status, registration = await register_student_rfid(target_roll, uid)
                if status == 200:
                    pending_registration = None
                else:
                    return JSONResponse(registration, status_code=status)
            else:
                registration = None

        if registration:
            status, data = await add_to_queue(registration["rollNo"], "rfid")
            if status in (201, 409):
                data["registration"] = registration
            return JSONResponse(data, status_code=status)

        # Normal operation: UID must already be registered.
        doc = await db["students"].find_one({RFID_FIELD: {"$in": [uid, uid.lower()]}})
        if not doc:
            await broadcast("scan", {"ok": False, "source": "rfid", "message": f"Unknown card {uid}"})
            return JSONResponse({"ok": False, "error": f"No student linked to card {uid}. Search their roll number and arm card registration first."}, status_code=404)
        roll = doc.get(ROLL_FIELD)

    status, data = await add_to_queue(roll, "rfid")
    return JSONResponse(data, status_code=status)


@app.post("/api/rfid/registration/arm")
async def arm_rfid_registration(request: Request):
    """Select a student; the next UID-only scan registers their card and queues them."""
    global pending_registration
    body = await read_body(request)
    roll = clean_roll(body.get("rollNo") or body.get("rollNumber"))
    if not roll:
        return JSONResponse({"ok": False, "error": "Roll number is required"}, status_code=400)
    student = await find_student(roll)
    if not student:
        return JSONResponse({"ok": False, "error": f"Roll no {roll} not found in database"}, status_code=404)
    async with pending_registration_lock:
        pending_registration = student["rollNo"]
    await broadcast("scan", {"ok": True, "source": "manual", "rollNo": student["rollNo"], "name": student["name"], "message": f"Ready to register card for {student['name']}. Tap their ID card now."})
    return {"ok": True, "armed": True, "student": {"rollNo": student["rollNo"], "name": student["name"]}, "message": "Student selected. Tap their ID card at the scanner now."}


@app.delete("/api/rfid/registration/arm")
async def cancel_rfid_registration():
    global pending_registration
    async with pending_registration_lock:
        pending_registration = None
    return {"ok": True, "armed": False}


# 2) Manual search (look up only)
@app.get("/api/students/{roll_no}")
async def search_student(roll_no: str):
    student = await find_student(clean_roll(roll_no))
    if not student:
        return JSONResponse({"ok": False, "error": "Student not found"}, status_code=404)
    in_queue = await db["queue"].find_one({"rollNo": student["rollNo"], "status": "waiting"})
    return {
        "ok": True,
        "student": {"rollNo": student["rollNo"], "name": student["name"]},
        "details": student["raw"],
        "inQueue": bool(in_queue),
    }


# 3) Manual add
@app.post("/api/queue")
async def manual_add(request: Request):
    body = await read_body(request)
    status, data = await add_to_queue(body.get("rollNo"), "manual")
    return JSONResponse(data, status_code=status)

@app.post("/api/integrations/rc/queue")
async def rc_add_to_queue(
    payload: RCScanRequest,
    request: Request,
):
    if not RC_QUEUE_API_KEY:
        return JSONResponse(
            {"ok": False, "error": "RC queue integration is not configured"},
            status_code=503,
        )

    supplied_key = request.headers.get("x-api-key", "")

    if not hmac.compare_digest(supplied_key, RC_QUEUE_API_KEY):
        return JSONResponse(
            {"ok": False, "error": "Invalid RC integration key"},
            status_code=401,
        )

    roll_no = payload.rollNumber.strip()

    if not roll_no:
        return JSONResponse(
            {"ok": False, "error": "A resolved roll number is required"},
            status_code=400,
        )

    status, data = await add_to_queue(roll_no, "rfid")

    return JSONResponse(content=data, status_code=status)

# ---------------- scoring / leaderboard ----------------
def check_darts_api_key(supplied_key: str):
    if not DARTS_API_KEY:
        return JSONResponse(
            {"ok": False, "error": "Darts API is not configured; set DARTS_API_KEY"},
            status_code=503,
        )
    if not hmac.compare_digest(supplied_key, DARTS_API_KEY):
        return JSONResponse({"ok": False, "error": "Invalid darts API key"}, status_code=401)
    return None


def _score_for_type(round_type: str) -> dict:
    """$group accumulator: sum of roundScore for one round type (legacy/untyped rounds give 0)."""
    return {"$sum": {"$cond": [{"$eq": ["$roundType", round_type]}, "$roundScore", 0]}}


async def build_leaderboard(limit: int = 20, offset: int = 0) -> dict:
    """Rank players by cumulative score from completed rounds only."""
    pipeline = [
        {"$match": {"status": "completed"}},
        {"$group": {
            "_id": "$rollNo",
            "playerName": {"$first": "$playerName"},
            "personType": {"$first": {"$ifNull": ["$personType", "student"]}},
            "overallScore": {"$sum": "$roundScore"},
            "staticScore": _score_for_type("static"),
            "linearScore": _score_for_type("linear"),
            "revolvingScore": _score_for_type("revolving"),
            "roundTypes": {"$addToSet": "$roundType"},
            "totalRounds": {"$sum": 1},
            "highestRoundScore": {"$max": "$roundScore"},
            "averageRoundScore": {"$avg": "$roundScore"},
            "totalDarts": {"$sum": "$dartsReceived"},
        }},
        {"$sort": {"overallScore": -1, "highestRoundScore": -1, "_id": 1}},
        {"$facet": {
            "players": [{"$skip": offset}, {"$limit": limit}],
            "metadata": [{"$count": "totalPlayers"}],
        }},
    ]
    cursor = await db["game_rounds"].aggregate(pipeline)
    result = await cursor.to_list(length=1)
    facet = result[0] if result else {"players": [], "metadata": []}
    players = []
    for index, item in enumerate(facet.get("players", []), start=offset + 1):
        types = item.get("roundTypes") or []
        completed_types = [t for t in ROUND_TYPES if t in types]
        players.append({
            "rank": index,
            "rollNo": item["_id"],
            "playerName": item.get("playerName") or "(no name)",
            "personType": item.get("personType", "student"),
            "staticScore": int(item.get("staticScore") or 0),
            "linearScore": int(item.get("linearScore") or 0),
            "revolvingScore": int(item.get("revolvingScore") or 0),
            "overallScore": int(item.get("overallScore") or 0),
            "completedRoundTypes": completed_types,
            "isComplete": len(completed_types) == len(ROUND_TYPES),
            "totalRounds": int(item.get("totalRounds") or 0),
            "totalDarts": int(item.get("totalDarts") or 0),
            "highestRoundScore": int(item.get("highestRoundScore") or 0),
            "averageRoundScore": round(float(item.get("averageRoundScore") or 0), 2),
        })
    metadata = facet.get("metadata", [])
    total_players = int(metadata[0]["totalPlayers"]) if metadata else 0
    return {
        "ok": True,
        "rankingMetric": "overallScore",
        "totalPlayers": total_players,
        "limit": limit,
        "offset": offset,
        "players": players,
    }


async def _start_round_for_player(roll_no: str, player_name: str, round_type: str, queue_entry: dict | None = None):
    """Create one active round, optionally taking a specific waiting queue entry."""
    async with game_state_lock:
        active = await db["active_rounds"].find_one({"_id": "default"})
        if active:
            active_round = await db["game_rounds"].find_one({"_id": active.get("roundId")})
            if active_round and active_round.get("status") == "in_progress":
                return JSONResponse({
                    "ok": False,
                    "error": "A round is already in progress",
                    "activeRound": {
                        "roundId": str(active_round["_id"]),
                        "rollNumber": active_round["rollNo"],
                        "playerName": active_round["playerName"],
                        "roundType": active_round.get("roundType"),
                        "dartsReceived": active_round.get("dartsReceived", 0),
                        "queueEntryId": str(active_round.get("queueEntryId")) if active_round.get("queueEntryId") else None,
                    },
                }, status_code=409)
            await db["active_rounds"].delete_one({"_id": "default"})

        # Enforce the sequence Static -> Linear -> Revolving. Queue entries
        # always use the next unfinished round, so the operator never reselects it.
        done_types = await completed_round_types(roll_no)
        next_type = next((t for t in ROUND_TYPES if t not in done_types), None)
        if queue_entry is not None:
            round_type = next_type
        elif round_type is None:
            round_type = next_type
        if round_type is None:
            return JSONResponse({
                "ok": False,
                "error": f"{player_name} has already completed all three round types.",
                "completedRoundTypes": done_types,
            }, status_code=409)
        if round_type in done_types:
            if len(done_types) == len(ROUND_TYPES):
                message = f"{player_name} has already completed all three round types."
            else:
                message = f"{player_name} has already completed the {round_type.title()} round. Choose a different round type."
            return JSONResponse({
                "ok": False,
                "error": message,
                "completedRoundTypes": done_types,
            }, status_code=409)

        # If this player came from the queue, atomically claim that exact waiting entry.
        queue_entry_id = None
        if queue_entry:
            queue_entry_id = queue_entry["_id"]
            claimed = await db["queue"].update_one(
                {"_id": queue_entry_id, "status": "waiting"},
                {"$set": {"status": "playing", "startedAt": now()}},
            )
            if claimed.modified_count != 1:
                return JSONResponse({
                    "ok": False,
                    "error": "This queue entry is no longer waiting. Refresh the queue and select again.",
                }, status_code=409)

        previous_rounds = await db["game_rounds"].count_documents({
            "rollNo": roll_no, "status": "completed"
        })
        round_doc = {
            "rollNo": roll_no,
            "playerName": player_name,
            "personType": (queue_entry.get("personType") or "student") if queue_entry else "student",
            "roundNumber": previous_rounds + 1,
            "roundType": round_type,
            "darts": [],
            "dartsReceived": 0,
            "roundScore": 0,
            "status": "in_progress",
            "playedAt": now(),
            "completedAt": None,
        }
        if queue_entry_id is not None:
            round_doc["queueEntryId"] = queue_entry_id
        result = await db["game_rounds"].insert_one(round_doc)
        round_id = result.inserted_id
        await db["active_rounds"].replace_one(
            {"_id": "default"},
            {"_id": "default", "roundId": round_id, "rollNo": roll_no, "roundType": round_type,
             "queueEntryId": queue_entry_id, "startedAt": now()},
            upsert=True,
        )

    if queue_entry_id is not None:
        await broadcast_queue()

    return JSONResponse({
        "ok": True,
        "roundId": str(round_id),
        "rollNumber": roll_no,
        "playerName": player_name,
        "personType": (queue_entry.get("personType") or "student") if queue_entry else "student",
        "roundNumber": previous_rounds + 1,
        "roundType": round_type,
        "completedRoundTypes": done_types,
        "dartsReceived": 0,
        "dartsRemaining": 3,
        "status": "in_progress",
        "queueEntryId": str(queue_entry_id) if queue_entry_id is not None else None,
    }, status_code=201)


@app.post("/api/games/rounds/start")
async def start_game_round(payload: StartRoundRequest):
    """Start a round by roll number; if the student is queued, claim their queue entry."""
    roll_no = clean_roll(payload.rollNumber)
    if not roll_no:
        return JSONResponse({"ok": False, "error": "Roll number is required"}, status_code=400)

    student = await find_student(roll_no)
    if not student:
        return JSONResponse({"ok": False, "error": f"Roll no {roll_no} not found in database"}, status_code=404)

    queue_entry = await db["queue"].find_one(
        {"rollNo": student["rollNo"], "status": "waiting"},
        sort=[("addedAt", 1)],
    )
    selected_type = payload.roundType.value if payload.roundType else None
    return await _start_round_for_player(student["rollNo"], student["name"], selected_type, queue_entry)


@app.post("/api/games/rounds/start-from-queue")
async def start_game_round_from_queue(payload: StartQueuedRoundRequest):
    """Start the exact participant selected by the operator from the waiting queue."""
    round_type = payload.roundType.value if payload.roundType else None
    try:
        queue_entry_oid = ObjectId(payload.queueEntryId)
    except InvalidId:
        return JSONResponse({"ok": False, "error": "Invalid queueEntryId"}, status_code=400)

    queue_entry = await db["queue"].find_one({"_id": queue_entry_oid, "status": "waiting"})
    if not queue_entry:
        return JSONResponse({
            "ok": False,
            "error": "Queue entry not found or is no longer waiting. Refresh the queue and try again.",
        }, status_code=404)

    # Outsiders do not have a student-database record. Give each outsider queue
    # entry a stable, unique scoring identity so different visitors never share scores.
    is_outsider = (
        queue_entry.get("personType") == "outsider"
        or clean_roll(queue_entry.get("rollNo")) == "OUTSIDER"
        or clean_roll(queue_entry.get("rollNo")).startswith("OUT-")
    )
    if is_outsider:
        outsider_id = f"OUT-{str(queue_entry['_id']).upper()}"
        current_roll = clean_roll(queue_entry.get("rollNo"))
        if current_roll == "OUTSIDER" or not current_roll.startswith("OUT-"):
            # Migrate older queue records that used the shared literal OUTSIDER.
            migrated = await db["queue"].update_one(
                {"_id": queue_entry["_id"], "status": "waiting"},
                {"$set": {"rollNo": outsider_id, "participantId": outsider_id}},
            )
            if migrated.modified_count != 1:
                return JSONResponse({
                    "ok": False,
                    "error": "Could not assign a unique identity to this outsider. Refresh the queue and try again.",
                }, status_code=409)
            queue_entry["rollNo"] = outsider_id
            queue_entry["participantId"] = outsider_id
        else:
            outsider_id = current_roll
            # Ensure records created before participantId was added are upgraded too.
            await db["queue"].update_one(
                {"_id": queue_entry["_id"], "status": "waiting"},
                {"$set": {"participantId": outsider_id}},
            )
            queue_entry["participantId"] = outsider_id

        # Use the unique per-entry identity as the scoring key and the supplied visitor name.
        return await _start_round_for_player(
            outsider_id,
            queue_entry.get("name") or "Guest",
            round_type,
            queue_entry,
        )

    student = await find_student(clean_roll(queue_entry.get("rollNo")))
    if not student:
        return JSONResponse({"ok": False, "error": "The selected queue participant is not in the students database"}, status_code=404)

    # Use the exact selected queue entry; do not silently select the first matching entry.
    return await _start_round_for_player(student["rollNo"], queue_entry.get("name") or student["name"], round_type, queue_entry)


async def _record_dart_for_active_round(dart_fields: dict, source: str):
    """Record the next dart for the active round, regardless of input source."""
    summary = None
    queue_status = None
    async with game_state_lock:
        active = await db["active_rounds"].find_one({"_id": "default"})
        if not active:
            return JSONResponse({"ok": False, "error": "No active round. Start a round first."}, status_code=409)

        round_doc = await db["game_rounds"].find_one(
            {"_id": active.get("roundId"), "status": "in_progress"}
        )
        if not round_doc:
            await db["active_rounds"].delete_one({"_id": "default"})
            return JSONResponse(
                {"ok": False, "error": "Active round was not found; start a new round"},
                status_code=409,
            )

        round_type = round_doc.get("roundType")
        if round_type not in ROUND_TYPES:
            # Round created before round types existed: it cannot be completed untyped.
            return JSONResponse({
                "ok": False,
                "error": "The active round has no round type. Cancel it and start a new round with a round type selected.",
            }, status_code=409)

        # Determine the next index from persisted round data, never from the request.
        existing_darts = list(round_doc.get("darts", []))
        expected_index = len(existing_darts) + 1
        stored_count = int(round_doc.get("dartsReceived", len(existing_darts)))

        if stored_count != len(existing_darts):
            return JSONResponse({
                "ok": False,
                "error": "Round data is inconsistent: dartsReceived does not match the stored darts. Please inspect this round before continuing.",
                "dartsReceived": stored_count,
                "storedDarts": len(existing_darts),
            }, status_code=409)

        if expected_index > 3:
            return JSONResponse({
                "ok": False,
                "error": "This round already contains three darts.",
                "dartsReceived": len(existing_darts),
            }, status_code=409)

        dart = dict(dart_fields)
        dart["dart_index"] = expected_index
        dart["source"] = source
        dart["receivedAt"] = now()
        new_darts = existing_darts + [dart]
        darts_received = len(new_darts)
        round_score = sum(int(item["score"]) for item in new_darts)
        update_fields = {"dartsReceived": darts_received}
        if darts_received == 3:
            update_fields.update({
                "status": "completed",
                "roundScore": round_score,
                "completedAt": now(),
            })

        try:
            update_result = await db["game_rounds"].update_one(
                {"_id": round_doc["_id"], "status": "in_progress", "dartsReceived": stored_count},
                {"$push": {"darts": dart}, "$set": update_fields},
            )
        except DuplicateKeyError:
            return JSONResponse({
                "ok": False,
                "error": f"This participant already has a completed {round_type.title()} round. Cancel this round and choose a different round type.",
            }, status_code=409)
        if update_result.modified_count != 1:
            return JSONResponse({
                "ok": False,
                "error": "The round changed while recording this dart. Please check the round state before retrying.",
            }, status_code=409)

        completed = darts_received == 3
        if completed:
            await db["active_rounds"].delete_one(
                {"_id": "default", "roundId": round_doc["_id"]}
            )
            all_rounds = await db["game_rounds"].find(
                {"rollNo": round_doc["rollNo"], "status": "completed"}
            ).to_list(length=None)
            summary = summarize_rounds(all_rounds)
            if round_doc.get("queueEntryId"):
                if summary["isComplete"]:
                    # All of Static, Linear and Revolving are done.
                    await db["queue"].update_one(
                        {"_id": round_doc["queueEntryId"], "status": "playing"},
                        {"$set": {"status": "served", "doneAt": now(), "nextRoundType": None}},
                    )
                    queue_status = "served"
                else:
                    # Keep the same queue entry and move it to its next round stage.
                    next_type = next((t for t in ROUND_TYPES if t not in summary["completedRoundTypes"]), None)
                    await db["queue"].update_one(
                        {"_id": round_doc["queueEntryId"], "status": "playing"},
                        {"$set": {"status": "waiting", "nextRoundType": next_type, "lastCompletedRoundType": round_type},
                         "$unset": {"startedAt": ""}},
                    )
                    queue_status = "waiting"

    if completed and round_doc.get("queueEntryId"):
        await broadcast_queue()

    response = {
        "ok": True,
        "roundId": str(round_doc["_id"]),
        "rollNumber": round_doc["rollNo"],
        "playerName": round_doc["playerName"],
        "personType": round_doc.get("personType", "student"),
        "roundType": round_type,
        "dartIndex": expected_index,
        "dartSource": source,
        "dartsReceived": darts_received,
        "dartsRemaining": 3 - darts_received,
        "roundCompleted": completed,
    }
    if completed:
        response["roundScore"] = round_score
        response["queueStatus"] = queue_status
        response["overallScore"] = summary["overallScore"]
        response["totalRounds"] = summary["totalRounds"]
        for key in ("staticScore", "linearScore", "revolvingScore",
                    "completedRoundTypes", "remainingRoundTypes", "isComplete"):
            response[key] = summary[key]
        await broadcast("leaderboard", await build_leaderboard(limit=20, offset=0))

    return JSONResponse(response, status_code=201 if completed else 200)


@app.post("/api/games/darts")
async def receive_dart(
    payload: DartEvent,
    x_api_key: str = Header(..., alias="x-api-key"),
):
    """Record a dart detected by OpenCV; its index is assigned by the backend."""
    auth_error = check_darts_api_key(x_api_key)
    if auth_error:
        return auth_error

    return await _record_dart_for_active_round(payload.model_dump(), source="opencv")


@app.post("/api/games/darts/manual")
async def receive_manual_dart(payload: ManualDartScore):
    """Manually record only the score for the next dart in the active round."""
    # Manual entries intentionally contain no fabricated coordinates.
    manual_dart = {
        "canonical_x": None,
        "canonical_y": None,
        "radial_distance_px": None,
        "score": payload.score,
    }
    return await _record_dart_for_active_round(manual_dart, source="manual")


@app.post("/api/games/rounds/cancel")
async def cancel_game_round():
    """Cancel an unfinished round, useful if a manual test stops before dart 3."""
    async with game_state_lock:
        active = await db["active_rounds"].find_one({"_id": "default"})
        if not active:
            return JSONResponse(
                {"ok": False, "error": "There is no active round to cancel"},
                status_code=404,
            )

        round_doc = await db["game_rounds"].find_one({"_id": active.get("roundId")})
        if not round_doc or round_doc.get("status") != "in_progress":
            await db["active_rounds"].delete_one({"_id": "default"})
            return JSONResponse(
                {"ok": False, "error": "Active round was not in progress; cleared active-round state"},
                status_code=409,
            )

        await db["game_rounds"].update_one(
            {"_id": round_doc["_id"], "status": "in_progress"},
            {"$set": {"status": "cancelled", "cancelledAt": now()}},
        )
        await db["active_rounds"].delete_one(
            {"_id": "default", "roundId": round_doc["_id"]}
        )
        if round_doc.get("queueEntryId"):
            await db["queue"].update_one(
                {"_id": round_doc["queueEntryId"], "status": "playing"},
                {"$set": {"status": "waiting"}, "$unset": {"startedAt": ""}},
            )

    if round_doc.get("queueEntryId"):
        await broadcast_queue()

    return {
        "ok": True,
        "roundId": str(round_doc["_id"]),
        "rollNumber": round_doc["rollNo"],
        "roundType": round_doc.get("roundType"),
        "status": "cancelled",
        "dartsReceived": int(round_doc.get("dartsReceived", 0)),
    }


@app.get("/api/leaderboard")
async def get_leaderboard(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    return await build_leaderboard(limit=limit, offset=offset)


@app.get("/api/players/{roll_no}/scorecard")
async def get_player_scorecard(roll_no: str):
    normalized_roll = clean_roll(roll_no)
    student = await find_student(normalized_roll)
    if not student:
        # Allow a scorecard to remain available even if the source student record was removed.
        # (This is also the path for outsiders, whose OUT-... IDs are not in the students collection.)
        any_round = await db["game_rounds"].find_one({"rollNo": normalized_roll, "status": "completed"})
        if not any_round:
            return JSONResponse({"ok": False, "error": "Player not found"}, status_code=404)
        player_name = any_round.get("playerName") or "(no name)"
        person_type = any_round.get("personType", "student")
    else:
        player_name = student["name"]
        person_type = "student"

    rounds = await db["game_rounds"].find(
        {"rollNo": normalized_roll, "status": "completed"}
    ).sort("playedAt", -1).to_list(length=None)

    summary = summarize_rounds(rounds)
    recent_rounds = []
    for item in rounds[:10]:
        recent_rounds.append({
            "roundId": str(item["_id"]),
            "roundNumber": item.get("roundNumber"),
            "roundType": item.get("roundType") if item.get("roundType") in ROUND_TYPES else None,
            "roundScore": int(item.get("roundScore") or 0),
            "dartsReceived": int(item.get("dartsReceived") or 0),
            "playedAt": item.get("playedAt").isoformat() if item.get("playedAt") else None,
            "completedAt": item.get("completedAt").isoformat() if item.get("completedAt") else None,
            "darts": [
                {key: (value.isoformat() if isinstance(value, datetime) else value)
                 for key, value in dart.items() if key != "_id"}
                for dart in item.get("darts", [])
            ],
        })
    return {
        "ok": True,
        "player": {"rollNo": normalized_roll, "playerName": player_name, "personType": person_type},
        "stats": {"gamesPlayed": summary["totalRounds"], **summary},
        "recentRounds": recent_rounds,
    }


# 4) Manual override for visitors / outsiders (no student database record required)
@app.post("/api/queue/outsider")
async def add_outsider(request: Request):
    body = await read_body(request)
    name = str(body.get("name") or "").strip()
    role = str(body.get("role") or "").strip()

    if not name:
        return JSONResponse({"ok": False, "error": "Outsider name is required"}, status_code=400)
    if not role:
        return JSONResponse({"ok": False, "error": "Outsider role is required"}, status_code=400)
    if len(name) > 120 or len(role) > 80:
        return JSONResponse({"ok": False, "error": "Name or role is too long"}, status_code=400)

    # Each outsider gets a unique participant key; never group every visitor under
    # the same roll number in round history or leaderboard aggregates.
    entry_id = ObjectId()
    outsider_id = f"OUT-{str(entry_id).upper()}"
    entry = {
        "_id": entry_id,
        "rollNo": outsider_id,
        "participantId": outsider_id,
        "name": name,
        "role": role,
        "personType": "outsider",
        "source": "manual",
        "status": "waiting",
        "nextRoundType": "static",
        "addedAt": now(),
    }
    await db["queue"].insert_one(entry)
    await broadcast("scan", {
        "ok": True,
        "name": name,
        "rollNo": outsider_id,
        "personType": "outsider",
        "source": "manual",
        "message": f"Outsider {name} ({role}) added to queue",
    })
    await broadcast_queue()
    return JSONResponse({"ok": True, "entry": serialize(entry)}, status_code=201)


# 5) Current queue
@app.get("/api/queue")
async def queue_list():
    return await get_queue()


# 6) Serve next (first in line)
@app.post("/api/queue/next")
async def serve_next():
    first = await db["queue"].find_one_and_update(
        {"status": "waiting"},
        {"$set": {"status": "served", "doneAt": now()}},
        sort=[("addedAt", 1)],
        return_document=ReturnDocument.AFTER,
    )
    await broadcast_queue()
    return {"ok": True, "served": serialize(first) if first else None}


# 7) Mark one entry served / removed
@app.patch("/api/queue/{entry_id}")
async def update_entry(entry_id: str, request: Request):
    """Remove/serve a queue entry. Removing a participant also cancels their active round."""
    body = await read_body(request)
    status = "removed" if body.get("status") == "removed" else "served"
    try:
        oid = ObjectId(entry_id)
    except InvalidId:
        raise HTTPException(400, "Invalid id")

    entry = await db["queue"].find_one({"_id": oid})
    if not entry:
        raise HTTPException(404, "Queue entry not found")

    if status == "removed":
        async with game_state_lock:
            active = await db["active_rounds"].find_one({"_id": "default"})
            if active:
                active_round = await db["game_rounds"].find_one(
                    {"_id": active.get("roundId"), "status": "in_progress"}
                )
                if active_round and active_round.get("queueEntryId") == oid:
                    await db["game_rounds"].update_one(
                        {"_id": active_round["_id"], "status": "in_progress"},
                        {"$set": {"status": "cancelled", "cancelledAt": now(), "cancelReason": "participant_removed"}},
                    )
                    await db["active_rounds"].delete_one(
                        {"_id": "default", "roundId": active_round["_id"]}
                    )

    await db["queue"].update_one(
        {"_id": oid},
        {"$set": {"status": status, "doneAt": now(), "nextRoundType": None}},
        )
    await broadcast_queue()
    return {"ok": True, "status": status}


# 8) Clear the whole queue
@app.delete("/api/queue")
async def clear_queue():
    await db["queue"].update_many(
        {"status": "waiting"}, {"$set": {"status": "removed", "doneAt": now()}}
    )
    await broadcast_queue()
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))