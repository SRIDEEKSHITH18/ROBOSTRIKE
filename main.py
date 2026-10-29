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
from pathlib import Path

from bson import ObjectId
from bson.errors import InvalidId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pymongo import AsyncMongoClient, ReturnDocument

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

BASE_DIR = Path(__file__).parent
db = {}  # filled at startup: db["students"], db["queue"]
# Single-scanner station: an operator selects a student, then the next card tap
# registers that card to the selected student. This state is in-memory.
pending_registration = None
pending_registration_lock = asyncio.Lock()


# ---------------- startup / shutdown ----------------
@asynccontextmanager
async def lifespan(_app: FastAPI):
    client = AsyncMongoClient(MONGO_URI, tz_aware=True)
    database = client[DB_NAME]
    db["students"] = database[STUDENTS_COLLECTION]
    db["queue"] = database["queue"]

    # Indexes keep lookups fast even with a very large student collection
    await db["students"].create_index(ROLL_FIELD)
    await db["students"].create_index(RFID_FIELD, sparse=True)
    await db["queue"].create_index([("status", 1), ("addedAt", 1)])
    await db["queue"].create_index([("rollNo", 1), ("status", 1)])
    print(f"Connected to MongoDB: {DB_NAME}.{STUDENTS_COLLECTION}")
    yield
    await client.close()


app = FastAPI(title="RFID Queue", lifespan=lifespan)
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


async def get_queue() -> list:
    cursor = db["queue"].find({"status": "waiting"}).sort("addedAt", 1)
    return [serialize(d) async for d in cursor]


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
async def rc_add_to_queue(request: Request):
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

    body = await read_body(request)

    roll_no = (
        body.get("rollNumber")
        or body.get("rollNo")
        or body.get("roll_no")
        or body.get("roll")
    )

    if not roll_no:
        return JSONResponse(
            {"ok": False, "error": "A resolved roll number is required"},
            status_code=400,
        )

    status, data = await add_to_queue(roll_no, "rfid")

    return JSONResponse(data, status_code=status)

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

    entry = {
        "rollNo": "OUTSIDER",
        "name": name,
        "role": role,
        "personType": "outsider",
        "source": "manual",
        "status": "waiting",
        "addedAt": now(),
    }
    result = await db["queue"].insert_one(entry)
    entry["_id"] = result.inserted_id
    await broadcast("scan", {
        "ok": True,
        "name": name,
        "rollNo": "OUTSIDER",
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
    body = await read_body(request)
    status = "removed" if body.get("status") == "removed" else "served"
    try:
        oid = ObjectId(entry_id)
    except InvalidId:
        raise HTTPException(400, "Invalid id")
    await db["queue"].update_one({"_id": oid}, {"$set": {"status": status, "doneAt": now()}})
    await broadcast_queue()
    return {"ok": True}


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
