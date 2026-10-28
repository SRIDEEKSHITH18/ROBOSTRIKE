# RFID Queue — FastAPI + uvicorn

Student scans RFID card → hardware sends roll no (or card UID) → server looks it up in MongoDB → name is added to a live queue on a robotics-style control panel. Staff can also type a roll no manually.

## Project structure

```
rfid-queue-py/
├── main.py                         # FastAPI app (API + live updates)
├── static/index.html               # The website (robotics HUD theme)
├── seed.py                         # Optional: adds 5 sample students
├── hardware/esp32_rfid_scanner.ino # ESP32 + MFRC522 reader sketch
├── requirements.txt
└── .env.example                    # Settings (copy to .env)
```

## Setup

1. Python 3.10+ and a running MongoDB (local or Atlas).
2. Install:
   ```bash
   python -m venv venv
   venv\Scripts\activate           # Windows
   source venv/bin/activate        # Linux / Mac
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and edit:
   - `MONGO_URI`, `DB_NAME` → your database
   - `STUDENTS_COLLECTION`, `ROLL_FIELD`, `NAME_FIELD` → match your existing data
     (e.g. docs like `{ roll_number: "21CS001", student_name: "..." }` →
     `ROLL_FIELD=roll_number`, `NAME_FIELD=student_name`)
   - `DEVICE_API_KEY` → any secret; the hardware sends the same value
4. (Optional) `python seed.py` to add sample students.
5. Run:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```
   Open http://localhost:8000 (or http://<your-pc-ip>:8000 from other devices).

   During development add `--reload`. Use **one worker only** (don't pass `--workers 4`),
   because live updates are kept in memory of a single process.

## Hardware → server

```
POST http://<server-ip>:8000/api/scan
Header: x-api-key: <DEVICE_API_KEY>
Body (JSON):  { "rollNo": "21CS001" }   ← if hardware gives roll no
         or   { "uid": "A1B2C3D4" }     ← if hardware gives card UID only
```

For `uid`, each student document needs the card UID in the `RFID_FIELD` (default `rfid`),
e.g. `{ rollNo: "21CS001", name: "Aarav", rfid: "A1B2C3D4" }`.

Responses: `201` added · `404` not found · `409` already in queue · `401` bad key.

Test without hardware:
```bash
curl -X POST http://localhost:8000/api/scan -H "Content-Type: application/json" -H "x-api-key: change-me" -d "{\"rollNo\":\"21CS001\"}"
```

**USB RFID reader?** Most act as a keyboard: click the roll-no box and scan. It types the
value and presses Enter to search.

## Running it permanently (Linux server / Raspberry Pi)

`/etc/systemd/system/rfid-queue.service`:
```ini
[Unit]
Description=RFID Queue
After=network.target

[Service]
WorkingDirectory=/home/pi/rfid-queue-py
ExecStart=/home/pi/rfid-queue-py/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```
Then `sudo systemctl enable --now rfid-queue`.

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/scan` | Hardware scan (needs API key) |
| GET | `/api/students/{rollNo}` | Look up a student |
| POST | `/api/queue` | Add by roll no (manual) |
| GET | `/api/queue` | Current waiting queue |
| POST | `/api/queue/next` | Serve the first student |
| PATCH | `/api/queue/{id}` | `{"status":"served"}` or `{"status":"removed"}` |
| DELETE | `/api/queue` | Clear queue |
| GET | `/api/events` | Live updates stream (used by the page) |

Interactive API docs: http://localhost:8000/docs
