"""Inserts a few sample students so you can test. Skip if you already have your own data."""
import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = os.getenv("DB_NAME", "college")
STUDENTS_COLLECTION = os.getenv("STUDENTS_COLLECTION", "students")
ROLL_FIELD = os.getenv("ROLL_FIELD", "rollNo")
NAME_FIELD = os.getenv("NAME_FIELD", "name")
RFID_FIELD = os.getenv("RFID_FIELD", "rfid")

SAMPLE = [
    ("21CS001", "Aarav Reddy", "A1B2C3D4"),
    ("21CS002", "Sneha Rao", "B2C3D4E5"),
    ("21CS003", "Kiran Kumar", "C3D4E5F6"),
    ("21EC014", "Divya Sri", "D4E5F6A7"),
    ("21ME027", "Rahul Varma", "E5F6A7B8"),
]

client = MongoClient(MONGO_URI)
col = client[DB_NAME][STUDENTS_COLLECTION]
for roll, name, uid in SAMPLE:
    col.update_one(
        {ROLL_FIELD: roll},
        {"$set": {ROLL_FIELD: roll, NAME_FIELD: name, RFID_FIELD: uid}},
        upsert=True,
    )
print(f"Seeded {len(SAMPLE)} students into {DB_NAME}.{STUDENTS_COLLECTION}")
client.close()
