
import json
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os

load_dotenv()

# Connect to MongoDB Atlas
client = MongoClient(os.environ["MONGO_URI"])
db = client["college"]
students = db["students"]

# Load the student JSON file from this folder
file_path = Path(__file__).with_name("contacts.json")

with file_path.open("r", encoding="utf-8") as file:
    records = json.load(file)

operations = []
skipped = 0

for record in records:
    roll_no = str(record.get("rollNumber", "")).strip()
    name = str(record.get("name", "")).strip()
    mail = str(record.get("mail", "")).strip()

    if not roll_no or not name:
        skipped += 1
        continue

    # Update existing students by roll number instead of duplicating them.
    # Preserve any RFID UID that has already been registered.
    operations.append(
        UpdateOne(
            {"rollNo": roll_no},
            {
                "$set": {
                    "rollNo": roll_no,
                    "name": name,
                    "mail": mail,
                },
                "$setOnInsert": {"rfid": ""},
            },
            upsert=True,
        )
    )

if operations:
    result = students.bulk_write(operations, ordered=False)
    print("Import completed!")
    print("Records in source file:", len(records))
    print("Inserted:", result.upserted_count)
    print("Updated:", result.modified_count)
    print("Skipped:", skipped)
    print("Total documents now in collection:", students.count_documents({}))
else:
    print("No valid student records found.")

client.close()
