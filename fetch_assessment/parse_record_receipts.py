import psycopg2
import json
import os
from dotenv import load_dotenv
import datetime


# Database connection details
load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Function to connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to PostgreSQL!")
        return conn
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None

def insert_receipt(json_file):
    conn = connect_db()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        #Load JSON file
        with open(json_file, "r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    data = json.loads(line.strip(""))

                    # Extract necessary fields
                    receiptId = str(data["_id"]["$oid"])
                    bonusPointsEarned = data.get("bonusPointsEarned",None)
                    bonusPointsEarnedReason = data.get("bonusPointsEarnedReason",None)
                    createdate_record = data.get("createDate").get("$date")
                    createDate = datetime.datetime.fromtimestamp(createdate_record / 1000.0)
                    datescanned_record = data.get("dateScanned").get("$date")
                    dateScanned = datetime.datetime.fromtimestamp(datescanned_record / 1000.0)
                    finished_date_record = data.get("finishedDate").get("$date")
                    finishedDate = datetime.datetime.fromtimestamp(finished_date_record / 1000.0)
                    modifydate_record = data.get("modifyDate").get("$date")
                    modifyDate = datetime.datetime.fromtimestamp(modifydate_record / 1000.0)
                    pointsAwardeddate_record = data.get("pointsAwardedDate").get("$date")
                    pointsAwardedDate = datetime.datetime.fromtimestamp(pointsAwardeddate_record / 1000.0)
                    pointsEarned = data.get("pointsEarned",None)
                    purchasedate_record = data.get("purchaseDate").get("$date")
                    purchaseDate = datetime.datetime.fromtimestamp(purchasedate_record / 1000.0)
                    purchasedItemCount = data.get("purchasedItemCount",None)
                    rewardsReceiptStatus = data.get("rewardsReceiptStatus",None)
                    totalSpent = data.get("totalSpent",None)
                    userId = data.get("userId")

                    values = (receiptId, bonusPointsEarned, bonusPointsEarnedReason, createDate, dateScanned, finishedDate, modifyDate, pointsAwardedDate, pointsEarned, purchaseDate, purchasedItemCount, rewardsReceiptStatus, totalSpent, userId)
                    print("Values:", values)
                    sql = """ INSERT INTO receipts (receiptId, bonusPointsEarned, bonusPointsEarnedReason, createDate, dateScanned, finishedDate, modifyDate, pointsAwardedDate, pointsEarned, purchaseDate, purchasedItemCount, rewardsReceiptStatus, totalSpent, userId )
                                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                     """
                    cursor.execute(sql, values)
                    conn.commit()
                    print("Values:", values)
                    print("Data inserted successfully!")
                except Exception as e:
                    print(f"Error inserting data: {e}")

    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Connection closed.")
insert_receipt("receipts.ndjson")
