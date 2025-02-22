import psycopg2
import json
import uuid

import os
from dotenv import load_dotenv

load_dotenv()
# Database connection details
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

def insert_receiptItem(json_file):
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
                    items = data.get("rewardsReceiptItemList", [])
                    #print(items)
                    receiptId = str(data["_id"]["$oid"])
                    if isinstance(items, list):
                        print("rewardsReceiptItemList is available")
                    for item in items:
                            # Extract necessary fields
                        receiptItemId = str(uuid.uuid4())
                        print(receiptItemId)
                        barcode = item.get("barcode")
                        print("Barcode:",barcode)
                        brandCode = item.get("brandCode")
                        description = item.get("description")
                        itemPrice = item.get("itemPrice")
                        finalPrice = item.get("finalPrice")
                        needsFetchReview = item.get("needsFetchReview")
                        needsFetchReviewReason = item.get("needsFetchReviewReason")
                        partnerItemId = item.get("partnerItemId")
                        pointsNotAwardedReason = item.get("pointsNotAwardedReason")
                        pointsPayerId = item.get("pointsPayerId")
                        preventTargetGapPoints = item.get("preventTargetGapPoints")
                        quantityPurchased = item.get("quantityPurchased")
                        rewardsGroup = item.get("rewardsGroup")
                        rewardsProductPartnerId = item.get("rewardsProductPartnerId")
                        userFlaggedBarcode = item.get("userFlaggedBarcode")
                        userFlaggedDescription = item.get("userFlaggedDescription")
                        userFlaggedNewItem = item.get("userFlaggedNewItem")
                        userFlaggedPrice = item.get("userFlaggedPrice")
                        userFlaggedQuantity = item.get("userFlaggedQuantity")

                        values = (receiptItemId, receiptId, brandCode, barcode, description, itemPrice, finalPrice, needsFetchReview,
                              needsFetchReviewReason, partnerItemId, pointsNotAwardedReason, pointsPayerId, preventTargetGapPoints,
                              quantityPurchased, rewardsGroup, rewardsProductPartnerId, userFlaggedBarcode,
                              userFlaggedDescription, userFlaggedNewItem, userFlaggedPrice, userFlaggedQuantity)

                        sql = """ INSERT INTO receiptItem (receiptItemId, receiptId, brandCode, barcode, description, itemPrice, finalPrice, needsFetchReview,
                              needsFetchReviewReason, partnerItemId, pointsNotAwardedReason, pointsPayerId, preventTargetGapPoints,
                              quantityPurchased, rewardsGroup, rewardsProductPartnerId, userFlaggedBarcode,
                              userFlaggedDescription, userFlaggedNewItem, userFlaggedPrice, userFlaggedQuantity)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                 """
                        cursor.execute(sql, values)
                        conn.commit()
                        print("Values:",values)
                        print("Data inserted successfully!")
                except Exception as e:
                    print(f"Error processing data: {e}")

    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
         cursor.close()
         conn.close()
         print("🔌 Connection closed.")


insert_receiptItem("receiptItem.ndjson")
