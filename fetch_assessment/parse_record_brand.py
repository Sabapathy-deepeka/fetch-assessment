import psycopg2
import json

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

def insert_brand(json_file):
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
                    brandId = str(data["_id"]["$oid"])
                    brandName = data.get("name")
                    cpgId = str(data["cpg"]["$id"]["$oid"])
                    cpgRef = data.get("cpg").get("$ref")
                    category = data.get("category")
                    categoryCode = data.get("categoryCode")
                    barcode = data.get("barcode")
                    brandCode = data.get("brandCode")
                    topBrand = data.get("topBrand")
                    #print(brandId,brandName,cpgId,cpgRef,category,categoryCode,barcode,brandCode,topBrand)
                    values = (brandId, brandName, cpgId, cpgRef, category, categoryCode, barcode, brandCode, topBrand)

                    sql = """ INSERT INTO brand (brandId, brandName, cpgId, cpgRef, category, categoryCode, barcode, brandCode, topBrand)
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
         print("Connection closed.")


insert_brand("brand.ndjson")
