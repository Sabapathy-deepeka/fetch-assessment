import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables (Optional)
load_dotenv()

# PostgreSQL connection parameters
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
        print("Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None

# Function to execute a single query and print results
def run_query(query, conn, check_name, table_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            if results:
                print(f"{check_name} Issues Found in {table_name}:\n")
                for row in results:
                    print(row)
            else:
                print(f"No {check_name} Issues Found in {table_name}")
    except Exception as e:
        print(f"Error executing query for {table_name}: {e}")

# Define separate data quality check queries
QUERIES = {
    "Missing Values": {
        "brand": "SELECT brandId, brandCode FROM brand WHERE brandId IS NULL OR brandCode IS NULL;",
        "receipts": "SELECT receiptId,purchaseDate FROM receipts WHERE receiptId IS NULL OR purchaseDate IS NULL;",
        "receiptItem": "SELECT receiptId, brandCode FROM receiptItem WHERE receiptId IS NULL OR brandCode IS NULL;"
    },
    "Duplicate Records": {
        "brand": "SELECT brandId, COUNT(*) FROM brand GROUP BY brandId HAVING COUNT(*) > 1;",
        "receipts": "SELECT receiptId, COUNT(*) FROM receipts GROUP BY receiptId HAVING COUNT(*) > 1;",
        "receiptItem": "SELECT barcode, COUNT(*) FROM receiptItem GROUP BY barcode HAVING COUNT(*) > 1;",
    },
    "Orphaned Foreign Keys": {
        "receiptId in receiptItem table, but not in receiptId": """
            SELECT ri.receiptId
            FROM receiptItem ri
            LEFT JOIN receipts r ON ri.receiptId = r.receiptId
            WHERE r.receiptId IS NULL;
        """
    },
    "Negative or Zero Values": {
        "receipts": "SELECT receiptId FROM receipts WHERE cast(totalSpent AS NUMERIC) <= 0.0;",
        "receiptItem": "SELECT receiptId FROM receiptItem WHERE itemPrice <= 0.0 OR finalPrice <= 0.0;"
    },
    "Invalid Date Ranges": {
        "receipts": "SELECT receiptId FROM receipts WHERE purchaseDate > NOW() OR purchaseDate < '2000-01-01';",
        "receipts": "SELECT receiptId FROM receipts WHERE purchaseDate > NOW() OR dateScanned < '2000-01-01';"
    },
    "Inconsistent Brand Codes": {
        "brand": "SELECT brandCode, COUNT(*) FROM brand GROUP BY brandCode, LOWER(brandCode) HAVING COUNT(*) > 1;"
    }
}

# Run all checks separately
def main():
    conn = connect_db()
    if conn:
        for check_name, tables in QUERIES.items():
            print(f"\nChecking: {check_name}")
            for table_name, query in tables.items():
                run_query(query, conn, check_name, table_name)
        conn.close()
        print("\nData Quality Checks Completed!")

# Run the script
if __name__ == "__main__":
    main()
