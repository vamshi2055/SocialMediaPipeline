import os
import stripe
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Stripe setup
stripe.api_key = os.getenv("STRIPE_API_KEY")

# Fetch charges
def fetch_charges(limit=100):
    charges = stripe.Charge.list(limit=limit)
    return [
        {
            "id": ch.id,
            "amount": ch.amount,
            "currency": ch.currency,
            "status": ch.status,
            "created": ch.created,
            "customer": ch.customer,
            "description": ch.description
        }
        for ch in charges.auto_paging_iter()
    ]

# Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

# Insert data into Snowflake
def insert_to_snowflake(data):
    conn = get_snowflake_connection()
    cs = conn.cursor()
    try:
        cs.execute("""
            CREATE TABLE IF NOT EXISTS stripe_charges (
                id STRING,
                amount NUMBER,
                currency STRING,
                status STRING,
                created TIMESTAMP,
                customer STRING,
                description STRING
            )
        """)
        for row in data:
            cs.execute("""
                INSERT INTO stripe_charges (
                    id, amount, currency, status, created, customer, description
                ) VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s)
            """, (
                row['id'], row['amount'], row['currency'], row['status'],
                row['created'], row['customer'], row['description']
            ))
    finally:
        cs.close()
        conn.close()

# Run it
if __name__ == "__main__":
    charges = fetch_charges(limit=50)
    insert_to_snowflake(charges)
    print(f"{len(charges)} Stripe charges ingested into Snowflake ✅")
