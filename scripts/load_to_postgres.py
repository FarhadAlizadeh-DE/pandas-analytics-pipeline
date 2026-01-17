import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"

def db_url() -> str:
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    pwd = os.environ["POSTGRES_PASSWORD"]
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

def main() -> None:
    engine = create_engine(db_url())

    # Read processed files
    dim_customers = pd.read_csv(PROCESSED / "dim_customers.csv")
    fct_orders = pd.read_csv(PROCESSED / "fct_orders.csv")
    fct_order_items = pd.read_csv(PROCESSED / "fct_order_items.csv")

    with engine.begin() as conn:
        # Optional: keep things tidy
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))

    # Load to Postgres (replace for repeatability)
    dim_customers.to_sql("dim_customers", engine, schema="analytics", if_exists="replace", index=False)
    fct_orders.to_sql("fct_orders", engine, schema="analytics", if_exists="replace", index=False)
    fct_order_items.to_sql("fct_order_items", engine, schema="analytics", if_exists="replace", index=False)

    print("✅ Loaded tables into Postgres schema analytics:")
    print(" - analytics.dim_customers")
    print(" - analytics.fct_orders")
    print(" - analytics.fct_order_items")

if __name__ == "__main__":
    main()
