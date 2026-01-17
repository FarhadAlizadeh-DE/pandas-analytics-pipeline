from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed"

def run_pipeline() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    customers = pd.read_csv(RAW / "customers.csv")
    orders = pd.read_csv(RAW / "orders.csv")
    items = pd.read_csv(RAW / "order_items.csv")

    # --- Clean customers ---
    customers["email"] = customers["email"].astype(str).str.strip().str.lower()
    customers["full_name"] = customers["full_name"].astype(str).str.strip()
    customers["updated_at"] = pd.to_datetime(customers["updated_at"], errors="coerce")

    # Keep latest record per customer_id
    dim_customers = (
        customers.sort_values(["customer_id", "updated_at"])
        .drop_duplicates(subset=["customer_id"], keep="last")
        .reset_index(drop=True)
    )

    # --- Clean orders ---
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders["customer_id"] = pd.to_numeric(orders["customer_id"], errors="coerce")

    # Enforce referential integrity: drop orders with missing customers
    valid_customer_ids = set(dim_customers["customer_id"].dropna().astype(int).tolist())
    orders = orders[orders["customer_id"].isin(valid_customer_ids)].copy()

    # --- Clean items ---
    items["quantity"] = pd.to_numeric(items["quantity"], errors="coerce")
    items["unit_price"] = pd.to_numeric(items["unit_price"], errors="coerce")

    # Basic sanity filters
    items = items[(items["quantity"] >= 1) & (items["unit_price"] > 0)].copy()
    items["line_total"] = items["quantity"] * items["unit_price"]

    # --- Build fct tables ---
    order_totals = (
        items.groupby("order_id", as_index=False)
        .agg(order_total=("line_total", "sum"), item_count=("product_sku", "count"))
    )

    fct_orders = orders.merge(order_totals, on="order_id", how="left").fillna(
        {"order_total": 0.0, "item_count": 0}
    )

    # Write outputs
    dim_customers.to_csv(OUT / "dim_customers.csv", index=False)
    fct_orders.to_csv(OUT / "fct_orders.csv", index=False)
    items.to_csv(OUT / "fct_order_items.csv", index=False)

if __name__ == "__main__":
    run_pipeline()
    print("✅ Pipeline complete. Wrote processed CSVs to data/processed/")
