from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import os
from datetime import datetime
import re
import uuid

app = Flask(__name__)

CUSTOMERS_CSV = "customers.csv"
HISTORY_CSV   = "purchase_history.csv"

CUSTOMERS_COLS = [
    "Customer_ID", "Name", "Mobile",
    "Total_Purchase", "Points", "Redeemed_Points",
    "Purchase_Date"  # last purchase date
]

HISTORY_COLS = [
    "Txn_ID", "Customer_ID", "Name", "Mobile",
    "Purchase_Date", "Amount", "Points_Earned"
]

def ensure_csv(path, cols):
    """Create CSV with headers if missing."""
    if not os.path.exists(path):
        pd.DataFrame(columns=cols).to_csv(path, index=False)

def load_customers():
    ensure_csv(CUSTOMERS_CSV, CUSTOMERS_COLS)
    df = pd.read_csv(
        CUSTOMERS_CSV,
        dtype={"Customer_ID": str, "Name": str, "Mobile": str, "Purchase_Date": str}
    )
    # ensure all columns exist and order them
    for c in CUSTOMERS_COLS:
        if c not in df.columns:
            df[c] = "" if c in ("Name","Mobile","Purchase_Date","Customer_ID") else 0
    df = df[CUSTOMERS_COLS]

    # coerce numeric columns
    for c in ["Total_Purchase", "Points", "Redeemed_Points"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    # strings
    df["Customer_ID"] = df["Customer_ID"].astype(str)
    df["Mobile"] = df["Mobile"].astype(str)
    df["Name"] = df["Name"].astype(str)
    df["Purchase_Date"] = df["Purchase_Date"].fillna("")
    return df

def save_customers(df):
    df.to_csv(CUSTOMERS_CSV, index=False)

def load_history():
    ensure_csv(HISTORY_CSV, HISTORY_COLS)
    df = pd.read_csv(
        HISTORY_CSV,
        dtype={"Txn_ID": str, "Customer_ID": str, "Name": str, "Mobile": str, "Purchase_Date": str}
    )
    for c in HISTORY_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[HISTORY_COLS]
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    df["Points_Earned"] = pd.to_numeric(df["Points_Earned"], errors="coerce").fillna(0).astype(int)
    return df

def save_history(df):
    df.to_csv(HISTORY_CSV, index=False)

def next_customer_id(df):
    """Generate next ID like CUST0001, CUST0002, ..."""
    if df.empty:
        return "CUST0001"
    nums = []
    for cid in df["Customer_ID"].dropna().astype(str):
        m = re.match(r"^CUST(\d{4,})$", cid)
        if m:
            nums.append(int(m.group(1)))
    n = max(nums) + 1 if nums else 1
    return f"CUST{n:04d}"

def points_from_amount(amount):
    # 10 riyal => 1 point (so 100 riyal => 10 points)
    return int(amount // 10)

@app.route("/", methods=["GET", "POST"])
def index():
    df = load_customers()
    if request.method == "POST":
        # Add Purchase form
        name = request.form.get("name", "").strip()
        mobile = request.form.get("mobile", "").strip()
        amount = float(request.form.get("amount", "0") or 0)
        purchase_date = request.form.get("purchase_date", "").strip()
        if not purchase_date:
            purchase_date = datetime.now().strftime("%Y-%m-%d")

        # primary key: Mobile
        exists = df["Mobile"] == str(mobile)
        if exists.any():
            idx = df[exists].index[0]
            df.at[idx, "Name"] = name or df.at[idx, "Name"]
            df.at[idx, "Total_Purchase"] = int(df.at[idx, "Total_Purchase"] + amount)
            df.at[idx, "Points"] = int(df.at[idx, "Points"] + points_from_amount(amount))
            df.at[idx, "Purchase_Date"] = purchase_date
            cust_id = df.at[idx, "Customer_ID"]
        else:
            cust_id = next_customer_id(df)
            new_row = pd.DataFrame([{
                "Customer_ID": cust_id,
                "Name": name,
                "Mobile": str(mobile),
                "Total_Purchase": int(amount),
                "Points": points_from_amount(amount),
                "Redeemed_Points": 0,
                "Purchase_Date": purchase_date
            }])[CUSTOMERS_COLS]
            df = pd.concat([df, new_row], ignore_index=True)

        save_customers(df)

        # write to purchase history
        hx = load_history()
        hx = pd.concat([hx, pd.DataFrame([{
            "Txn_ID": uuid.uuid4().hex[:12],
            "Customer_ID": cust_id,
            "Name": name,
            "Mobile": str(mobile),
            "Purchase_Date": purchase_date,
            "Amount": amount,
            "Points_Earned": points_from_amount(amount)
        }])[HISTORY_COLS]], ignore_index=True)
        save_history(hx)

        return redirect(url_for("index"))

    customers = df.to_dict(orient="records")
    return render_template("index.html", customers=customers)

@app.route("/update_customer", methods=["POST"])
def update_customer():
    # Update only Name & Mobile (layout stays clean)
    customer_id = request.form.get("customer_id")
    new_name = request.form.get("new_name", "").strip()
    new_mobile = request.form.get("new_mobile", "").strip()

    df = load_customers()
    if customer_id not in df["Customer_ID"].values:
        return redirect(url_for("index"))

    # If mobile is changed, ensure no duplicate to another customer
    if new_mobile:
        dup = df[(df["Mobile"] == new_mobile) & (df["Customer_ID"] != customer_id)]
        if not dup.empty:
            # refuse duplicate mobile change
            return redirect(url_for("index"))

    idx = df[df["Customer_ID"] == customer_id].index[0]
    if new_name:
        df.at[idx, "Name"] = new_name
    if new_mobile:
        df.at[idx, "Mobile"] = new_mobile

    save_customers(df)
    return redirect(url_for("index"))

@app.route("/delete_customer", methods=["POST"])
def delete_customer():
    customer_id = request.form.get("customer_id")
    df = load_customers()
    if customer_id in df["Customer_ID"].values:
        df = df[df["Customer_ID"] != customer_id]
        save_customers(df)
    # We keep purchase history intact
    return redirect(url_for("index"))

@app.route("/redeem", methods=["GET", "POST"])
def redeem():
    df = load_customers()
    results = []

    if request.method == "POST":
        action = request.form.get("action")
        if action == "search":
            query = request.form.get("query", "").strip()
            if query:
                mask = (
                    df["Mobile"].astype(str).str.contains(query, case=False, na=False) |
                    df["Customer_ID"].astype(str).str.contains(query, case=False, na=False)
                )
                results = df[mask].to_dict(orient="records")
        elif action == "redeem":
            customer_id = request.form.get("customer_id")
            redeem_points = int(request.form.get("redeem_points", "0") or 0)
            if customer_id in df["Customer_ID"].values:
                idx = df[df["Customer_ID"] == customer_id].index[0]
                available = int(df.at[idx, "Points"])
                # Only allow redeem if available >= 100 and redeem_points between 100..available
                if available >= 100 and 100 <= redeem_points <= available:
                    df.at[idx, "Points"] = available - redeem_points
                    df.at[idx, "Redeemed_Points"] = int(df.at[idx, "Redeemed_Points"] + redeem_points)
                    save_customers(df)
            return redirect(url_for("redeem"))

    return render_template("redeem.html", customers=results)

@app.route("/history", methods=["GET", "POST"])
def history():
    hx = load_history()
    results = []
    header = None

    if request.method == "POST":
        query = request.form.get("query", "").strip()
        if query:
            mask = (
                hx["Mobile"].astype(str).str.contains(query, case=False, na=False) |
                hx["Customer_ID"].astype(str).str.contains(query, case=False, na=False)
            )
            results = hx[mask].copy()
            # Sort by date (YYYY-MM-DD expected)
            results["Purchase_Date"] = pd.to_datetime(results["Purchase_Date"], errors="coerce")
            results = results.sort_values("Purchase_Date", ascending=False)
            results["Purchase_Date"] = results["Purchase_Date"].dt.strftime("%Y-%m-%d")
            if not results.empty:
                first = results.iloc[0]
                header = {
                    "Customer_ID": first["Customer_ID"],
                    "Name": first["Name"],
                    "Mobile": first["Mobile"]
                }
            results = results.to_dict(orient="records")

    return render_template("purchase_history.html", purchases=results, header=header)

if __name__ == "__main__":
    import os
    # use the PORT env var if set (Render provides it), default to 5000 for local testing
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

