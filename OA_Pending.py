import requests
import json
import logging
import sys
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import io
import base64
from dotenv import load_dotenv
from requests.exceptions import RequestException

load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

# Google credentials from secret (raw JSON or base64 encoded)
GOOGLE_CREDS_BASE64 = os.getenv("GOOGLE_CREDS_BASE64")
if GOOGLE_CREDS_BASE64:
    stripped = GOOGLE_CREDS_BASE64.strip()
    if stripped.startswith("{"):
        creds_json = stripped
    else:
        padded = stripped + "=" * (-len(stripped) % 4)
        creds_json = base64.b64decode(padded).decode("utf-8")
    with open("service_account.json", "w") as f:
        f.write(creds_json)

# Google Sheet config for OA Pending
OA_PENDING_SHEET_ID = "1knZ6hN-1iMmsKL26m2lIKOHgvf7FY7lxAMwDMI4vbCk"

COMPANIES = {
    1: "Zipper",
    3: "Metal",
}

SHEET_TABS = {
    1: "Zipper",
    3: "Metal",
}

session = requests.Session()
USER_ID = None


def retry_request(method, url, max_retries=3, backoff=3, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            r = method(url, **kwargs)
            if not r.ok:
                print(f"⚠️ HTTP {r.status_code} response body: {r.text[:500]}")
            r.raise_for_status()
            return r
        except RequestException as e:
            print(f"⚠️ Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                print(f"⏳ Retrying in {backoff} seconds...")
                time.sleep(backoff)
            else:
                print("❌ All retry attempts failed.")
                raise


# ========= LOGIN ==========
def login():
    global USER_ID
    payload = {"jsonrpc": "2.0", "params": {"db": DB, "login": USERNAME, "password": PASSWORD}}
    r = retry_request(session.post, f"{ODOO_URL}/web/session/authenticate", json=payload)
    result = r.json().get("result")
    if result and "uid" in result:
        USER_ID = result["uid"]
        print(f"✅ Logged in (uid={USER_ID})")
        return result
    else:
        raise Exception("❌ Login failed")


# ========= SWITCH COMPANY ==========
def switch_company(company_id):
    if USER_ID is None:
        raise Exception("User not logged in yet")
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "res.users",
            "method": "write",
            "args": [[USER_ID], {"company_id": company_id}],
            "kwargs": {"context": {"allowed_company_ids": [company_id], "company_id": company_id}},
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    r.raise_for_status()
    if "error" in r.json():
        print(f"❌ Failed to switch to company {company_id}: {r.json()['error']}")
        return False
    print(f"🔄 Session switched to company {company_id}")
    return True


# ========= FETCH OA PENDING VIA JSON-RPC (no CSRF needed) ==========
MANY2ONE_FIELDS = ["partner_id", "oa_id", "product_template_id", "tape", "pinbox"]

SIMPLE_FIELDS = [
    "date_order", "fg_categ_type", "product_uom_qty", "done_qty", "balance_qty",
    "sizein", "sizecm", "slidercodesfg", "topbottom",
    "slider_con", "tape_con", "wire_con", "pinbox_con", "topwire_con", "botomwire_con",
]

# Column rename + order to match the expected Excel layout
COLUMN_ORDER = [
    "Customer", "OA", "Order Date", "Item", "Product",
    "Quantity", "Done Qty", "Balance",
    "Size (Inch)", "Size (CM)", "Slider", "Dyed Tape", "Pin-Box Finish",
    "Top/Bottom", "Slider C.", "Tape C.", "Wire C.", "Pinbox C.", "Topwire C.", "Botomwire C.",
]

COLUMN_RENAME = {
    "partner_id":         "Customer",
    "oa_id":              "OA",
    "date_order":         "Order Date",
    "fg_categ_type":      "Item",
    "product_template_id":"Product",
    "product_uom_qty":    "Quantity",
    "done_qty":           "Done Qty",
    "balance_qty":        "Balance",
    "sizein":             "Size (Inch)",
    "sizecm":             "Size (CM)",
    "slidercodesfg":      "Slider",
    "tape":               "Dyed Tape",
    "pinbox":             "Pin-Box Finish",
    "topbottom":          "Top/Bottom",
    "slider_con":         "Slider C.",
    "tape_con":           "Tape C.",
    "wire_con":           "Wire C.",
    "pinbox_con":         "Pinbox C.",
    "topwire_con":        "Topwire C.",
    "botomwire_con":      "Botomwire C.",
}

DOMAIN = [
    "&", "&",
    ["oa_total_balance", ">", 0],
    ["oa_id", "!=", False],
    ["state", "not in", ["closed", "cancel", "hold"]],
]


def download_oa_pending_xlsx(company_id):
    """
    Fetches OA Pending records via web_search_read (JSON-RPC, no CSRF),
    paginates through all results, and returns Excel bytes.
    """
    specification = {f: {"fields": {"display_name": {}}} for f in MANY2ONE_FIELDS}
    specification.update({f: {} for f in SIMPLE_FIELDS})

    all_records = []
    offset = 0
    limit = 500

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "manufacturing.order",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": specification,
                    "offset": offset,
                    "order": "date_order asc",
                    "limit": limit,
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": USER_ID,
                        "allowed_company_ids": [3, 1, 2, 4],
                    },
                    "count_limit": 100000,
                    "domain": DOMAIN + [["company_id", "=", company_id]],
                },
            },
        }
        r = retry_request(
            session.post,
            f"{ODOO_URL}/web/dataset/call_kw/manufacturing.order/web_search_read",
            json=payload,
        )
        result = r.json().get("result", {})
        records = result.get("records", [])
        total = result.get("length", 0)

        if not records:
            break

        all_records.extend(records)
        print(f"  Fetched {len(all_records)}/{total} records...")

        if len(all_records) >= total:
            break
        offset += limit

    if not all_records:
        print("⚠️ No OA Pending records found")
        return None

    print(f"Total records: {len(all_records)}")

    # Flatten many2one dicts to display_name, drop internal id
    flat = []
    for rec in all_records:
        row = {}
        for key, val in rec.items():
            if key == "id":
                continue
            if isinstance(val, dict):
                row[key] = val.get("display_name", "")
            elif val is False:
                row[key] = ""
            else:
                row[key] = val
        flat.append(row)

    df = pd.DataFrame(flat).rename(columns=COLUMN_RENAME)[COLUMN_ORDER]
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    print(f"📥 OA Pending Excel built ({len(df)} rows, {len(df.columns)} columns)")
    return buf.read()


# ========= PASTE TO GOOGLE SHEETS ==========
def paste_to_google_sheet(company_id, cname, xlsx_content):
    tab_name = SHEET_TABS[company_id]
    try:
        df = pd.read_excel(io.BytesIO(xlsx_content))
        client = gspread.service_account(filename="service_account.json")
        sheet = client.open_by_key(OA_PENDING_SHEET_ID)
        worksheet = sheet.worksheet(tab_name)

        worksheet.clear()
        set_with_dataframe(worksheet, df)
        print(f"✅ {cname} OA Pending pasted to tab '{tab_name}' ({len(df)} rows)")
    except Exception as e:
        print(f"❌ Error pasting {cname} OA Pending to Google Sheets: {e}")


# ========= MAIN ==========
if __name__ == "__main__":
    login()

    for cid, cname in COMPANIES.items():
        print(f"\n📋 Processing OA Pending for {cname} (company {cid})...")
        if not switch_company(cid):
            continue

        xlsx_content = download_oa_pending_xlsx(cid)
        if xlsx_content:
            output_file = f"oa_pending_{cname.lower()}.xlsx"
            with open(output_file, "wb") as f:
                f.write(xlsx_content)
            print(f"📂 Saved: {output_file}")

            paste_to_google_sheet(cid, cname, xlsx_content)
        else:
            print(f"❌ Failed to download OA Pending for {cname}")
