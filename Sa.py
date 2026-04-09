import requests
import json
import logging
import sys
import os
import time
import io
import base64
from dotenv import load_dotenv
from requests.exceptions import RequestException
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd

load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

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

SA_SHEET_ID = "1iGsycyVskgCMwyaY3P6Sxf3I3DdUNKAag9p_oQ-ziy4"
SA_TAB_NAME = "Sa_Zip_Raw"

session = requests.Session()
USER_ID = None

COLUMN_ORDER = [
    "Customer", "Item", "Product", "Slider Code", "SA",
    "Order Date", "SA/ED Date", "SA/Closing Date",
    "Shade", "Size", "Size (Inch)", "Size (CM)",
    "OA Qty", "Qty Done", "Output", "OA Balance",
    "Style", "SA/Style Ref.", "Balance", "ID", "SA/ID", "State",
]


def retry_request(method, url, max_retries=3, backoff=3, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            r = method(url, **kwargs)
            if not r.ok:
                print(f"[WARN] HTTP {r.status_code}: {r.text[:500]}")
            r.raise_for_status()
            return r
        except RequestException as e:
            print(f"[WARN] Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                time.sleep(backoff)
            else:
                raise


def login():
    global USER_ID
    payload = {
        "jsonrpc": "2.0",
        "params": {"db": DB, "login": USERNAME, "password": PASSWORD},
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/session/authenticate", json=payload)
    result = r.json().get("result")
    if result and "uid" in result:
        USER_ID = result["uid"]
        print(f"[OK] Logged in (uid={USER_ID})")
        return result
    raise Exception("[ERR] Login failed")


# ========= FETCH sample.packing RECORDS ==========
PACKING_FIELDS = [
    "partner_id", "fg_categ_type", "product_template_id", "slidercodesfg",
    "oa_id", "date_order",
    "shade", "sizcommon", "sizein", "sizecm",
    "actual_qty", "done_qty", "uotput_qty", "ac_balance_qty",
    "style", "balance_qty", "state",
]

DOMAIN = [
    ["oa_id", "!=", False],
]


def fetch_packing_records():
    all_records = []
    offset = 0
    limit = 500

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "sample.packing",
                "method": "search_read",
                "args": [DOMAIN],
                "kwargs": {
                    "fields": PACKING_FIELDS,
                    "offset": offset,
                    "limit": limit,
                    "order": "date_order asc",
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": USER_ID,
                        "allowed_company_ids": [1, 3, 2, 4],
                    },
                },
            },
        }
        r = retry_request(
            session.post,
            f"{ODOO_URL}/web/dataset/call_kw/sample.packing/search_read",
            json=payload,
        )
        records = r.json().get("result", [])
        if not records:
            break
        all_records.extend(records)
        print(f"  Fetched {len(all_records)} packing records...")
        if len(records) < limit:
            break
        offset += limit

    print(f"Total packing records: {len(all_records)}")
    return all_records


# ========= FETCH oa_id RELATED FIELDS ==========
def fetch_oa_fields(oa_ids):
    """Fetch expected_delivery_date, closing_date, style_ref from sale.order."""
    if not oa_ids:
        return {}

    all_oas = []
    chunk = 500
    id_list = list(oa_ids)

    for i in range(0, len(id_list), chunk):
        batch = id_list[i:i + chunk]
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "sale.order",
                "method": "search_read",
                "args": [[["id", "in", batch]]],
                "kwargs": {
                    "fields": ["id", "expected_delivery_date", "closing_date", "style_ref"],
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": USER_ID,
                        "allowed_company_ids": [1, 3, 2, 4],
                    },
                },
            },
        }
        r = retry_request(
            session.post,
            f"{ODOO_URL}/web/dataset/call_kw/sale.order/search_read",
            json=payload,
        )
        result = r.json().get("result", [])
        all_oas.extend(result)

    print(f"  Fetched {len(all_oas)} OA records")
    return {rec["id"]: rec for rec in all_oas}


# ========= BUILD DATAFRAME ==========
def build_dataframe(records, oa_map):
    rows = []
    for rec in records:
        def m2o_name(val):
            if isinstance(val, (list, tuple)) and len(val) == 2:
                return val[1]
            return "" if val is False else val

        def m2o_id(val):
            if isinstance(val, (list, tuple)) and len(val) == 2:
                return val[0]
            return "" if val is False else val

        def clean(val):
            return "" if val is False else val

        oa_id_val = rec.get("oa_id")
        oa_id_int = m2o_id(oa_id_val)
        oa = oa_map.get(oa_id_int, {}) if oa_id_int else {}

        rows.append({
            "Customer":       m2o_name(rec.get("partner_id")),
            "Item":           clean(rec.get("fg_categ_type")),
            "Product":        m2o_name(rec.get("product_template_id")),
            "Slider Code":    clean(rec.get("slidercodesfg")),
            "SA":             m2o_name(oa_id_val),
            "Order Date":     clean(rec.get("date_order")),
            "SA/ED Date":     clean(oa.get("expected_delivery_date")),
            "SA/Closing Date":clean(oa.get("closing_date")),
            "Shade":          clean(rec.get("shade")),
            "Size":           clean(rec.get("sizcommon")),
            "Size (Inch)":    clean(rec.get("sizein")),
            "Size (CM)":      clean(rec.get("sizecm")),
            "OA Qty":         clean(rec.get("actual_qty")),
            "Qty Done":       clean(rec.get("done_qty")),
            "Output":         clean(rec.get("uotput_qty")),
            "OA Balance":     clean(rec.get("ac_balance_qty")),
            "Style":          clean(rec.get("style")),
            "SA/Style Ref.":  clean(oa.get("style_ref")),
            "Balance":        clean(rec.get("balance_qty")),
            "ID":             rec.get("id", ""),
            "SA/ID":          oa_id_int if oa_id_int else "",
            "State":          clean(rec.get("state")),
        })

    df = pd.DataFrame(rows, columns=COLUMN_ORDER)
    print(f"[INFO] DataFrame: {len(df)} rows × {len(df.columns)} columns")
    return df


# ========= PASTE TO GOOGLE SHEETS ==========
def paste_to_sheet(df):
    try:
        client = gspread.service_account(filename="service_account.json")
        sheet = client.open_by_key(SA_SHEET_ID)
        worksheet = sheet.worksheet(SA_TAB_NAME)
        worksheet.clear()
        set_with_dataframe(worksheet, df)
        print(f"[OK] Pasted {len(df)} rows to '{SA_TAB_NAME}'")
    except Exception as e:
        print(f"[ERR] Google Sheets error: {e}")
        raise


# ========= MAIN ==========
if __name__ == "__main__":
    login()

    records = fetch_packing_records()
    if not records:
        print("[ERR] No SA packing records found")
        sys.exit(1)

    # Collect unique oa_ids
    oa_ids = set()
    for rec in records:
        val = rec.get("oa_id")
        if isinstance(val, (list, tuple)) and len(val) == 2:
            oa_ids.add(val[0])

    oa_map = fetch_oa_fields(oa_ids)
    df = build_dataframe(records, oa_map)

    # Save locally
    df.to_excel("sa_packing.xlsx", index=False)
    print("[SAVED] Saved: sa_packing.xlsx")

    paste_to_sheet(df)
