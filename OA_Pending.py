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
MANY2ONE_FIELDS = [
    "bottom", "buying_house", "company_id", "partner_id", "payment_term",
    "pinbox", "product_template_id", "product_id", "product_uom",
    "resign", "slider", "tape", "top", "wire", "oa_id",
]

SIMPLE_FIELDS = [
    "assembly_done", "b_part", "back_part", "balance_qty", "botomwire_con",
    "bottom_stock", "bot_plat_plan_end", "bot_plat_output", "bot_plat_plan_qty",
    "bpl_rec_plan_qty", "bot_plat_plan", "buyer_name", "c_part", "closing_date",
    "chain_making_done", "d_part", "diping_done", "done_qty", "dy_rec_plan_qty",
    "dyeing_output", "dyeing_plan", "dyeing_plan_due", "dyeing_plan_end",
    "dyeing_plan_qty", "dyeing_qc_pass", "exp_close_date", "validity_date",
    "finish", "finish_ref", "shade_name", "gmt", "fg_categ_type", "fg_categ_group",
    "lead_time", "logo", "logoref", "logo_type", "num_of_lots", "numberoftop",
    "oa_total_balance", "oa_total_qty", "date_order", "packing_done",
    "pin_plat_plan_end", "pin_plat_output", "pin_plat_plan_qty", "ppl_rec_plan_qty",
    "pin_plat_plan", "pinbox_con", "pinbox_stock", "plan_ids",
    "plating_plan_end", "plating_output", "plating_plan_qty", "pl_rec_plan_qty",
    "plating_plan", "product_code", "product_uom_qty", "resign_stock",
    "is_revised", "revision_no", "revised_status", "shade_code", "shade",
    "shade_ref", "shade_ref_2", "shade_ref_3", "shape", "shapefin",
    "sizecm", "sizein", "sizemm", "slidercodesfg", "sli_asmbl_output",
    "sli_asmbl_plan_end", "sli_asmbl_plan_qty", "sli_asmbl_plan", "sass_rec_plan_qty",
    "slider_con", "slider_stock", "st_lead_time", "state", "style",
    "tape_con", "tape_stock", "tbwire_con", "top_plat_plan_end", "top_plat_output",
    "top_plat_plan_qty", "tpl_rec_plan_qty", "top_plat_plan", "top_stock",
    "topbottom", "topwire_con", "wire_con", "wire_stock",
]

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
                        "allowed_company_ids": [company_id],
                    },
                    "count_limit": 100000,
                    "domain": DOMAIN,
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

    # Flatten many2one dicts to display_name, remove internal id column
    flat = []
    for rec in all_records:
        row = {}
        for key, val in rec.items():
            if key == "id":
                continue
            row[key] = val.get("display_name", "") if isinstance(val, dict) else val
        flat.append(row)

    df = pd.DataFrame(flat)
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    print(f"📥 OA Pending Excel built ({len(df)} rows)")
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
