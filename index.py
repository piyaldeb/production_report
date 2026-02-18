import requests
import json
import logging
import sys
import os
from datetime import date, datetime, timedelta
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import io
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

# Google credentials from secret (raw JSON or base64 encoded)
import base64
GOOGLE_CREDS_BASE64 = os.getenv("GOOGLE_CREDS_BASE64")
if GOOGLE_CREDS_BASE64:
    # Try raw JSON first, fall back to base64 decoding
    stripped = GOOGLE_CREDS_BASE64.strip()
    if stripped.startswith("{"):
        creds_json = stripped
    else:
        padded = stripped + "=" * (-len(stripped) % 4)
        creds_json = base64.b64decode(padded).decode("utf-8")
    with open("service_account.json", "w") as f:
        f.write(creds_json)

COMPANIES = {
    1: "Zipper",
    3: "Button",
}

# Google Sheet config
GOOGLE_SHEET_ID = "1cFzPYXoI-HdtBjUvMadI9kTKvBs4E9dj2FgAzrxEPq4"
SHEET_TABS = {
    1: "Zipper",   # Company 1 -> Zipper tab
    3: "Metal",    # Company 3 -> Metal tab
}

# DPR Date: yesterday
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()

session = requests.Session()
USER_ID = None

import time
from requests.exceptions import RequestException

def retry_request(method, url, max_retries=3, backoff=3, **kwargs):
    """
    Wrapper for requests with retry logic.
    Retries up to `max_retries` times with `backoff` seconds delay.
    """
    for attempt in range(1, max_retries + 1):
        try:
            r = method(url, **kwargs)
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
    else:
        print(f"🔄 Session switched to company {company_id}")
        return True


# ========= CREATE DPR WIZARD ==========
def create_dpr_wizard(company_id, date_from, date_to):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "mrp.report.custom",
            "method": "web_save",
            "args": [[], {
                "report_type": "dpr",
                "challan_no": False,
                "date_from": date_from,
                "date_to": date_to
            }],
            "kwargs": {
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
                            "allowed_company_ids": [company_id]},
                "specification": {
                    "report_type": {},
                    "challan_no": {},
                    "date_from": {},
                    "date_to": {},
                },
            },
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw/mrp.report.custom/web_save", json=payload)
    r.raise_for_status()
    result = r.json().get("result", [])
    if isinstance(result, list) and result:
        wiz_id = result[0]["id"]
        print(f"🪄 DPR wizard {wiz_id} created for company {company_id}")
        return wiz_id
    else:
        raise Exception(f"❌ Failed to create DPR wizard: {r.text}")


# ========= GENERATE DPR XLSX REPORT ==========
def generate_dpr_report(company_id, wizard_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "args": [[wizard_id]],
            "kwargs": {
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
                            "allowed_company_ids": [company_id]}
            },
            "method": "action_generate_xlsx_report",
            "model": "mrp.report.custom"
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_button", json=payload)
    r.raise_for_status()
    result = r.json().get("result", {})
    if "error" in r.json():
        print(f"❌ Error generating DPR report for {company_id}: {r.json()['error']}")
        return None
    print(f"⚡ DPR report generated for wizard {wizard_id} (company {company_id})")
    print(f"📎 Generate result: {json.dumps(result, indent=2)[:1000]}")
    return result


# ========= DOWNLOAD DPR REPORT ==========
def download_dpr_report(company_id, wizard_id, date_from, date_to):
    import urllib.parse

    options = json.dumps({"month_list": False, "date_from": date_from, "date_to": date_to})
    context = json.dumps({
        "lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
        "allowed_company_ids": [company_id],
        "active_model": "mrp.report.custom",
        "active_id": wizard_id,
        "active_ids": [wizard_id]
    })

    report_url = f"{ODOO_URL}/report/xlsx/taps_manufacturing.packing_invoice/{wizard_id}?options={urllib.parse.quote(options)}&context={urllib.parse.quote(context)}"

    r = session.get(report_url)
    if r.status_code != 200:
        print(f"❌ Download failed (HTTP {r.status_code}): {r.text[:500]}")
        return None

    if r.headers.get("content-type", "").startswith("application/vnd.openxmlformats"):
        print(f"📥 DPR report downloaded for company {company_id}")
        return r.content
    else:
        print(f"❌ Failed to download DPR report: {r.text[:200]}")
        return None


# ========= MAIN ==========
if __name__ == "__main__":
    userinfo = login()
    print("User info (allowed companies):", userinfo.get("user_companies", {}))

    for cid, cname in COMPANIES.items():
        if switch_company(cid):
            # ========= DPR (Daily Production Report) ==========
            print(f"\n📋 Generating DPR for {cname} (date: {YESTERDAY})...")
            try:
                dpr_wiz_id = create_dpr_wizard(cid, YESTERDAY, YESTERDAY)
                dpr_result = generate_dpr_report(cid, dpr_wiz_id)
                if dpr_result:
                    xlsx_content = download_dpr_report(cid, dpr_wiz_id, YESTERDAY, YESTERDAY)
                    if xlsx_content:
                        # Save to local file
                        dpr_output_file = f"{cname.lower()}_dpr_{YESTERDAY}.xlsx"
                        with open(dpr_output_file, "wb") as f:
                            f.write(xlsx_content)
                        print(f"📂 DPR Saved: {dpr_output_file}")

                        # ========= PASTE TO GOOGLE SHEETS ==========
                        try:
                            df = pd.read_excel(io.BytesIO(xlsx_content))
                            client = gspread.service_account(filename="service_account.json")
                            sheet = client.open_by_key(GOOGLE_SHEET_ID)
                            tab_name = SHEET_TABS.get(cid)
                            if tab_name:
                                worksheet = sheet.worksheet(tab_name)
                                worksheet.clear()
                                set_with_dataframe(worksheet, df)
                                print(f"✅ DPR pasted to Google Sheet tab: {tab_name}")
                            else:
                                print(f"⚠️ No tab configured for company {cid}")
                        except Exception as e:
                            print(f"❌ Error pasting to Google Sheets: {e}")

            except Exception as e:
                print(f"❌ Error generating DPR for {cname}: {e}")
