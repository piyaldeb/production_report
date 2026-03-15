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


# ========= DOWNLOAD OA PENDING XLSX ==========
def download_oa_pending_xlsx(company_id):
    """
    Downloads OA Pending report as XLSX via /web/export/xlsx for the given company.
    Domain: oa_total_balance > 0, oa_id != False, state not in [closed, cancel, hold]
    Ordered by date_order asc.
    """
    export_fields = [
        {"name": "assembly_done", "label": "Assembly Output", "type": "float"},
        {"name": "b_part", "label": "B Part", "type": "text"},
        {"name": "back_part", "label": "Back Part", "type": "text"},
        {"name": "balance_qty", "label": "Balance", "type": "float"},
        {"name": "botomwire_con", "label": "Botomwire C.", "type": "float"},
        {"name": "bottom", "label": "Bottom", "type": "many2one"},
        {"name": "bottom_stock", "label": "Bottom Stock", "type": "float"},
        {"name": "bot_plat_plan_end", "label": "Btm Plat/Paint End", "type": "datetime"},
        {"name": "bot_plat_output", "label": "Btm Plat/Paint Output", "type": "float"},
        {"name": "bot_plat_plan_qty", "label": "Btm Plat/Paint Plan Qty", "type": "float"},
        {"name": "bpl_rec_plan_qty", "label": "Btm Plat/Paint Recplan Qty", "type": "float"},
        {"name": "bot_plat_plan", "label": "Btm Plat/Paint Start", "type": "datetime"},
        {"name": "buying_house", "label": "Buyeing House", "type": "many2one"},
        {"name": "buyer_name", "label": "Buyer", "type": "char"},
        {"name": "c_part", "label": "C Part", "type": "text"},
        {"name": "closing_date", "label": "Closing Date", "type": "datetime"},
        {"name": "chain_making_done", "label": "CM Output", "type": "float"},
        {"name": "company_id", "label": "Company", "type": "many2one"},
        {"name": "partner_id", "label": "Customer", "type": "many2one"},
        {"name": "d_part", "label": "D Part", "type": "text"},
        {"name": "diping_done", "label": "Dipping Output", "type": "float"},
        {"name": "done_qty", "label": "Done Qty", "type": "float"},
        {"name": "dy_rec_plan_qty", "label": "Dye Last Plan", "type": "float"},
        {"name": "dyeing_output", "label": "Dye Output", "type": "float"},
        {"name": "dyeing_plan", "label": "Dye Plan", "type": "datetime"},
        {"name": "dyeing_plan_due", "label": "Dye Plan Due", "type": "float"},
        {"name": "dyeing_plan_end", "label": "Dye Plan End", "type": "datetime"},
        {"name": "dyeing_plan_qty", "label": "Dye Plan Qty", "type": "float"},
        {"name": "dyeing_qc_pass", "label": "Dye QC Pass", "type": "float"},
        {"name": "exp_close_date", "label": "Expected Closing Date", "type": "date"},
        {"name": "validity_date", "label": "Expiration", "type": "date"},
        {"name": "finish", "label": "Finish", "type": "char"},
        {"name": "finish_ref", "label": "Finish Ref", "type": "text"},
        {"name": "shade_name", "label": "Full Shade", "type": "text"},
        {"name": "gmt", "label": "Gmt", "type": "text"},
        {"name": "fg_categ_type", "label": "Item", "type": "char"},
        {"name": "fg_categ_group", "label": "Item Group", "type": "char"},
        {"name": "lead_time", "label": "Lead Time", "type": "integer"},
        {"name": "logo", "label": "Logo", "type": "text"},
        {"name": "logoref", "label": "Logo Ref", "type": "text"},
        {"name": "logo_type", "label": "Logo Type", "type": "text"},
        {"name": "num_of_lots", "label": "N. of Lots", "type": "integer"},
        {"name": "numberoftop", "label": "N.Top", "type": "char"},
        {"name": "oa_id", "label": "OA", "type": "many2one"},
        {"name": "oa_total_balance", "label": "OA Balance", "type": "float"},
        {"name": "oa_total_qty", "label": "OA Total Qty", "type": "float"},
        {"name": "date_order", "label": "Order Date", "type": "datetime"},
        {"name": "packing_done", "label": "Packing Output", "type": "float"},
        {"name": "payment_term", "label": "Payment Term", "type": "many2one"},
        {"name": "pin_plat_plan_end", "label": "Pbox Plat/Paint End", "type": "datetime"},
        {"name": "pin_plat_output", "label": "Pbox Plat/Paint Output", "type": "float"},
        {"name": "pin_plat_plan_qty", "label": "Pbox Plat/Paint Plan Qty", "type": "float"},
        {"name": "ppl_rec_plan_qty", "label": "Pbox Plat/Paint Recplan Qty", "type": "float"},
        {"name": "pin_plat_plan", "label": "Pbox Plat/Paint Start", "type": "datetime"},
        {"name": "pinbox", "label": "Pinbox", "type": "many2one"},
        {"name": "pinbox_con", "label": "Pinbox C.", "type": "float"},
        {"name": "pinbox_stock", "label": "Pinbox Stock", "type": "float"},
        {"name": "plan_ids", "label": "Plan Ids", "type": "char"},
        {"name": "plating_plan_end", "label": "Plat/Paint End", "type": "datetime"},
        {"name": "plating_output", "label": "Plat/Paint Output", "type": "float"},
        {"name": "plating_plan_qty", "label": "Plat/Paint Plan Qty", "type": "float"},
        {"name": "pl_rec_plan_qty", "label": "Plat/Paint Rceplan Qty", "type": "float"},
        {"name": "plating_plan", "label": "Plat/Paint Start", "type": "datetime"},
        {"name": "product_template_id", "label": "Product", "type": "many2one"},
        {"name": "product_code", "label": "Product Code", "type": "text"},
        {"name": "product_id", "label": "Product Id", "type": "many2one"},
        {"name": "product_uom_qty", "label": "Quantity", "type": "float"},
        {"name": "resign", "label": "Resign", "type": "many2one"},
        {"name": "resign_stock", "label": "Resign Stock", "type": "float"},
        {"name": "is_revised", "label": "Revision", "type": "boolean"},
        {"name": "revision_no", "label": "Revision No", "type": "char"},
        {"name": "revised_status", "label": "Revision Status", "type": "selection"},
        {"name": "slider", "label": "RM Slider", "type": "many2one"},
        {"name": "shade_code", "label": "Shade Code", "type": "text"},
        {"name": "shade", "label": "Shade Name", "type": "text"},
        {"name": "shade_ref", "label": "Shade Ref 1", "type": "text"},
        {"name": "shade_ref_2", "label": "Shade Ref 2", "type": "text"},
        {"name": "shade_ref_3", "label": "Shade Ref 3", "type": "text"},
        {"name": "shape", "label": "Shape", "type": "text"},
        {"name": "shapefin", "label": "Shape Finish", "type": "text"},
        {"name": "sizecm", "label": "Size (CM)", "type": "char"},
        {"name": "sizein", "label": "Size (Inch)", "type": "char"},
        {"name": "sizemm", "label": "Size (MM)", "type": "char"},
        {"name": "slidercodesfg", "label": "Slider", "type": "char"},
        {"name": "sli_asmbl_output", "label": "Slider Asmbl Output", "type": "float"},
        {"name": "sli_asmbl_plan_end", "label": "Slider Asmbl Plan End", "type": "datetime"},
        {"name": "sli_asmbl_plan_qty", "label": "Slider Asmbl Plan Qty", "type": "float"},
        {"name": "sli_asmbl_plan", "label": "Slider Asmbl Plan Start", "type": "datetime"},
        {"name": "sass_rec_plan_qty", "label": "Slider Asmbl Rceplan Qty", "type": "float"},
        {"name": "slider_con", "label": "Slider C.", "type": "float"},
        {"name": "slider_stock", "label": "Slider Stock", "type": "float"},
        {"name": "st_lead_time", "label": "Standard Lead", "type": "integer"},
        {"name": "state", "label": "State", "type": "selection"},
        {"name": "style", "label": "Style", "type": "text"},
        {"name": "tape", "label": "Tape", "type": "many2one"},
        {"name": "tape_con", "label": "Tape C.", "type": "float"},
        {"name": "tape_stock", "label": "Tape Stock", "type": "float"},
        {"name": "tbwire_con", "label": "TBwire C.", "type": "float"},
        {"name": "top", "label": "Top", "type": "many2one"},
        {"name": "top_plat_plan_end", "label": "Top Plat/Paint End", "type": "datetime"},
        {"name": "top_plat_output", "label": "Top Plat/Paint Output", "type": "float"},
        {"name": "top_plat_plan_qty", "label": "Top Plat/Paint Plan Qty", "type": "float"},
        {"name": "tpl_rec_plan_qty", "label": "Top Plat/Paint Recplan Qty", "type": "float"},
        {"name": "top_plat_plan", "label": "Top Plat/Paint Start", "type": "datetime"},
        {"name": "top_stock", "label": "Top Stock", "type": "float"},
        {"name": "topbottom", "label": "Top/Bottom", "type": "char"},
        {"name": "topwire_con", "label": "Topwire C.", "type": "float"},
        {"name": "product_uom", "label": "Unit", "type": "many2one"},
        {"name": "wire", "label": "Wire", "type": "many2one"},
        {"name": "wire_con", "label": "Wire C.", "type": "float"},
        {"name": "wire_stock", "label": "Wire Stock", "type": "float"},
    ]

    export_data = {
        "import_compat": False,
        "context": {
            "lang": "en_US",
            "tz": "Asia/Dhaka",
            "uid": USER_ID,
            "allowed_company_ids": [company_id],
            "order": "date_order asc",
        },
        "domain": [
            "&", "&",
            ["oa_total_balance", ">", 0],
            ["oa_id", "!=", False],
            ["state", "not in", ["closed", "cancel", "hold"]],
        ],
        "fields": export_fields,
        "groupby": [],
        "ids": False,
        "model": "manufacturing.order",
    }

    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/export/xlsx",
        data={"data": json.dumps(export_data)},
    )

    content_type = r.headers.get("content-type", "")
    if "openxmlformats" in content_type or "octet-stream" in content_type:
        print(f"📥 OA Pending XLSX downloaded ({len(r.content)} bytes)")
        return r.content
    else:
        print(f"❌ Unexpected response content-type: {content_type}")
        print(f"   Body: {r.text[:300]}")
        return None


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
