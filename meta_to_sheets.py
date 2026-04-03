import os
import json
import sys
import datetime
from decimal import Decimal, InvalidOperation

import requests
import gspread


API_VERSION = "v25.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
TARGET_SHEET_KEY = "gitreport"
MEDIA_NAME = "meta"

PURCHASE_ACTION_KEYS = [
    "purchase",
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.purchase",
]


def log(msg: str):
    print(msg, flush=True)


def fail(msg: str):
    print(f"ERROR: {msg}", flush=True)
    sys.exit(1)


def load_config():
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        fail("APP_SECRET_JSON が未設定です。")

    try:
        config = json.loads(secret_env)
    except json.JSONDecodeError as e:
        fail(f"APP_SECRET_JSON のJSONが不正です: {e}")

    required = ["m_token", "m_act_id", "s_id", "sheets", "g_creds"]
    missing = [k for k in required if k not in config or config.get(k) in (None, "", {})]
    if missing:
        fail(f"Secretsの必須項目不足: {missing}")

    if TARGET_SHEET_KEY not in config["sheets"]:
        fail(f"sheets に '{TARGET_SHEET_KEY}' がありません。現在の keys: {list(config['sheets'].keys())}")

    return config


def normalize_act_id(raw_act_id):
    s = str(raw_act_id).strip()
    s = s.replace("act=", "").replace("act_", "").replace("act", "").strip()
    if not s:
        fail("m_act_id が空です。")
    return f"act_{s}"


def get_spreadsheet_id(s_id):
    if isinstance(s_id, list):
        if not s_id:
            fail("s_id が空配列です。")
        return s_id[0]
    return s_id


def get_dates():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    this_month_start = datetime.date(yesterday.year, yesterday.month, 1)
    last_month_end = this_month_start - datetime.timedelta(days=1)
    last_month_start = datetime.date(last_month_end.year, last_month_end.month, 1)
    return last_month_start, yesterday


def build_time_range(start_date, end_date):
    return json.dumps({
        "since": start_date.strftime("%Y-%m-%d"),
        "until": end_date.strftime("%Y-%m-%d"),
    })


def to_decimal(v):
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def decimal_to_str(v):
    if isinstance(v, Decimal):
        s = format(v.normalize(), "f")
        return s.rstrip("0").rstrip(".") if "." in s else s
    return str(v)


def extract_metric(items, keys):
    if not items:
        return Decimal("0")
    for key in keys:
        for item in items:
            if item.get("action_type") == key:
                return to_decimal(item.get("value", 0))
    return Decimal("0")


def fetch_all_insights(act_id, params):
    url = f"{BASE_URL}/{act_id}/insights"
    all_data = []
    page = 1

    while url:
        log(f"Meta API request page={page}")
        try:
            res = requests.get(url, params=params, timeout=120)
        except Exception as e:
            fail(f"Meta API request 失敗: {e}")

        try:
            data = res.json()
        except Exception:
            fail(f"Meta API response がJSONではありません。status={res.status_code}, body={res.text[:1000]}")

        if res.status_code >= 400 or "error" in data:
            fail(f"Meta API error: status={res.status_code}, detail={data}")

        batch = data.get("data", [])
        all_data.extend(batch)
        log(f"  -> rows fetched: {len(batch)}")

        url = data.get("paging", {}).get("next")
        params = None
        page += 1

    return all_data


def fetch_campaign_day_rows(act_id, token, start_date, end_date):
    params = {
        "access_token": token,
        "level": "campaign",
        "time_increment": "1",
        "time_range": build_time_range(start_date, end_date),
        "fields": "campaign_name,adset_name,ad_name,actions,action_values,date_start",
        "action_attribution_windows": ["1d_click"],
        "action_report_time": "conversion",
        "limit": 5000,
    }

    raw = fetch_all_insights(act_id, params)
    rows = []
    for item in raw:
        cv = extract_metric(item.get("actions", []), PURCHASE_ACTION_KEYS)
        sales = extract_metric(item.get("action_values", []), PURCHASE_ACTION_KEYS)

        rows.append([
            MEDIA_NAME,
            "campaign_day",
            item.get("date_start", ""),
            item.get("campaign_name", ""),
            "",
            "",
            float(cv),
            float(sales),
        ])
    return rows


def fetch_ad_month_rows(act_id, token, start_date, end_date):
    params = {
        "access_token": token,
        "level": "ad",
        "time_increment": "monthly",
        "time_range": build_time_range(start_date, end_date),
        "fields": "campaign_name,adset_name,ad_name,actions,action_values,date_start",
        "action_attribution_windows": ["1d_click"],
        "action_report_time": "conversion",
        "limit": 5000,
    }

    raw = fetch_all_insights(act_id, params)
    rows = []
    for item in raw:
        cv = extract_metric(item.get("actions", []), PURCHASE_ACTION_KEYS)
        sales = extract_metric(item.get("action_values", []), PURCHASE_ACTION_KEYS)

        date_start = item.get("date_start", "")
        period = date_start[:7] if len(date_start) >= 7 else date_start

        rows.append([
            MEDIA_NAME,
            "ad",
            period,
            item.get("campaign_name", ""),
            item.get("adset_name", ""),
            item.get("ad_name", ""),
            float(cv),
            float(sales),
        ])
    return rows


def main():
    log("=== Start Meta to Sheets ===")
    config = load_config()

    token = config["m_token"]
    act_id = normalize_act_id(config["m_act_id"])
    spreadsheet_id = get_spreadsheet_id(config["s_id"])
    worksheet_name = config["sheets"][TARGET_SHEET_KEY]

    start_date, end_date = get_dates()

    log(f"Date range: {start_date} to {end_date}")
    log(f"Spreadsheet ID exists: {'yes' if spreadsheet_id else 'no'}")
    log(f"Worksheet: {worksheet_name}")
    log(f"Act ID tail: {act_id[-4:]}")

    # Google Sheets
    try:
        client = gspread.service_account_from_dict(config["g_creds"])
        sh = client.open_by_key(spreadsheet_id)
        ws = sh.worksheet(worksheet_name)
        log("Google Sheets connection: OK")
    except Exception as e:
        fail(f"Google Sheets 接続失敗: {e}")

    # Meta
    campaign_rows = fetch_campaign_day_rows(act_id, token, start_date, end_date)
    log(f"campaign_day rows = {len(campaign_rows)}")

    ad_rows = fetch_ad_month_rows(act_id, token, start_date, end_date)
    log(f"ad rows = {len(ad_rows)}")

    rows = campaign_rows + ad_rows

    header = [
        "media",
        "scope",
        "period",
        "campaign_name",
        "adset_name",
        "ad_name",
        "cv_click_1d",
        "sales_click_1d",
    ]

    try:
        ws.clear()
        ws.update([header] + rows)
        log(f"Write success: {len(rows)} rows")
    except Exception as e:
        fail(f"シート書き込み失敗: {e}")

    log("=== Finished successfully ===")


if __name__ == "__main__":
    main()
