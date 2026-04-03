import os
import json
import datetime
from decimal import Decimal, InvalidOperation

import requests
import gspread


API_VERSION = "v25.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# 画面表示名とAPI上のaction_typeは口径差が出ることがあるため、
# 購入系は候補を複数見にいく
PURCHASE_ACTION_KEYS = [
    "purchase",
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.purchase",
]

MEDIA_NAME = "meta"
TARGET_SHEET_KEY = "gitreport"


def log(message: str) -> None:
    print(message, flush=True)


def load_config() -> dict:
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        raise ValueError("APP_SECRET_JSON が未設定です。")

    try:
        config = json.loads(secret_env)
    except json.JSONDecodeError as e:
        raise ValueError(f"APP_SECRET_JSON のJSON形式が不正です: {e}") from e

    required_keys = ["m_token", "m_act_id", "s_id", "sheets", "g_creds"]
    missing = [k for k in required_keys if k not in config or config.get(k) in (None, "", {})]
    if missing:
        raise ValueError(f"Secretsに不足があります: {', '.join(missing)}")

    if TARGET_SHEET_KEY not in config["sheets"]:
        raise ValueError(f"Secretsの sheets に '{TARGET_SHEET_KEY}' がありません。")

    return config


def normalize_act_id(raw_act_id: str) -> str:
    cleaned = str(raw_act_id).strip()
    cleaned = cleaned.replace("act=", "").replace("act_", "").replace("act", "").strip()
    if not cleaned:
        raise ValueError("m_act_id が空です。")
    return f"act_{cleaned}"


def get_gspread_client(creds_dict: dict) -> gspread.Client:
    return gspread.service_account_from_dict(creds_dict)


def get_target_worksheet(client: gspread.Client, sheet_id_value, worksheet_name: str):
    # s_id が配列でも文字列でも動くように吸収
    if isinstance(sheet_id_value, list):
        if not sheet_id_value:
            raise ValueError("s_id が空配列です。")
        spreadsheet_id = sheet_id_value[0]
    else:
        spreadsheet_id = sheet_id_value

    spreadsheet = client.open_by_key(spreadsheet_id)
    return spreadsheet.worksheet(worksheet_name)


def get_date_range():
    """
    JST基準ではなくGitHub Actions実行環境のUTC日に依存するとズレる可能性があるため、
    環境変数 TZ=Asia/Tokyo を workflow 側で指定する前提。
    """
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    this_month_start = datetime.date(yesterday.year, yesterday.month, 1)
    last_month_end = this_month_start - datetime.timedelta(days=1)
    last_month_start = datetime.date(last_month_end.year, last_month_end.month, 1)

    return last_month_start, yesterday


def decimal_to_str(value) -> str:
    """
    シートに入れる時の余計な指数表記を避ける。
    """
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        normalized = value.normalize()
        s = format(normalized, "f")
        return s.rstrip("0").rstrip(".") if "." in s else s
    return str(value)


def to_decimal(value) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def build_time_range(start_date: datetime.date, end_date: datetime.date) -> str:
    return json.dumps({
        "since": start_date.strftime("%Y-%m-%d"),
        "until": end_date.strftime("%Y-%m-%d"),
    }, ensure_ascii=False)


def fetch_all_insights(object_id: str, params: dict) -> list:
    """
    ページネーション対応。
    """
    url = f"{BASE_URL}/{object_id}/insights"
    results = []

    while url:
        res = requests.get(url, params=params, timeout=120)
        data = res.json()

        if res.status_code >= 400 or "error" in data:
            message = data.get("error", {}).get("message", f"HTTP {res.status_code}")
            raise RuntimeError(f"Meta API error: {message}")

        results.extend(data.get("data", []))

        paging = data.get("paging", {})
        next_url = paging.get("next")
        url = next_url
        params = None  # next には必要パラメータが含まれる

    return results


def extract_action_metric(items: list, candidate_keys: list) -> Decimal:
    """
    actions / action_values から購入系の値を拾う。
    action_type はアカウント実装差が出ることがあるため候補を順に探索。
    """
    if not items:
        return Decimal("0")

    for key in candidate_keys:
        for item in items:
            if item.get("action_type") == key:
                return to_decimal(item.get("value", "0"))

    return Decimal("0")


def get_common_params(token: str, start_date: datetime.date, end_date: datetime.date) -> dict:
    return {
        "access_token": token,
        "time_range": build_time_range(start_date, end_date),
        "fields": "campaign_name,adset_name,ad_name,actions,action_values,date_start",
        "action_attribution_windows": "['1d_click']",
        # オフMetaのWeb購入は mixed だと conversion-based time で返る仕様
        "action_report_time": "mixed",
        "limit": 5000,
    }


def fetch_campaign_day_rows(act_id: str, token: str, start_date: datetime.date, end_date: datetime.date) -> list:
    params = get_common_params(token, start_date, end_date)
    params.update({
        "level": "campaign",
        "time_increment": "1",
    })

    raw_rows = fetch_all_insights(act_id, params)

    rows = []
    for item in raw_rows:
        period = item.get("date_start", "")
        cv_click_1d = extract_action_metric(item.get("actions", []), PURCHASE_ACTION_KEYS)
        sales_click_1d = extract_action_metric(item.get("action_values", []), PURCHASE_ACTION_KEYS)

        rows.append([
            MEDIA_NAME,
            "campaign_day",
            period,
            item.get("campaign_name", ""),
            "",  # adset_name
            "",  # ad_name
            decimal_to_str(cv_click_1d),
            decimal_to_str(sales_click_1d),
        ])

    return rows


def fetch_ad_month_rows(act_id: str, token: str, start_date: datetime.date, end_date: datetime.date) -> list:
    params = get_common_params(token, start_date, end_date)
    params.update({
        "level": "ad",
        "time_increment": "monthly",
    })

    raw_rows = fetch_all_insights(act_id, params)

    rows = []
    for item in raw_rows:
        # monthly の date_start は通常 YYYY-MM-01 で返るため YYYY-MM に整形
        date_start = item.get("date_start", "")
        period = date_start[:7] if len(date_start) >= 7 else date_start

        cv_click_1d = extract_action_metric(item.get("actions", []), PURCHASE_ACTION_KEYS)
        sales_click_1d = extract_action_metric(item.get("action_values", []), PURCHASE_ACTION_KEYS)

        rows.append([
            MEDIA_NAME,
            "ad",
            period,
            item.get("campaign_name", ""),
            item.get("adset_name", ""),
            item.get("ad_name", ""),
            decimal_to_str(cv_click_1d),
            decimal_to_str(sales_click_1d),
        ])

    return rows


def sort_rows(rows: list) -> list:
    """
    並び順:
    1. scope
    2. period 降順
    3. campaign_name
    4. adset_name
    5. ad_name
    """
    return sorted(
        rows,
        key=lambda x: (
            x[1],
            x[2],
            x[3],
            x[4],
            x[5],
        ),
        reverse=False
    )


def write_sheet(worksheet, rows: list) -> None:
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

    output = [header] + rows

    worksheet.clear()
    worksheet.update(output)


def main():
    log("Starting Meta -> Google Sheets job")

    config = load_config()
    token = config["m_token"]
    act_id = normalize_act_id(config["m_act_id"])
    sheet_name = config["sheets"][TARGET_SHEET_KEY]

    start_date, end_date = get_date_range()
    log(f"Date range: {start_date} to {end_date}")
    log(f"Account: {act_id[-4:]}")

    client = get_gspread_client(config["g_creds"])
    worksheet = get_target_worksheet(client, config["s_id"], sheet_name)

    campaign_day_rows = fetch_campaign_day_rows(act_id, token, start_date, end_date)
    log(f"campaign_day rows: {len(campaign_day_rows)}")

    ad_rows = fetch_ad_month_rows(act_id, token, start_date, end_date)
    log(f"ad rows: {len(ad_rows)}")

    all_rows = campaign_day_rows + ad_rows
    all_rows = sort_rows(all_rows)

    write_sheet(worksheet, all_rows)
    log(f"Done. Wrote {len(all_rows)} rows to '{sheet_name}'")


if __name__ == "__main__":
    main()