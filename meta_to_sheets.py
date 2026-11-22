import os
import json
import sys
import logging
import math
import re
import time
import uuid
from contextlib import contextmanager
from urllib.parse import parse_qs, urlsplit
import datetime
from decimal import Decimal, InvalidOperation

import requests
import gspread


API_VERSION = "v27.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"
TARGET_SHEET_KEY = "gitreport"
MEDIA_NAME = "meta"

PURCHASE_ACTION_KEYS = [
    "purchase",
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
    "onsite_conversion.purchase",
]

HEADER = ["media", "scope", "period", "campaign_name", "adset_name", "ad_name",
          "cv_click_1d", "sales_click_1d"]


def load_config():
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        raise ExportError("APP_SECRET_JSON が未設定です。")

    try:
        config = json.loads(secret_env)
    except json.JSONDecodeError:
        raise ExportError("APP_SECRET_JSON のJSONが不正です。") from None

    required = ["m_token", "m_act_id", "s_id", "sheets", "g_creds"]
    missing = [k for k in required if k not in config or config.get(k) in (None, "", {})]
    if missing:
        raise ExportError(f"Secretsの必須項目不足: {missing}")

    if TARGET_SHEET_KEY not in config["sheets"]:
        raise ExportError("sheets に gitreport がありません。")

    return config


def normalize_act_id(raw_act_id):
    s = str(raw_act_id).strip()
    s = s.replace("act=", "").replace("act_", "").replace("act", "").strip()
    if not re.fullmatch(r"[0-9]+", s):
        raise ExportError("m_act_id の形式が不正です。")
    return f"act_{s}"


def get_spreadsheet_id(s_id):
    if isinstance(s_id, list):
        if not s_id:
            raise ExportError("s_id が空配列です。")
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


def to_decimal(value):
    try:
        result = Decimal(str(value))
        if not result.is_finite() or not math.isfinite(float(result)):
            raise ValueError
    except (InvalidOperation, OverflowError, ValueError):
        raise ExportError("Meta API returned an invalid purchase value.") from None
    return result


def decimal_to_sheet_number(value):
    if value is None:
        return 0

    if not isinstance(value, Decimal):
        value = to_decimal(value)

    if value == value.to_integral_value():
        return int(value)
    return float(value)


def extract_metric(items, keys, attribution_key="1d_click"):
    """
    actions / action_values から、対象 action_type の指定attribution値のみ取得する。
    attribution_key が無い場合は 0 を返す。
    """
    if not isinstance(items, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("action_type"), str)
        or not item["action_type"] for item in items
    ):
        raise ExportError("Meta API returned invalid actions.")
    if not items:
        return Decimal("0")

    for key in keys:
        for item in items:
            if item.get("action_type") == key:
                return to_decimal(item.get(attribution_key, 0))

    return Decimal("0")


def next_cursor(endpoint, paging, params):
    if not isinstance(paging["next"], str):
        raise ExportError("Meta API returned invalid paging.")
    target = urlsplit(paging["next"])
    expected = urlsplit(endpoint)
    if (target.scheme, target.netloc, target.path) != (expected.scheme, expected.netloc, expected.path):
        raise ExportError("Meta API returned an unexpected paging destination.")
    query = parse_qs(target.query, keep_blank_values=True)
    if target.fragment or set(query) - (set(params) | {"after", "access_token"}):
        raise ExportError("Meta API returned unsupported paging.")
    cursors = paging.get("cursors", {})
    if not isinstance(cursors, dict):
        raise ExportError("Meta API returned invalid cursors.")
    candidates = query.get("after", [])
    if len(candidates) > 1:
        raise ExportError("Meta API returned ambiguous paging.")
    cursor = cursors.get("after", candidates[0] if candidates else None)
    if not isinstance(cursor, str) or not cursor or (candidates and candidates != [cursor]):
        raise ExportError("Meta API returned unsupported paging.")
    return cursor


class ExportError(Exception):
    """Contains only fixed operation labels and validated numeric diagnostics."""


def request_json(label, method, url, **kwargs):
    try:
        response = requests.request(method, url, allow_redirects=False, **kwargs)
    except requests.RequestException:
        raise ExportError(f"{label} connection failed.") from None
    status = response.status_code
    try:
        payload = response.json()
    except ValueError:
        raise ExportError(f"{label} returned invalid JSON (HTTP {status}).") from None
    if not 200 <= status < 300 or (isinstance(payload, dict) and "error" in payload):
        details = []
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            for key in ("code", "error_subcode"):
                if type(error.get(key)) is int:
                    details.append(f"{key}={error[key]}")
        reason = ", ".join([f"HTTP {status}", *details])
        raise ExportError(f"{label} failed ({reason}).")
    return payload


def fetch_rows(act_id, token, start_date, end_date, level):
    daily = level == "campaign"
    url = f"{BASE_URL}/{act_id}/insights"
    params = {
        "level": level,
        "time_increment": "1" if daily else "monthly",
        "time_range": build_time_range(start_date, end_date),
        "fields": "campaign_name,adset_name,ad_name,actions,action_values,date_start",
        "action_attribution_windows": '["1d_click"]',
        "limit": 5000,
    }
    seen = set()
    rows = []
    required = ("date_start", "campaign_name") if daily else ("date_start", "campaign_name", "adset_name", "ad_name")
    while True:
        payload = request_json("Meta API", "GET", url, headers={"Authorization": f"Bearer {token}"},
                               params=params, timeout=120)
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ExportError("Meta API returned invalid data.")
        for item in payload["data"]:
            if not isinstance(item, dict) or any(not isinstance(item.get(k), str) or not item[k] for k in required):
                raise ExportError("Meta API returned an incomplete row.")
            day = item["date_start"]
            try:
                parsed = datetime.date.fromisoformat(day)
            except ValueError:
                raise ExportError("Meta API returned an invalid date.") from None
            if parsed.isoformat() != day or not start_date <= parsed <= end_date:
                raise ExportError("Meta API returned an invalid reporting date.")
            cv = extract_metric(item.get("actions", []), PURCHASE_ACTION_KEYS)
            sales = extract_metric(item.get("action_values", []), PURCHASE_ACTION_KEYS)
            rows.append([
                MEDIA_NAME, "campaign_day" if daily else "ad", day if daily else day[:7],
                item["campaign_name"], "" if daily else item["adset_name"], "" if daily else item["ad_name"],
                decimal_to_sheet_number(cv), decimal_to_sheet_number(sales),
            ])
        paging = payload.get("paging", {})
        if not isinstance(paging, dict):
            raise ExportError("Meta API returned invalid paging.")
        if "next" not in paging:
            return rows
        cursor = next_cursor(url, paging, params)
        if cursor in seen or token in cursor:
            raise ExportError("Meta API returned an invalid or repeated cursor.")
        seen.add(cursor)
        params["after"] = cursor


LOCK_NAME = "fry_export_lock_v1"
LOCK_PREFIX = "_fry_export_"
LEASE_SECONDS = 3 * 60 * 60


def active_lock(book):
    metadata = book.fetch_sheet_metadata(params={
        "fields": "sheets(properties(sheetId,title,hidden)),namedRanges(namedRangeId,name,range(sheetId))"
    })
    for item in metadata.get("namedRanges", []):
        if item["name"].casefold() != LOCK_NAME:
            continue
        guard_id = item["namedRangeId"]
        sheet_id = item["range"]["sheetId"]
        for sheet in metadata["sheets"]:
            props = sheet["properties"]
            if props["sheetId"] != sheet_id:
                continue
            title = props["title"]
            match = re.fullmatch(LOCK_PREFIX + r"([0-9]+)_([a-f0-9]{32})", title)
            if match and props.get("hidden") and guard_id == "fry_" + match[2]:
                return (guard_id, sheet_id, title, int(match[1]))
        raise ExportError("Sheets lock metadata is invalid.")
    return None


def remove_lock(owner):
    # Both operations are atomic. A retired owner cannot delete a successor's tab.
    return [
        {"deleteNamedRange": {"namedRangeId": owner[0]}},
        {"deleteSheet": {"sheetId": owner[1]}},
    ]


@contextmanager
def export_lock(book):
    previous = active_lock(book)
    now = int(time.time())
    if previous and previous[3] > now:
        raise ExportError("Another export holds the Sheets lock.")
    nonce = uuid.uuid4().hex
    sheet_id = 1 + uuid.uuid4().int % 2147483646
    expires = now + LEASE_SECONDS
    title = f"{LOCK_PREFIX}{expires}_{nonce}"
    guard_id = "fry_" + nonce
    owner = (guard_id, sheet_id, title, expires)
    commands = remove_lock(previous) if previous else []
    commands.extend([
        {"addSheet": {"properties": {
            "sheetId": sheet_id, "title": title, "hidden": True,
            "gridProperties": {"rowCount": 1, "columnCount": 1},
        }}},
        {"addNamedRange": {"namedRange": {
            "namedRangeId": guard_id, "name": LOCK_NAME,
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": 1},
        }}},
    ])
    try:
        book.batch_update({"requests": commands})
    except Exception:
        if active_lock(book) != owner:
            raise ExportError("Sheets lock acquisition failed or another export is running.") from None
    try:
        yield owner
    finally:
        try:
            book.batch_update({"requests": remove_lock(owner)})
        except Exception:
            if active_lock(book) == owner:
                print("Sheets lock release unconfirmed. The lease expires automatically.", file=sys.stderr)


def publish(book, owner, worksheet, rows):
    # Count previous rows using column A. Every report row has a media value there.
    tab = "'" + worksheet.title.replace("'", "''") + "'"
    previous = book.values_get(f"{tab}!A:A", params={"valueRenderOption": "FORMULA"})
    previous_height = len(previous.get("values", []))
    values = [HEADER, *rows]
    height = len(values)
    values.extend([[""] * len(HEADER) for _ in range(previous_height - height)])
    growth = []
    for dimension, need, current in (("ROWS", height, worksheet.row_count),
                                      ("COLUMNS", len(HEADER), worksheet.col_count)):
        if need > current:
            growth.append({"appendDimension": {"sheetId": worksheet.id, "dimension": dimension,
                                               "length": need - current}})
    if growth:
        guard = {"updateNamedRange": {"namedRange": {"namedRangeId": owner[0], "name": LOCK_NAME},
                                      "fields": "name"}}
        book.batch_update({"requests": [guard, *growth]})
    # Removing the unique owner tab on takeover invalidates any late write by that runner.
    marker = f"'{owner[2]}'!A1"
    body = {"valueInputOption": "USER_ENTERED", "data": [
        {"range": marker, "values": [["PUBLISHED"]]},
        {"range": f"{tab}!A1", "values": values},
    ]}
    try:
        book.values_batch_update(body)
    except Exception:
        try:
            receipt = book.values_get(marker)
        except Exception:
            raise ExportError("Sheets publication outcome is unknown.") from None
        if receipt.get("values") != [["PUBLISHED"]]:
            raise ExportError("Sheets publication was not confirmed.") from None


def main():
    config = load_config()
    token = config["m_token"]
    if not isinstance(token, str) or not token.strip():
        raise ExportError("Invalid Meta token configuration.")
    act_id = normalize_act_id(config["m_act_id"])
    book = gspread.service_account_from_dict(config["g_creds"]).open_by_key(get_spreadsheet_id(config["s_id"]))
    worksheet = book.worksheet(config["sheets"][TARGET_SHEET_KEY])
    with export_lock(book) as owner:
        start, end = get_dates()
        print(f"Date range: {start} to {end}")
        campaign = fetch_rows(act_id, token, start, end, "campaign")
        ads = fetch_rows(act_id, token, start, end, "ad")
        publish(book, owner, worksheet, campaign + ads)
        print(f"Export complete: campaign_day={len(campaign)}, ad={len(ads)}.")


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        main()
    except ExportError as error:
        sys.exit(str(error))
    except Exception:
        sys.exit("Export failed. Check configuration, response format or Sheets access.")
