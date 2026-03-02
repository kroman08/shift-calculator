# ============================================================
# HMU SHIFT PROCESSOR – CLEAN FINAL VERSION
# ============================================================

import re
import hashlib
from datetime import datetime, date, time, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError

from icalendar import Calendar, Event
from dateutil.rrule import rrulestr
from streamlit_autorefresh import st_autorefresh
from zoneinfo import ZoneInfo

# ============================================================
# CONFIG
# ============================================================

APP_TZ = ZoneInfo("America/New_York")
ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2

SPECIAL_BLUE_PERIODS = [
    (date(2025, 7, 7),  date(2025, 8, 3)),
    (date(2025, 9, 29), date(2025, 10, 26)),
    (date(2026, 1, 5),  date(2026, 2, 1)),
    (date(2026, 4, 6),  date(2026, 5, 3)),
]

FIXED_MD_COLORS = {"yellow", "purple", "blue", "bronze", "green", "orange"}

# ============================================================
# S3
# ============================================================

def s3_client():
    return boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )

def head_feed(bucket: str, key: str):
    try:
        return s3_client().head_object(Bucket=bucket, Key=key)
    except ClientError:
        return None

def put_feed(bucket: str, key: str, body: bytes, metadata: Dict[str, str]):
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/calendar; charset=utf-8",
        CacheControl="no-cache, max-age=0",
        Metadata=metadata,
    )

# ============================================================
# SHIFT LOGIC
# ============================================================

def normalize_title(s: Any) -> str:
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[^A-Za-z0-9\s\-]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def get_day_number(d: date) -> int:
    delta = (d - ANCHOR_DATE).days
    return (delta + ANCHOR_DAY_NUM - 1) % 4 + 1

def shift_times(shift_type: str) -> Tuple[time, time]:
    if shift_type == "Early":
        return time(6,45), time(17,0)
    if shift_type == "Middle":
        return time(8,0), time(17,0)
    if shift_type == "Late":
        return time(8,0), time(18,45)
    if shift_type == "Fixed":
        return time(8,0), time(17,0)
    return time(8,0), time(17,0)

def calc_shift_type(title: str, d: date, role: str) -> Optional[str]:
    t = normalize_title(title).lower()
    day_num = get_day_number(d)

    if role == "MD":
        if any(t.startswith(color) for color in FIXED_MD_COLORS):
            return "Fixed"

    if t.startswith("gold"):
        m = re.search(r"(\d+)", t)
        if not m:
            return None
        n = int(m.group(1))
        if n == 1:
            return "Early"
        if n >= 6:
            return "Middle"
        if n in (3,5):
            return "Early" if day_num in (1,3) else "Middle"
        if n in (2,4):
            return "Middle" if day_num in (1,3) else "Early"

    if t.startswith("silver"):
        m = re.search(r"(\d+)", t)
        if not m:
            return "Early"
        return "Early" if int(m.group(1)) == 1 else "Middle"

    return None

# ============================================================
# ICS PARSING
# ============================================================

@st.cache_data(ttl=3600)
def fetch_ics(url: str) -> str:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text

def expand_events(cal_text: str, start_d: date, end_d: date):
    cal = Calendar.from_ical(cal_text)
    events = []
    errors = []

    for comp in cal.walk("VEVENT"):
        try:
            summary = str(comp.get("SUMMARY",""))
            start = comp.decoded("DTSTART")
            end = comp.decoded("DTEND")

            if isinstance(start, datetime):
                start = start.astimezone(APP_TZ)
                end = end.astimezone(APP_TZ)

                if start.date() < start_d or start.date() > end_d:
                    continue

                events.append({
                    "title": summary,
                    "start_dt": start,
                    "end_dt": end,
                    "all_day": False,
                    "uid": str(comp.get("UID",""))
                })

        except Exception as e:
            errors.append((summary, str(e)))

    return events, errors

# ============================================================
# OUTPUT ICS
# ============================================================

def build_output_ics(processed, untouched, feed_id):
    cal = Calendar()
    cal.add("prodid", "-//HMU Shift Processor//EN")
    cal.add("version", "2.0")
    cal.add("X-WR-TIMEZONE", "America/New_York")

    now = datetime.now(APP_TZ)

    # Last Updated marker
    marker = Event()
    marker.add("summary", f"HMU Shifts — Last Updated: {now.strftime('%Y-%m-%d %H:%M')}")
    marker.add("uid", f"hmushifts-last-updated@{feed_id}")
    marker.add("dtstamp", now)
    start_marker = datetime.combine(now.date(), time(0,1)).replace(tzinfo=APP_TZ)
    marker.add("dtstart", start_marker)
    marker.add("dtend", start_marker + timedelta(minutes=1))
    marker.add("transp", "TRANSPARENT")
    cal.add_component(marker)

    for e in processed + untouched:
        ev = Event()
        ev.add("summary", e["title"])
        ev.add("uid", e["uid"] or hashlib.sha256(e["title"].encode()).hexdigest())
        ev.add("dtstamp", now)
        ev.add("dtstart", e["start_dt"])
        ev.add("dtend", e["end_dt"])
        cal.add_component(ev)

    return cal.to_ical()

# ============================================================
# STREAMLIT UI
# ============================================================

st.set_page_config(page_title="HMU Shift Processor", layout="centered")
st.title("HMU Shift Processor")

st_autorefresh(interval=3600*1000, key="auto_refresh")

today = date.today()
default_end = date(today.year+1, 6, 30)

role = st.selectbox("Role", ["MD","APP"], index=0)
window_start = st.date_input("Start Date", value=today)
window_end = st.date_input("End Date", value=default_end)

input_mode = st.radio("Input Method", ["Subscription URL","Upload ICS"])

cal_text = None
url = None

if input_mode == "Subscription URL":
    url = st.text_input("ICS Subscription URL")
    if url:
        try:
            cal_text = fetch_ics(url)
        except Exception as e:
            st.error(f"Unable to fetch calendar: {e}")

else:
    upload = st.file_uploader("Upload ICS file", type=["ics"])
    if upload:
        cal_text = upload.read().decode("utf-8")

# ============================================================
# PUBLISH / RESTORE (ALWAYS VISIBLE)
# ============================================================

st.subheader("Publish Subscription Feed")

feed_id = st.text_input("Feed ID", key="feed_id")
st.caption("*Forgot Feed ID? In your calendar app, open the subscribed calendar info. URL shows .../feeds/yourFeedID.ics*")

token = st.text_input("Ownership Token", type="password", key="token")
st.caption("*Ownership token format: First initial + Middle initial + Last initial + Birth month (MM). Example: JMS01*")

bucket = st.secrets["S3_BUCKET"]
region = st.secrets["AWS_REGION"]

col_pub, col_restore = st.columns(2)

# ============================================================
# RESTORE BUTTON
# ============================================================

with col_restore:
    if st.button("Restore Feed"):
        if not feed_id or not token:
            st.error("Feed ID and Ownership Token required.")
        else:
            key = f"feeds/{feed_id}.ics"
            existing = head_feed(bucket, key)
            if not existing:
                st.error("Feed not found.")
            else:
                stored_token = existing["Metadata"].get("owner-token","")
                if token.strip().upper() != stored_token.upper():
                    st.error("Incorrect ownership token.")
                else:
                    restored_url = existing["Metadata"].get("source-url","")
                    restored_role = existing["Metadata"].get("role","MD")
                    restored_end = existing["Metadata"].get("window-end","")

                    if not restored_url:
                        st.error("No source URL stored.")
                    else:
                        cal_text = fetch_ics(restored_url)
                        role = restored_role
                        window_start = date.today()
                        window_end = date.fromisoformat(restored_end)

                        events, _ = expand_events(cal_text, window_start, window_end)

                        processed = []
                        untouched = []

                        for e in events:
                            shift = calc_shift_type(e["title"], e["start_dt"].date(), role)
                            if shift:
                                st_t, en_t = shift_times(shift)
                                new_start = datetime.combine(e["start_dt"].date(), st_t).replace(tzinfo=APP_TZ)
                                new_end = datetime.combine(e["start_dt"].date(), en_t).replace(tzinfo=APP_TZ)
                                processed.append({**e,"start_dt":new_start,"end_dt":new_end})
                            else:
                                untouched.append(e)

                        final_ics = build_output_ics(processed, untouched, feed_id)

                        put_feed(bucket, key, final_ics, {
                            "owner-token": stored_token,
                            "source-url": restored_url,
                            "role": role,
                            "window-end": str(window_end)
                        })

                        sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

                        st.success("Feed restored and republished.")
                        st.code(sub_url)
                        st.warning(
                            "Important: The subscription URL does NOT automatically update in the background. "
                            "If your source calendar changes, reopen this app and republish."
                        )

# ============================================================
# PUBLISH BUTTON
# ============================================================

with col_pub:
    if st.button("Publish Feed"):
        if not cal_text:
            st.error("Provide a source calendar first.")
        elif not feed_id or not token:
            st.error("Feed ID and Ownership Token required.")
        else:
            events, _ = expand_events(cal_text, window_start, window_end)

            processed = []
            untouched = []

            for e in events:
                shift = calc_shift_type(e["title"], e["start_dt"].date(), role)
                if shift:
                    st_t, en_t = shift_times(shift)
                    new_start = datetime.combine(e["start_dt"].date(), st_t).replace(tzinfo=APP_TZ)
                    new_end = datetime.combine(e["start_dt"].date(), en_t).replace(tzinfo=APP_TZ)
                    processed.append({**e,"start_dt":new_start,"end_dt":new_end})
                else:
                    untouched.append(e)

            key = f"feeds/{feed_id}.ics"
            final_ics = build_output_ics(processed, untouched, feed_id)

            put_feed(bucket, key, final_ics, {
                "owner-token": token.strip().upper(),
                "source-url": url or "",
                "role": role,
                "window-end": str(window_end)
            })

            sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

            st.success("Feed published successfully.")
            st.code(sub_url)
            st.warning(
                "Important: The subscription URL does NOT automatically update in the background. "
                "If your source calendar changes, reopen this app and republish."
            )