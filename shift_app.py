import re
import secrets
import hashlib
from datetime import datetime, date, time
from typing import List, Dict

import pandas as pd
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError
from icalendar import Calendar, Event
from streamlit_autorefresh import st_autorefresh
from zoneinfo import ZoneInfo

# =====================================================
# GLOBAL TIMEZONE (DST SAFE)
# =====================================================

APP_TZ = ZoneInfo("America/New_York")

# =====================================================
# CONFIG
# =====================================================

ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2

FIXED_MD_COLORS = {"yellow", "purple", "blue", "bronze", "green", "orange"}

# =====================================================
# UTILITIES
# =====================================================

def badge(text, color):
    st.markdown(
        f"<span style='background-color:{color};padding:4px 10px;border-radius:12px;font-size:12px;font-weight:600;color:white;'>{text}</span>",
        unsafe_allow_html=True,
    )

def normalize_title(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[^A-Za-z0-9\s\-]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def clean_input(text):
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())

def get_day_number(d):
    delta = (d - ANCHOR_DATE).days
    return (delta + ANCHOR_DAY_NUM - 1) % 4 + 1

def shift_times(shift_type):
    if shift_type == "Early":
        return time(6, 45), time(17, 0)
    if shift_type == "Middle":
        return time(8, 0), time(17, 0)
    if shift_type == "Late":
        return time(8, 0), time(18, 45)
    if shift_type == "Fixed":
        return time(8, 0), time(17, 0)
    return None, None

# =====================================================
# SHIFT RULE ENGINE
# =====================================================

def calc_shift_type(title, target_date, role):
    clean_shift = clean_input(normalize_title(title))

    if role == "MD":
        clean_shift = clean_shift.replace("gray 1 collaborator", "gray 1 md")
        clean_shift = clean_shift.replace("gray 2 collaborator", "gray 2 md")

    day_num = get_day_number(target_date)

    if role == "MD":
        first = clean_shift.split(" ")[0]
        if first in FIXED_MD_COLORS:
            return "Fixed"

    if clean_shift.startswith("gold"):
        m = re.search(r"(\d+)", clean_shift)
        if not m:
            return None
        n = int(m.group(1))
        if n == 1:
            return "Early"
        if n >= 6:
            return "Middle"
        if n in (3, 5):
            return "Early" if day_num in (1, 3) else "Middle"
        if n in (2, 4):
            return "Middle" if day_num in (1, 3) else "Early"

    if clean_shift.startswith("silver"):
        m = re.search(r"(\d+)", clean_shift)
        n = int(m.group(1)) if m else 1
        return "Early" if n == 1 else "Middle"

    return None  # everything else untouched

# =====================================================
# ICS BUILDER
# =====================================================

def stable_uid(feed_id, title, start_dt):
    raw = f"{feed_id}|{title}|{start_dt}".encode()
    return hashlib.sha256(raw).hexdigest()[:20] + "@hmu-shifts"

def build_output_ics(processed_rows, untouched_rows, feed_id="download"):
    cal = Calendar()
    cal.add("prodid", "-//HMU Shift Processor//EN")
    cal.add("version", "2.0")
    cal.add("X-WR-TIMEZONE", "America/New_York")

    def add_event(r):
        ev = Event()
        ev.add("summary", r["title"])
        ev.add("uid", stable_uid(feed_id, r["title"], r["start_dt"]))
        ev.add("dtstamp", datetime.now(APP_TZ))
        ev.add("dtstart", r["start_dt"])
        ev.add("dtend", r["end_dt"])
        cal.add_component(ev)

    for r in processed_rows:
        add_event(r)
    for r in untouched_rows:
        add_event(r)

    return cal.to_ical()

# =====================================================
# S3 HELPERS
# =====================================================

def s3_client():
    return boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )

def get_existing_token(bucket, key):
    try:
        resp = s3_client().head_object(Bucket=bucket, Key=key)
        return resp.get("Metadata", {}).get("owner-token")
    except ClientError:
        return None

def upload_feed(bucket, key, ics_bytes, token):
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=ics_bytes,
        ContentType="text/calendar; charset=utf-8",
        CacheControl="no-cache, max-age=0",
        Metadata={"owner-token": token},
    )

# =====================================================
# STREAMLIT APP
# =====================================================

st.set_page_config(page_title="HMU Shift Processor")
st.title("HMU Shift Processor")

role = st.selectbox("Role", ["MD", "APP"], index=0)

st_autorefresh(interval=3600 * 1000, key="auto_refresh")

input_mode = st.radio("Input method", ["Subscription URL", "Upload ICS file"])

today = date.today()
default_end = date(today.year + 1, 6, 30)

start_date = st.date_input("Start date", today)
end_date = st.date_input("End date", default_end)

cal_text = None

if input_mode == "Subscription URL":
    url = st.text_input("Paste ICS subscription URL")
    if st.button("Sync Now") and url:
        st.cache_data.clear()
    if url:
        cal_text = requests.get(url).text
else:
    up = st.file_uploader("Upload ICS file", type=["ics"])
    if up:
        cal_text = up.getvalue().decode()

if not cal_text:
    st.stop()

cal = Calendar.from_ical(cal_text)

processed_rows = []
untouched_rows = []

for comp in cal.walk():
    if comp.name != "VEVENT":
        continue

    title = str(comp.get("SUMMARY", "")).strip()
    dtstart = comp.get("DTSTART").dt
    dtend = comp.get("DTEND").dt if comp.get("DTEND") else None

    if not isinstance(dtstart, datetime):
        continue

    dtstart = dtstart.astimezone(APP_TZ)
    d = dtstart.date()

    if not (start_date <= d <= end_date):
        continue

    shift = calc_shift_type(title, d, role)

    if shift:
        start_t, end_t = shift_times(shift)
        new_start = datetime.combine(d, start_t, tzinfo=APP_TZ)
        new_end = datetime.combine(d, end_t, tzinfo=APP_TZ)
        processed_rows.append({"title": title, "start_dt": new_start, "end_dt": new_end})
    else:
        untouched_rows.append({
            "title": title,
            "start_dt": dtstart,
            "end_dt": dtend.astimezone(APP_TZ) if isinstance(dtend, datetime) else dtstart
        })

# SUMMARY
st.markdown("---")
col1, col2 = st.columns(2)
col1.metric("Processed", len(processed_rows))
col2.metric("Left Untouched", len(untouched_rows))

# DOWNLOAD
ics_bytes = build_output_ics(processed_rows, untouched_rows)
st.download_button("Download output as ICS", ics_bytes, "hmu_output.ics")

# =====================================================
# PUBLISH
# =====================================================

st.markdown("---")
st.subheader("Publish Subscription Feed")

st.markdown(
    "You can publish a subscription URL (paste into Outlook once). "
    "A token is needed if you later republish/update the calendar subscription URL "
    "associated with this Feed ID. "
    "This is intended to prevent multiple people from using the same Feed ID "
    "and inadvertently overwriting the others' calendars."
)

st.warning(
    "Important: The subscription URL does NOT automatically update in the background. "
    "If your source calendar changes, you must reopen this app and click Sync Now "
    "to re-parse and republish the updated feed. "
    "Otherwise, the subscription calendar will remain unchanged."
)

feed_id = st.text_input("Feed ID (3–40 letters/numbers/_/-)")

if feed_id and re.match(r"^[a-zA-Z0-9_-]{3,40}$", feed_id):
    bucket = st.secrets["S3_BUCKET"]
    key = f"feeds/{feed_id}.ics"

    existing_token = get_existing_token(bucket, key)

    if existing_token:
        st.warning("Feed ID already exists.")
        entered = st.text_input("Enter ownership token to update:", type="password")
        if entered == existing_token:
            upload_feed(bucket, key, ics_bytes, existing_token)
            st.success("Feed updated successfully.")
        elif entered:
            st.error("Incorrect token.")
    else:
        token = secrets.token_hex(6).upper()
        upload_feed(bucket, key, ics_bytes, token)
        st.success("Feed created.")
        st.write("Save this ownership token:")
        st.code(token)
        st.write("Subscription URL:")
        st.code(f"https://{bucket}.s3.{st.secrets['AWS_REGION']}.amazonaws.com/{key}")
