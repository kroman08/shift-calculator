import re
import secrets
from datetime import datetime, date, timedelta

import pandas as pd
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError

from icalendar import Calendar
from streamlit_autorefresh import st_autorefresh

# =====================================================
# CONFIG
# =====================================================

ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2

SPECIAL_BLUE_PERIODS = [
    (date(2025, 7, 7),  date(2025, 8, 3)),
    (date(2025, 9, 29), date(2025, 10, 26)),
    (date(2026, 1, 5),  date(2026, 2, 1)),
    (date(2026, 4, 6),  date(2026, 5, 3)),
]

FIXED_MD_COLORS = {"yellow", "purple", "blue", "bronze", "green", "orange"}

YPBB = {
    "1-1": {1: "Early",  2: "Middle", 3: "Late",   4: "Middle"},
    "1-2": {1: "Middle", 2: "Late",   3: "Middle", 4: "Early"},
    "2-1": {1: "Late",   2: "Middle", 3: "Early",  4: "Middle"},
    "2-2": {1: "Middle", 2: "Early",  3: "Middle", 4: "Late"},
    "3":   {1: "Middle", 2: "Early",  3: "Middle", 4: "Late"},
}

GREEN = {
    "1": {1: "Early",  2: "Middle", 3: "Late",   4: "Middle"},
    "2": {1: "Middle", 2: "Late",   3: "Middle", 4: "Early"},
    "3": {1: "Late",   2: "Middle", 3: "Early",  4: "Middle"},
}

MIST_SCU = {1: "Middle", 2: "Early", 3: "Middle", 4: "Late"}

# =====================================================
# UTILITIES
# =====================================================

def badge(text, color):
    st.markdown(
        f"""
        <span style="
            background-color:{color};
            padding:4px 10px;
            border-radius:12px;
            font-size:12px;
            font-weight:600;
            color:white;">
            {text}
        </span>
        """,
        unsafe_allow_html=True,
    )

def generate_token():
    return secrets.token_hex(6).upper()

def get_day_number(d):
    delta = (d - ANCHOR_DATE).days
    return (delta + ANCHOR_DAY_NUM - 1) % 4 + 1

def is_special_blue(d):
    return any(start <= d <= end for start, end in SPECIAL_BLUE_PERIODS)

def normalize_title(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[^A-Za-z0-9\s\-]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def shift_start_end(shift):
    if shift == "Early":
        return "06:45", "17:00"
    if shift == "Middle":
        return "08:00", "17:00"
    if shift == "Late":
        return "08:00", "18:45"
    if shift == "Fixed":
        return "08:00", "17:00"
    return None, None

# =====================================================
# SHIFT LOGIC
# =====================================================

def calc_shift(title, d, role):
    t = normalize_title(title).lower()
    if not t:
        return None

    if role == "MD":
        t = t.replace("gray 1 collaborator", "gray 1 md")
        t = t.replace("gray 2 collaborator", "gray 2 md")

    day = get_day_number(d)

    if role == "MD":
        for c in FIXED_MD_COLORS:
            if t.startswith(c):
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
        if n in (3, 5):
            return "Early" if day in (1, 3) else "Middle"
        if n in (2, 4):
            return "Middle" if day in (1, 3) else "Early"
        return None

    if t.startswith("silver"):
        m = re.search(r"(\d+)", t)
        n = int(m.group(1)) if m else 1
        return "Early" if n == 1 else "Middle"

    return None  # All others left untouched

# =====================================================
# S3 FUNCTIONS
# =====================================================

def get_existing_token(key):
    s3 = boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )
    try:
        response = s3.head_object(Bucket=st.secrets["S3_BUCKET"], Key=key)
        return response.get("Metadata", {}).get("owner-token")
    except ClientError:
        return None

def upload_feed(ics_text, key, token):
    s3 = boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )
    s3.put_object(
        Bucket=st.secrets["S3_BUCKET"],
        Key=key,
        Body=ics_text.encode("utf-8"),
        ContentType="text/calendar; charset=utf-8",
        CacheControl="no-cache",
        Metadata={"owner-token": token},
    )

# =====================================================
# STREAMLIT APP
# =====================================================

st.set_page_config(page_title="HMU Shift Processor")
st.title("HMU Shift Processor")

# Default role = MD
role = st.selectbox("Role", ["MD", "APP"], index=0)

st_autorefresh(interval=3600 * 1000, key="auto_refresh")

url = st.text_input("Paste ICS Calendar URL")

if st.button("Sync Now") and url:
    st.cache_data.clear()

# Default date window: today → June 30 next calendar year
today = date.today()
default_end = date(today.year + 1, 6, 30)

start_date = st.date_input("Start Date", today)
end_date = st.date_input("End Date", default_end)

events = []

if url:
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        cal = Calendar.from_ical(response.text)

        for comp in cal.walk():
            if comp.name == "VEVENT":
                title = str(comp.get("SUMMARY", "")).strip()
                dtstart = comp.get("DTSTART").dt
                dtend = comp.get("DTEND").dt if comp.get("DTEND") else None

                if isinstance(dtstart, datetime):
                    d = dtstart.date()
                    start = dtstart.strftime("%H:%M")
                else:
                    d = dtstart
                    start = "00:00"

                if isinstance(dtend, datetime):
                    end = dtend.strftime("%H:%M")
                else:
                    end = "23:59"

                if start_date <= d <= end_date:
                    events.append((title, d, start, end))
    except Exception as e:
        st.error(f"Failed to fetch calendar: {e}")

processed = []
untouched = []
rejected = []

for title, d, original_start, original_end in events:
    if not isinstance(d, date):
        rejected.append((title, "Invalid date"))
        continue

    shift = calc_shift(title, d, role)

    if shift:
        start, end = shift_start_end(shift)
        processed.append((title, d, start, end))
    else:
        untouched.append((title, d, original_start, original_end))

total_count = len(processed) + len(untouched) + len(rejected)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Events", total_count)
col2.metric("Processed", len(processed))
col3.metric("Left Untouched", len(untouched))
col4.metric("Rejected", len(rejected))

filter_option = st.selectbox(
    "Filter View",
    ["Show All", "Processed Only", "Left Untouched Only", "Errors Only"]
)

if filter_option in ["Show All", "Processed Only"]:
    badge("Processed", "#2E8B57")
    st.write(processed)

if filter_option in ["Show All", "Left Untouched Only"]:
    badge("Left Untouched", "#F4A300")
    st.write(untouched)

if filter_option in ["Show All", "Errors Only"]:
    badge("Data Errors", "#B22222")
    st.write(rejected)

st.subheader("Publish Subscription Feed")

feed_id = st.text_input("Enter personal Feed ID (3–40 letters/numbers/_/-)")

if feed_id and re.match(r"^[a-zA-Z0-9_-]{3,40}$", feed_id):
    key = f"feeds/{feed_id}.ics"
    existing_token = get_existing_token(key)

    if existing_token:
        st.warning("Feed ID already exists.")
        entered_token = st.text_input("Enter ownership token to update:", type="password")

        if entered_token == existing_token:
            upload_feed("ICS_PLACEHOLDER", key, existing_token)
            st.success("Feed updated.")
        elif entered_token:
            st.error("Incorrect token.")
    else:
        token = generate_token()
        upload_feed("ICS_PLACEHOLDER", key, token)

        st.success("Feed created.")
        st.write("Save this ownership token:")
        st.code(token)

        st.write("Subscription URL:")
        st.code(f"https://{st.secrets['S3_BUCKET']}.s3.{st.secrets['AWS_REGION']}.amazonaws.com/{key}")
