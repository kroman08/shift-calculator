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
        summary = ""
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
    marker.add("summary", f"HMU Shifts - Last Updated: {now.strftime('%Y-%m-%d %H:%M')}")
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

if "source_url_input" not in st.session_state:
    st.session_state["source_url_input"] = ""
if "input_mode" not in st.session_state:
    st.session_state["input_mode"] = "Subscription URL"
if "lookup_checked" not in st.session_state:
    st.session_state["lookup_checked"] = False
if "calendar_exists" not in st.session_state:
    st.session_state["calendar_exists"] = False
if "stored_metadata" not in st.session_state:
    st.session_state["stored_metadata"] = {}
if "restore_validated" not in st.session_state:
    st.session_state["restore_validated"] = False
if "validated_token" not in st.session_state:
    st.session_state["validated_token"] = ""
if "checked_calendar_name" not in st.session_state:
    st.session_state["checked_calendar_name"] = ""


def process_events(events, role):
    processed = []
    untouched = []

    for e in events:
        shift = calc_shift_type(e["title"], e["start_dt"].date(), role)
        if shift:
            st_t, en_t = shift_times(shift)
            new_start = datetime.combine(e["start_dt"].date(), st_t).replace(tzinfo=APP_TZ)
            new_end = datetime.combine(e["start_dt"].date(), en_t).replace(tzinfo=APP_TZ)
            processed.append({**e, "start_dt": new_start, "end_dt": new_end, "shift_type": shift})
        else:
            untouched.append(e)

    return processed, untouched


def show_event_preview(processed, untouched, errors):
    st.markdown("### Event Preview")
    st.write(f"Processed events: **{len(processed)}**")
    st.write(f"Untouched events: **{len(untouched)}**")
    st.write(f"Parse errors: **{len(errors)}**")

    if processed:
        st.dataframe(pd.DataFrame([
            {
                "title": e["title"],
                "date": e["start_dt"].date().isoformat(),
                "start": e["start_dt"].strftime("%H:%M"),
                "end": e["end_dt"].strftime("%H:%M"),
                "shift_type": e.get("shift_type", "")
            }
            for e in processed
        ]), use_container_width=True)

    if untouched:
        with st.expander("Untouched events", expanded=False):
            st.dataframe(pd.DataFrame([
                {
                    "title": e["title"],
                    "date": e["start_dt"].date().isoformat(),
                    "start": e["start_dt"].strftime("%H:%M"),
                    "end": e["end_dt"].strftime("%H:%M")
                }
                for e in untouched
            ]), use_container_width=True)

    if errors:
        with st.expander("Parse errors", expanded=False):
            st.dataframe(pd.DataFrame({
                "summary": [item[0] for item in errors],
                "error": [item[1] for item in errors],
            }), use_container_width=True)


today = date.today()
default_end = date(today.year + 1, 6, 30)

st.info("Returning user? If you already have a Calendar Name, you can proceed directly to the Your Calendar Name lookup below to update your feed.")
st.caption("*Forgot Calendar Name? In your calendar app, open subscribed calendar info. URL shows .../feeds/yourCalendarName.ics*")

lookup_calendar_name = st.text_input("Your Calendar Name", key="lookup_calendar_name")
st.caption("*(Use only letters, numbers, and hyphens. No spaces.)*")

bucket = st.secrets["S3_BUCKET"]
region = st.secrets["AWS_REGION"]

if st.button("Check Calendar Name"):
    st.session_state["lookup_checked"] = True
    st.session_state["restore_validated"] = False
    st.session_state["validated_token"] = ""
    st.session_state["checked_calendar_name"] = lookup_calendar_name.strip()

    if not lookup_calendar_name.strip():
        st.session_state["calendar_exists"] = False
        st.session_state["stored_metadata"] = {}
        st.error("Enter Your Calendar Name to look it up.")
    else:
        key = f"feeds/{lookup_calendar_name.strip()}.ics"
        existing = head_feed(bucket, key)
        if not existing:
            st.session_state["calendar_exists"] = False
            st.session_state["stored_metadata"] = {}
            st.error("No calendar was found matching the Calendar Name provided.")
        else:
            st.session_state["calendar_exists"] = True
            st.session_state["stored_metadata"] = existing.get("Metadata", {})
            st.success("Calendar found. Confirm ownership token to restore source URL and update this feed.")

if st.session_state["lookup_checked"] and st.session_state["calendar_exists"]:
    restore_token = st.text_input("Ownership Token", type="password", key="restore_token")
    st.caption("*(Format: first initial + middle initial + last initial + birth month (MM). Example: John M Smith born in February = JMS02. Case-insensitive.)*")

    if st.button("Validate Ownership Token"):
        stored_token = st.session_state["stored_metadata"].get("owner-token", "")
        if not restore_token:
            st.error("Ownership token is required.")
        elif restore_token.strip().upper() != stored_token.upper():
            st.session_state["restore_validated"] = False
            st.error("Incorrect ownership token.")
        else:
            restored_url = st.session_state["stored_metadata"].get("source-url", "")
            restored_role = st.session_state["stored_metadata"].get("role", "MD")
            restored_end = st.session_state["stored_metadata"].get("window-end", str(default_end))

            if not restored_url:
                st.session_state["restore_validated"] = False
                st.error("No source URL stored for this calendar.")
            else:
                st.session_state["restore_validated"] = True
                st.session_state["validated_token"] = restore_token.strip().upper()
                st.session_state["source_url_input"] = restored_url
                st.session_state["input_mode"] = "Subscription URL"
                st.session_state["restored_role"] = restored_role
                st.session_state["restored_end"] = restored_end
                st.success("Ownership confirmed. Source URL restored. Review parsed events, then click Update Existing Feed.")

role_default = 0
if st.session_state.get("restore_validated"):
    role_default = 0 if st.session_state.get("restored_role", "MD") == "MD" else 1

role = st.selectbox("Role", ["MD", "APP"], index=role_default)
window_start = st.date_input("Start Date", value=today)
window_end_value = default_end
if st.session_state.get("restore_validated"):
    try:
        window_end_value = date.fromisoformat(st.session_state.get("restored_end", str(default_end)))
    except ValueError:
        window_end_value = default_end
window_end = st.date_input("End Date", value=window_end_value)

input_mode = st.radio("Input Method", ["Subscription URL", "Upload ICS"], key="input_mode")

cal_text = None
url = None

if input_mode == "Subscription URL":
    url = st.text_input("ICS Subscription URL", key="source_url_input")
    if url:
        try:
            cal_text = fetch_ics(url)
        except Exception as e:
            st.error(f"Unable to fetch calendar: {e}")
else:
    upload = st.file_uploader("Upload ICS file", type=["ics"])
    if upload:
        cal_text = upload.read().decode("utf-8")

parse_errors = []
processed = []
untouched = []

if cal_text:
    parsed_events, parse_errors = expand_events(cal_text, window_start, window_end)
    processed, untouched = process_events(parsed_events, role)
    show_event_preview(processed, untouched, parse_errors)

# ============================================================
# FINAL ACTION
# ============================================================

if st.session_state.get("restore_validated"):
    if st.button("Update Existing Feed"):
        if not cal_text:
            st.error("Provide a source calendar first.")
        else:
            calendar_name = st.session_state.get("checked_calendar_name", "").strip()
            token = st.session_state.get("validated_token", "").strip().upper()
            key = f"feeds/{calendar_name}.ics"
            final_ics = build_output_ics(processed, untouched, calendar_name)

            put_feed(bucket, key, final_ics, {
                "owner-token": token,
                "source-url": url or "",
                "role": role,
                "window-end": str(window_end)
            })

            sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
            st.success("Feed updated successfully. You can now subscribe to this calendar in your native calendar app.")
            st.code(sub_url)
            st.warning(
                "Important: The subscription URL does NOT automatically update in the background. "
                "If your source calendar changes, you must reopen this app and re-enter your Calendar Name, ownership token, and republish/update the feed.\n"
                "Otherwise, the subscription calendar will remain unchanged.\n\n"
                "Note: you can confirm the date of your last-update by searching your calendar for 'HMU Shifts - Last Updated'."
            )
else:
    if st.session_state.get("lookup_checked") and st.session_state.get("calendar_exists"):
        st.info("This Calendar Name already exists. Validate ownership token above to update it.")
    else:
        st.subheader("Publish Feed")
        publish_calendar_name = st.text_input("Your Calendar Name", value=lookup_calendar_name, key="publish_calendar_name")
        st.caption("*(Use only letters, numbers, and hyphens. No spaces.)*")
        st.caption(
            "This ownership token will be required if you later republish/update the subscription URL associated with this calendar. "
            "This is intended to prevent multiple people from using the same calendar name and inadvertently overwriting each other's calendars."
        )
        st.caption("*(Format: first initial + middle initial + last initial + birth month (MM). Example: John M Smith born in February = JMS02. Case-insensitive.)*")
        publish_token = st.text_input("Ownership Token", type="password", key="publish_token")

        if st.button("Publish Feed"):
            if not cal_text:
                st.error("Provide a source calendar first.")
            elif not publish_calendar_name.strip() or not publish_token.strip():
                st.error("Your Calendar Name and Ownership Token are required for publishing.")
            else:
                key = f"feeds/{publish_calendar_name.strip()}.ics"
                final_ics = build_output_ics(processed, untouched, publish_calendar_name.strip())

                put_feed(bucket, key, final_ics, {
                    "owner-token": publish_token.strip().upper(),
                    "source-url": url or "",
                    "role": role,
                    "window-end": str(window_end)
                })

                sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
                st.success("Feed published successfully. You can now subscribe to this calendar in your native calendar app.")
                st.code(sub_url)
                st.warning(
                    "Important: The subscription URL does NOT automatically update in the background. "
                    "If your source calendar changes, you must reopen this app and re-enter your Calendar Name, ownership token, and republish/update the feed.\n"
                    "Otherwise, the subscription calendar will remain unchanged.\n\n"
                    "Note: you can confirm the date of your last-update by searching your calendar for 'HMU Shifts - Last Updated'."
                )
