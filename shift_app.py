import re
import secrets
import hashlib
import json
from datetime import datetime, date, time, timedelta
from typing import Optional, List, Dict, Any, Tuple

import pandas as pd
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError

from icalendar import Calendar, Event
from dateutil.rrule import rrulestr
from streamlit_autorefresh import st_autorefresh
from zoneinfo import ZoneInfo

# =====================================================
# TIMEZONE (DST-safe Eastern)
# =====================================================
APP_TZ = ZoneInfo("America/New_York")

# =====================================================
# SHIFT RULE CONFIG
# =====================================================
ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2  # 7/1/2025 is Day 2

SPECIAL_BLUE_PERIODS = [
    (date(2025, 7, 7),  date(2025, 8, 3)),
    (date(2025, 9, 29), date(2025, 10, 26)),
    (date(2026, 1, 5),  date(2026, 2, 1)),
    (date(2026, 4, 6),  date(2026, 5, 3)),
]

# If MD is selected, these colors are always "Fixed"
FIXED_MD_COLORS = {"yellow", "purple", "blue", "bronze", "green", "orange"}

# Yellow, Purple, Blue (standard), Bronze, Orange share this suffix mapping
YPBB_SUFFIX_ROTATION = {
    "1-1": {1: "Early",  2: "Middle", 3: "Late",   4: "Middle"},
    "1-2": {1: "Middle", 2: "Late",   3: "Middle", 4: "Early"},
    "2-1": {1: "Late",   2: "Middle", 3: "Early",  4: "Middle"},
    "2-2": {1: "Middle", 2: "Early",  3: "Middle", 4: "Late"},
    "3":   {1: "Middle", 2: "Early",  3: "Middle", 4: "Late"},
}

GREEN_SUFFIX_ROTATION = {
    "1": {1: "Early",  2: "Middle", 3: "Late",   4: "Middle"},
    "2": {1: "Middle", 2: "Late",   3: "Middle", 4: "Early"},
    "3": {1: "Late",   2: "Middle", 3: "Early",  4: "Middle"},
}

MIST_SCU_ROTATION = {1: "Middle", 2: "Early", 3: "Middle", 4: "Late"}

GRAY_MIST_EARLY_ON_1_3 = {"gray 1 md", "mist transplant"}
GRAY_MIST_MIDDLE_ON_1_3 = {"gray 2 md", "gray 3 app"}
ALL_GRAY_MIST_ROLES = GRAY_MIST_EARLY_ON_1_3.union(GRAY_MIST_MIDDLE_ON_1_3)

# =====================================================
# UI HELPERS
# =====================================================
def badge(text: str, color: str) -> None:
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

# =====================================================
# NORMALIZATION & DAY NUMBER
# =====================================================
def normalize_title(s: Any) -> str:
    """Remove trailing ($) etc. Keep only letters/numbers/spaces/hyphens; collapse spaces."""
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[^A-Za-z0-9\s\-]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def clean_input(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    text = text.strip().lower().replace(",", "")
    text = re.sub(r"\s+", " ", text)
    return text

def get_day_number(target_dt: date) -> int:
    delta_days = (target_dt - ANCHOR_DATE).days
    return (delta_days + ANCHOR_DAY_NUM - 1) % 4 + 1

def is_in_special_blue_period(target_dt: date) -> bool:
    for start_date, end_date in SPECIAL_BLUE_PERIODS:
        if start_date <= target_dt <= end_date:
            return True
    return False

# =====================================================
# SHIFT CALCULATION
# =====================================================
def calc_shift_type(title: str, target_date: date, role: str) -> Optional[str]:
    """
    Returns: Early / Middle / Late / Fixed, or None (meaning leave untouched)
    """
    clean_shift = clean_input(normalize_title(title))
    if not clean_shift:
        return None

    # Gray collaborator correction when MD selected
    if role == "MD":
        clean_shift = clean_shift.replace("gray 1 collaborator", "gray 1 md")
        clean_shift = clean_shift.replace("gray 2 collaborator", "gray 2 md")

    day_num = get_day_number(target_date)

    # MD fixed override for certain colors
    if role == "MD":
        first = clean_shift.split(" ")[0] if clean_shift.split(" ") else ""
        if first in FIXED_MD_COLORS:
            return "Fixed"

    # Gray/MIST transplant special roles
    if clean_shift in ALL_GRAY_MIST_ROLES:
        if clean_shift in GRAY_MIST_EARLY_ON_1_3:
            return "Early" if day_num in (1, 3) else "Middle"
        if clean_shift in GRAY_MIST_MIDDLE_ON_1_3:
            return "Middle" if day_num in (1, 3) else "Early"
        return None

    # MIST SCU
    if clean_shift.startswith("mist scu"):
        return MIST_SCU_ROTATION.get(day_num)

    parts = clean_shift.split()
    shift_name = parts[0] if parts else ""
    suffix = parts[1] if len(parts) > 1 else ""

    # GOLD
    if shift_name == "gold":
        m = re.search(r"(\d+)", clean_shift)
        if not m:
            return None
        num = int(m.group(1))
        if num == 1:
            return "Early"
        if num >= 6:
            return "Middle"
        if num in (3, 5):
            return "Early" if day_num in (1, 3) else "Middle"
        if num in (2, 4):
            return "Middle" if day_num in (1, 3) else "Early"
        return None

    # SILVER (assume 1 if missing)
    if shift_name == "silver":
        m = re.search(r"(\d+)", clean_shift)
        num = int(m.group(1)) if m else 1
        return "Early" if num == 1 else "Middle"

    # BLUE (special periods vs standard)
    if shift_name == "blue":
        if not suffix:
            return None
        if is_in_special_blue_period(target_date):
            if suffix == "1":
                return "Early" if day_num in (1, 3) else "Middle"
            if suffix in ("3-1", "3-2"):
                return "Middle" if day_num in (1, 3) else "Early"
            return None
        else:
            if suffix in YPBB_SUFFIX_ROTATION:
                return YPBB_SUFFIX_ROTATION[suffix].get(day_num)
            return None

    # Yellow/Purple/Bronze/Orange
    if shift_name in ("yellow", "purple", "bronze", "orange"):
        if not suffix:
            return None
        if suffix in YPBB_SUFFIX_ROTATION:
            return YPBB_SUFFIX_ROTATION[suffix].get(day_num)
        return None

    # Green
    if shift_name == "green":
        if not suffix:
            return None
        if suffix in GREEN_SUFFIX_ROTATION:
            return GREEN_SUFFIX_ROTATION[suffix].get(day_num)
        return None

    # Unknown -> leave untouched
    return None

def shift_times(shift_type: str) -> Tuple[time, time]:
    """
    Early = 06:45–17:00
    Middle = 08:00–17:00
    Late = 08:00–18:45
    Fixed = 08:00–17:00
    """
    if shift_type == "Early":
        return time(6, 45), time(17, 0)
    if shift_type == "Middle":
        return time(8, 0), time(17, 0)
    if shift_type == "Late":
        return time(8, 0), time(18, 45)
    if shift_type == "Fixed":
        return time(8, 0), time(17, 0)
    return time(8, 0), time(17, 0)

# =====================================================
# ICS INPUT: FETCH / READ
# =====================================================
@st.cache_data(ttl=3600)
def fetch_ics_text(url: str) -> Tuple[str, str]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _is_all_day(dt_val: Any) -> bool:
    return isinstance(dt_val, date) and not isinstance(dt_val, datetime)

def _as_eastern(dt: datetime) -> datetime:
    """Convert aware datetime to Eastern; for naive, assume Eastern."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=APP_TZ)
    return dt.astimezone(APP_TZ)

def _clamp_same_day(start_dt: datetime, end_dt: datetime) -> datetime:
    """
    Ignore multi-day spans beyond start:
    if end date differs, clamp to same start date using end's time-of-day.
    If that becomes <= start, fallback to +1 hour.
    """
    if end_dt.date() != start_dt.date():
        end_dt = datetime.combine(start_dt.date(), end_dt.timetz())
        if start_dt.tzinfo is not None:
            end_dt = end_dt.replace(tzinfo=start_dt.tzinfo)
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=1)
    return end_dt

def _collect_exdates(comp) -> set:
    ex = set()
    exprop = comp.get("EXDATE")
    if not exprop:
        return ex
    if not isinstance(exprop, list):
        exprop = [exprop]
    for item in exprop:
        for dtv in getattr(item, "dts", []):
            dt = getattr(dtv, "dt", None)
            if isinstance(dt, datetime):
                ex.add(_as_eastern(dt).date())
            elif isinstance(dt, date):
                ex.add(dt)
    return ex

def _collect_rdates(comp) -> set:
    rd = set()
    rdprop = comp.get("RDATE")
    if not rdprop:
        return rd
    if not isinstance(rdprop, list):
        rdprop = [rdprop]
    for item in rdprop:
        for dtv in getattr(item, "dts", []):
            dt = getattr(dtv, "dt", None)
            if isinstance(dt, datetime):
                rd.add(_as_eastern(dt).date())
            elif isinstance(dt, date):
                rd.add(dt)
    return rd

def expand_events(cal_text: str, window_start: date, window_end: date) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
    """
    Expand VEVENTs (including RRULE) into individual occurrences.
    - Ignore multi-day spans beyond start (clamp end to same start day).
    - Process recurring occurrences independently.
    Returns (events, errors).
    """
    errors: List[Tuple[str, str]] = []
    events: List[Dict[str, Any]] = []

    cal = Calendar.from_ical(cal_text)

    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue

        title = str(comp.get("SUMMARY", "")).strip() or "(no title)"
        uid = str(comp.get("UID", "")).strip() or None

        dtstart_prop = comp.get("DTSTART")
        if not dtstart_prop:
            errors.append((title, "Missing DTSTART"))
            continue

        dtstart_val = dtstart_prop.dt
        dtend_val = comp.get("DTEND").dt if comp.get("DTEND") else None

        # All-day
        if _is_all_day(dtstart_val):
            d = dtstart_val
            if not (window_start <= d <= window_end):
                continue
            events.append({
                "title": title,
                "start_dt": d,     # date
                "end_dt": None,
                "all_day": True,
                "uid": uid,
            })
            continue

        # Timed
        if not isinstance(dtstart_val, datetime):
            errors.append((title, "Invalid DTSTART"))
            continue

        dtstart = _as_eastern(dtstart_val)

        if isinstance(dtend_val, datetime):
            dtend = _as_eastern(dtend_val)
        else:
            dtend = dtstart + timedelta(hours=1)

        dtend = _clamp_same_day(dtstart, dtend)

        exdates = _collect_exdates(comp)
        rdates = _collect_rdates(comp)

        rrule_prop = comp.get("RRULE")

        # Recurring
        if rrule_prop:
            try:
                rrule_str = rrule_prop.to_ical().decode("utf-8") if isinstance(rrule_prop.to_ical(), (bytes, bytearray)) else str(rrule_prop.to_ical())
                rule = rrulestr(rrule_str, dtstart=dtstart)
            except Exception:
                errors.append((title, "Invalid RRULE"))
                continue

            duration = dtend - dtstart

            ws = datetime.combine(window_start, time.min).replace(tzinfo=APP_TZ)
            we = datetime.combine(window_end, time.max).replace(tzinfo=APP_TZ)

            for occ in rule.between(ws, we, inc=True):
                occ = _as_eastern(occ)
                if occ.date() in exdates:
                    continue
                if not (window_start <= occ.date() <= window_end):
                    continue

                occ_end = occ + duration
                occ_end = _clamp_same_day(occ, occ_end)

                events.append({
                    "title": title,
                    "start_dt": occ,
                    "end_dt": occ_end,
                    "all_day": False,
                    "uid": uid,
                })

            # Explicit RDATEs
            for rd in rdates:
                if not (window_start <= rd <= window_end):
                    continue
                if rd in exdates:
                    continue
                occ = datetime.combine(rd, dtstart.timetz()).replace(tzinfo=APP_TZ)
                occ_end = occ + duration
                occ_end = _clamp_same_day(occ, occ_end)
                events.append({
                    "title": title,
                    "start_dt": occ,
                    "end_dt": occ_end,
                    "all_day": False,
                    "uid": uid,
                })

        else:
            # Non-recurring
            d = dtstart.date()
            if not (window_start <= d <= window_end):
                continue
            events.append({
                "title": title,
                "start_dt": dtstart,
                "end_dt": dtend,
                "all_day": False,
                "uid": uid,
            })

    # De-dupe exact start/end/title
    seen = set()
    uniq = []
    for e in events:
        key = (e["title"], str(e["start_dt"]), str(e["end_dt"]), e["all_day"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)

    return uniq, errors

# =====================================================
# OUTPUT ICS BUILD (Eastern)
# =====================================================
def stable_uid(feed_id: str, title: str, start_dt: Any) -> str:
    raw = f"{feed_id}|{title}|{start_dt}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:20] + "@hmu-shifts"

def build_output_ics(processed_rows, untouched_rows, feed_id_for_uid):
    cal = Calendar()
    cal.add("prodid", "-//HMU Shift Processor//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("X-WR-TIMEZONE", "America/New_York")

# Add subtle "Last Updated" marker event
updated_now = datetime.now(APP_TZ)

meta_event = Event()
meta_event.add(
    "summary",
    f"HMU Shifts — Last Updated: {updated_now.strftime('%Y-%m-%d %H:%M')}"
)
meta_event.add("uid", f"hmushifts-last-updated@{feed_id_for_uid}")
meta_event.add("dtstamp", updated_now)

# 1-minute event at 12:01 AM on publish day
start_marker = datetime.combine(
    updated_now.date(),
    time(0, 1)
).replace(tzinfo=APP_TZ)

end_marker = start_marker + timedelta(minutes=1)

meta_event.add("dtstart", start_marker)
meta_event.add("dtend", end_marker)

# Transparent so it does not block the calendar visually
meta_event.add("transp", "TRANSPARENT")
meta_event.add("description", "System-generated update marker. Safe to ignore.")

cal.add_component(meta_event)
    
def add_row(row):
    title = row["title"]
    uid = row.get("uid") or stable_uid(feed_id_for_uid, title, row["start_dt"])

    ev = Event()
    ev.add("summary", title)
    ev.add("uid", uid)
    ev.add("dtstamp", datetime.now(APP_TZ))

    if row["all_day"]:
        ev.add("dtstart", row["start_dt"])
    else:
        sd = row["start_dt"]
        ed = row["end_dt"]

        if isinstance(sd, datetime):
            sd = sd.astimezone(APP_TZ)
        if isinstance(ed, datetime):
            ed = ed.astimezone(APP_TZ)

        ev.add("dtstart", sd)
        ev.add("dtend", ed)

    cal.add_component(ev)

    for r in processed_rows:
        add_row(r)

    for r in untouched_rows:
        add_row(r)

    return cal.to_ical()
# =====================================================
# S3 PUBLISH (token ownership)
# =====================================================
def s3_client():
    return boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )

def get_existing_token(bucket: str, key: str) -> Optional[str]:
    try:
        resp = s3_client().head_object(Bucket=bucket, Key=key)
        return resp.get("Metadata", {}).get("owner-token")
    except ClientError:
        return None

def upload_feed(bucket: str, key: str, ics_bytes: bytes, token: str) -> None:
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=ics_bytes,
        ContentType="text/calendar; charset=utf-8",
        CacheControl="no-cache, max-age=0",
        Metadata={"owner-token": token},
    )

def generate_token() -> str:
    return secrets.token_hex(6).upper()

# =====================================================
# STREAMLIT APP UI
# =====================================================
st.set_page_config(page_title="HMU Shift Processor", layout="centered")
st.title("HMU Shift Processor")

# Default role = MD
role = st.selectbox("Role", ["MD", "APP"], index=0)

# Auto-refresh hourly (only while app is open)
st_autorefresh(interval=3600 * 1000, key="auto_refresh_hourly")

input_mode = st.radio("Input method", ["Subscription URL", "Upload ICS file"], horizontal=True)

# Default window: today -> June 30 next calendar year
today = date.today()
default_end = date(today.year + 1, 6, 30)

st.subheader("Date window")
window_start = st.date_input("Start date", value=today)
window_end = st.date_input("End date", value=default_end)
if window_end < window_start:
    st.error("End date must be on/after start date.")
    st.stop()

cal_text: Optional[str] = None
last_sync: Optional[str] = None

url: Optional[str] = None

if input_mode == "Subscription URL":
    url = st.text_input("Paste ICS subscription URL (.ics)")
    colA, colB = st.columns([1, 1])
    with colA:
        sync_now = st.button("Sync Now", disabled=not bool(url))
    with colB:
        st.caption("Auto-refresh: every 1 hour (while open)")

    if url and sync_now:
        fetch_ics_text.clear()
        st.rerun()

    if url:
        try:
            cal_text, last_sync = fetch_ics_text(url)
            st.caption(f"Last sync: {last_sync}")
        except Exception as e:
            st.error(f"Failed to fetch calendar: {e}")
            st.stop()

else:
    up = st.file_uploader("Upload an .ics file", type=["ics"])
    if up is not None:
        try:
            cal_text = up.getvalue().decode("utf-8", errors="replace")
            st.caption("Loaded uploaded ICS file.")
        except Exception as e:
            st.error(f"Failed to read uploaded file: {e}")
            st.stop()

if not cal_text:
    st.info("Provide an ICS subscription URL or upload an ICS file to begin.")
    st.stop()

# Expand events
events, data_errors = expand_events(cal_text, window_start, window_end)

processed_rows: List[Dict[str, Any]] = []
untouched_rows: List[Dict[str, Any]] = []
rejected_rows: List[Tuple[str, str]] = list(data_errors)

for e in events:
    title = e["title"]
    if e["all_day"]:
        # Leave all-day untouched
        untouched_rows.append(e)
        continue

    sd: datetime = _as_eastern(e["start_dt"])
    ed: datetime = _as_eastern(e["end_dt"])
    ed = _clamp_same_day(sd, ed)

    shift_type = calc_shift_type(title, sd.date(), role)

    if shift_type:
        st_t, en_t = shift_times(shift_type)
        new_start = datetime.combine(sd.date(), st_t).replace(tzinfo=APP_TZ)
        new_end = datetime.combine(sd.date(), en_t).replace(tzinfo=APP_TZ)
        processed_rows.append({
            "title": title,
            "start_dt": new_start,
            "end_dt": new_end,
            "all_day": False,
            "uid": e.get("uid"),
        })
    else:
        untouched_rows.append({
            "title": title,
            "start_dt": sd,
            "end_dt": ed,
            "all_day": False,
            "uid": e.get("uid"),
        })

# ---------------------------
# Display (sorted, formatted MM-DD-YYYY)
# ---------------------------
def to_display_df_processed_or_untouched(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    disp = []
    for r in rows:
        if r.get("all_day"):
            d = r["start_dt"]
            disp.append({"Event": r["title"], "Date": d, "Start": "(all-day)", "End": ""})
        else:
            sd = r["start_dt"]
            ed = r["end_dt"]
            disp.append({
                "Event": r["title"],
                "Date": sd.date(),
                "Start": sd.strftime("%H:%M"),
                "End": ed.strftime("%H:%M"),
            })
    df = pd.DataFrame(disp, columns=["Event", "Date", "Start", "End"])
    if not df.empty:
        df = df.sort_values(["Date", "Start", "Event"], kind="mergesort")
        df["Date"] = df["Date"].apply(lambda x: x.strftime("%m-%d-%Y"))
    return df

processed_df = to_display_df_processed_or_untouched(processed_rows)
untouched_df = to_display_df_processed_or_untouched(untouched_rows)
rejected_df = pd.DataFrame(rejected_rows, columns=["Event", "Reason"]) if rejected_rows else pd.DataFrame(columns=["Event", "Reason"])

# Counts summary
total_count = len(processed_rows) + len(untouched_rows) + len(rejected_rows)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Events", total_count)
c2.metric("Processed", len(processed_rows))
c3.metric("Left Untouched", len(untouched_rows))
c4.metric("Rejected (Errors)", len(rejected_rows))

filter_option = st.selectbox("Filter View", ["Show All", "Processed Only", "Left Untouched Only", "Errors Only"])

if filter_option in ("Show All", "Processed Only"):
    badge("Processed", "#2E8B57")
    st.dataframe(processed_df, use_container_width=True)

if filter_option in ("Show All", "Left Untouched Only"):
    badge("Left Untouched", "#F4A300")
    st.dataframe(untouched_df, use_container_width=True)

if filter_option in ("Show All", "Errors Only"):
    badge("Data Errors", "#B22222")
    st.dataframe(rejected_df, use_container_width=True)

# ---------------------------
# Output ICS download
# ---------------------------
output_ics_bytes = build_output_ics(processed_rows, untouched_rows, feed_id_for_uid="download")

st.subheader("Download output")
st.download_button(
    "Download output as ICS",
    data=output_ics_bytes,
    file_name="hmu_output.ics",
    mime="text/calendar",
)

# ---------------------------
# Publish subscription feed (S3)
# ---------------------------
# ---------------------------
# Publish subscription feed (S3)
# ---------------------------
st.subheader("Publish Subscription Feed (S3)")

st.markdown(
    "You can publish a subscription URL (paste into Outlook once). "
    "A token is needed if you later republish/update the calendar subscription URL "
    "associated with this Feed ID. "
    "This is intended to prevent multiple people from using the same Feed ID "
    "and inadvertently overwriting the others' calendars."
)

# Show warning only after URL provided (and only URL mode)
if input_mode == "Subscription URL" and url:
    st.warning(
        "Important: The subscription URL does NOT automatically update in the background. "
        "If your source calendar changes, you must reopen this app and click Sync Now "
        "to re-parse and republish the updated feed. "
        "Otherwise, the subscription calendar will remain unchanged."
    )

feed_id = st.text_input("Feed ID (3–40 letters/numbers/_/-)")
valid_id = bool(re.match(r"^[A-Za-z0-9_-]{3,40}$", feed_id or ""))

if valid_id:
    bucket = st.secrets["S3_BUCKET"]
    region = st.secrets["AWS_REGION"]
    key = f"feeds/{feed_id}.ics"
    sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

    existing_token = get_existing_token(bucket, key)

    if existing_token:
        st.info("This Feed ID already exists.")
        entered_token = st.text_input("Enter ownership token to update:", type="password")
        if st.button("Update Feed"):
            if not entered_token:
                st.error("You must enter the ownership token.")
            elif entered_token != existing_token:
                st.error("Incorrect ownership token. Update failed.")
            else:
                try:
                    upload_feed(bucket, key, output_ics_bytes, existing_token)
                    st.success("Feed successfully updated.")
                    st.code(sub_url)
                except Exception as e:
                    st.error(f"Update failed: {e}")

    else:
        if st.button("Create Feed"):
            try:
                token = generate_token()
                upload_feed(bucket, key, output_ics_bytes, token)
                st.success("Feed successfully created.")
                st.write("Subscription URL:")
                st.code(sub_url)
                st.warning("Save this ownership token. You will need it to update this feed later.")
                st.code(token)
            except Exception as e:
                st.error(f"Creation failed: {e}")
