import re
import hashlib
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Set

import pandas as pd
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError

from icalendar import Calendar
from dateutil.rrule import rrulestr
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
# HELPERS
# =====================================================
def get_day_number(d: date) -> int:
    delta = (d - ANCHOR_DATE).days
    return (delta + ANCHOR_DAY_NUM - 1) % 4 + 1

def is_special_blue(d: date) -> bool:
    return any(start <= d <= end for start, end in SPECIAL_BLUE_PERIODS)

def normalize_title(s: object) -> str:
    """Remove symbols like ($). Keep only letters/numbers/spaces/hyphens."""
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[^A-Za-z0-9\s\-]", "", s)
    return re.sub(r"\s+", " ", s).strip()

def parse_date_any(x: object) -> Optional[date]:
    dt = pd.to_datetime(str(x).strip(), errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()

def shift_start_end(shift: str) -> Tuple[Optional[str], Optional[str]]:
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
# SHIFT LOGIC (ALL COLORS)
# =====================================================
def calc_shift(title: str, d: date, role: str) -> Optional[str]:
    t = normalize_title(title).lower()
    if not t:
        return None

    # Gray collaborator correction (MD only)
    if role == "MD":
        t = t.replace("gray 1 collaborator", "gray 1 md")
        t = t.replace("gray 2 collaborator", "gray 2 md")

    day = get_day_number(d)

    # MD fixed override for selected colors
    if role == "MD":
        for c in FIXED_MD_COLORS:
            if t.startswith(c):
                return "Fixed"

    # GOLD
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

    # SILVER (default to 1 if missing number)
    if t.startswith("silver"):
        m = re.search(r"(\d+)", t)
        n = int(m.group(1)) if m else 1
        return "Early" if n == 1 else "Middle"

    # BLUE (special periods vs standard)
    if t.startswith("blue"):
        parts = t.split()
        if len(parts) < 2:
            return None
        suffix = parts[1]
        if is_special_blue(d):
            if suffix == "1":
                return "Early" if day in (1, 3) else "Middle"
            if suffix in ("3-1", "3-2"):
                return "Middle" if day in (1, 3) else "Early"
            return None
        else:
            if suffix in YPBB:
                return YPBB[suffix][day]
            return None

    # YELLOW / PURPLE / BRONZE / ORANGE (standard YPBB suffix rotation)
    for c in ("yellow", "purple", "bronze", "orange"):
        if t.startswith(c):
            parts = t.split()
            if len(parts) < 2:
                return None
            suffix = parts[1]
            if suffix in YPBB:
                return YPBB[suffix][day]
            return None

    # GREEN
    if t.startswith("green"):
        parts = t.split()
        if len(parts) < 2:
            return None
        suffix = parts[1]
        if suffix in GREEN:
            return GREEN[suffix][day]
        return None

    # GRAY + MIST Transplant
    if "mist transplant" in t or "gray 1 md" in t:
        return "Early" if day in (1, 3) else "Middle"
    if "gray 2 md" in t or "gray 3 app" in t:
        return "Middle" if day in (1, 3) else "Early"

    # MIST SCU
    if t.startswith("mist scu"):
        return MIST_SCU[day]

    return None


# =====================================================
# ICS (INPUT) FETCH + PARSE + RECURRENCE EXPANSION
# =====================================================
@st.cache_data(ttl=3600)  # 1 hour
def fetch_ics_text(url: str) -> Tuple[str, str]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _to_date(dtlike) -> Optional[date]:
    if dtlike is None:
        return None
    # icalendar may give date or datetime
    if isinstance(dtlike, date) and not isinstance(dtlike, datetime):
        return dtlike
    if isinstance(dtlike, datetime):
        return dtlike.date()
    # fallback
    try:
        return pd.to_datetime(dtlike, errors="coerce").date()
    except Exception:
        return None

def _collect_exdates(component) -> Set[date]:
    exdates: Set[date] = set()
    ex = component.get("EXDATE")
    if not ex:
        return exdates
    if not isinstance(ex, list):
        ex = [ex]
    for exprop in ex:
        # exprop.dts can hold multiple
        for dt in getattr(exprop, "dts", []):
            d = _to_date(getattr(dt, "dt", None))
            if d:
                exdates.add(d)
    return exdates

def _collect_rdates(component) -> Set[date]:
    rdates: Set[date] = set()
    rd = component.get("RDATE")
    if not rd:
        return rdates
    if not isinstance(rd, list):
        rd = [rd]
    for rdprop in rd:
        for dt in getattr(rdprop, "dts", []):
            d = _to_date(getattr(dt, "dt", None))
            if d:
                rdates.add(d)
    return rdates

def parse_ics_events_expand(text: str, window_start: date, window_end: date) -> List[Tuple[str, date]]:
    """
    Returns list of (title, occurrence_start_date) within [window_start, window_end].
    - Multi-day spans ignored beyond start (we only use DTSTART date)
    - Recurrences expanded and treated independently
    """
    cal = Calendar.from_ical(text)
    out: List[Tuple[str, date]] = []

    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue

        title = str(comp.get("SUMMARY", "")).strip()
        if not title:
            continue

        dtstart = _to_date(getattr(comp.get("DTSTART"), "dt", None))
        if not dtstart:
            continue

        exdates = _collect_exdates(comp)
        rdates = _collect_rdates(comp)

        # Recurrence rule expansion
        rrule_prop = comp.get("RRULE")
        if rrule_prop:
            # Build an RRULE string dateutil can parse.
            # icalendar gives dict-ish values; safest: reconstruct from to_ical().
            # Example: b'FREQ=WEEKLY;BYDAY=MO'
            rrule_bytes = rrule_prop.to_ical()
            rrule_str = rrule_bytes.decode("utf-8") if isinstance(rrule_bytes, (bytes, bytearray)) else str(rrule_bytes)

            # dateutil wants a DTSTART; we'll provide dtstart as midnight datetime
            dtstart_dt = datetime.combine(dtstart, datetime.min.time())
            rule = rrulestr(rrule_str, dtstart=dtstart_dt)

            # Expand occurrences within window
            ws = datetime.combine(window_start, datetime.min.time())
            we = datetime.combine(window_end + timedelta(days=1), datetime.min.time())  # inclusive end
            for occ in rule.between(ws, we, inc=True):
                od = occ.date()
                if window_start <= od <= window_end and od not in exdates:
                    out.append((title, od))

            # Include explicit RDATEs too
            for rd in rdates:
                if window_start <= rd <= window_end and rd not in exdates:
                    out.append((title, rd))
        else:
            # Non-recurring (still include if in window)
            if window_start <= dtstart <= window_end:
                out.append((title, dtstart))
            # Include any RDATEs even without RRULE
            for rd in rdates:
                if window_start <= rd <= window_end and rd not in exdates:
                    out.append((title, rd))

    # Deduplicate exact (title, date)
    out = list(dict.fromkeys(out))
    return out


# =====================================================
# OUTPUT ICS GENERATION
# =====================================================
def build_output_ics(rows: pd.DataFrame) -> str:
    ics = "BEGIN:VCALENDAR\nVERSION:2.0\nCALSCALE:GREGORIAN\nPRODID:-//HMU Shift Processor//EN\n"
    for _, r in rows.iterrows():
        if pd.isna(r["Date"]) or not r["Start"] or not r["End"]:
            continue
        start_dt = datetime.combine(r["Date"], datetime.strptime(r["Start"], "%H:%M").time())
        end_dt = datetime.combine(r["Date"], datetime.strptime(r["End"], "%H:%M").time())

        summary = f"{r['Event']} ({r['Shift']})"
        summary = str(summary).replace("\\", "\\\\").replace("\n", "\\n").replace(",", "\\,").replace(";", "\\;")

        ics += (
            "BEGIN:VEVENT\n"
            f"SUMMARY:{summary}\n"
            f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%S')}\n"
            f"DTEND:{end_dt.strftime('%Y%m%dT%H%M%S')}\n"
            "END:VEVENT\n"
        )
    ics += "END:VCALENDAR\n"
    return ics


# =====================================================
# S3 PUBLISH (STABLE SUBSCRIPTION URL)
# =====================================================
def upload_ics_to_s3(ics_text: str) -> str:
    s3 = boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )
    bucket = st.secrets["S3_BUCKET"]
    key = st.secrets.get("S3_KEY", "hmu-shifts.ics")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=ics_text.encode("utf-8"),
        ContentType="text/calendar; charset=utf-8",
        CacheControl="no-cache, max-age=0",
    )
    return f"https://{bucket}.s3.{st.secrets['AWS_REGION']}.amazonaws.com/{key}"


# =====================================================
# STREAMLIT UI
# =====================================================
st.set_page_config(page_title="HMU Shift Processor", layout="centered")
st.title("HMU Shift Processor")

role = st.selectbox("Role", ["APP", "MD"])

mode = st.radio("Input source", ["Upload CSV", "Calendar URL"], horizontal=True)

# Auto-rerun timer (1 hour) — only really useful in Calendar URL mode
if mode == "Calendar URL":
    st_autorefresh(interval=3600 * 1000, key="auto_refresh_hourly")

events: List[Tuple[str, date]] = []
last_sync_label: Optional[str] = None

# Default expansion window + filter window
today = date.today()
default_end = today + timedelta(days=180)

st.subheader("Date window")
window_start = st.date_input("Start date", value=today)
window_end = st.date_input("End date", value=default_end)

if window_end < window_start:
    st.error("End date must be on/after start date.")
    st.stop()

# -------- CSV mode --------
if mode == "Upload CSV":
    file = st.file_uploader("Upload CSV", type=["csv"])
    if file:
        df = pd.read_csv(file)
        st.caption(f"Detected columns: {list(df.columns)}")
        name_col = st.selectbox("Title column", df.columns)
        date_col = st.selectbox("Date column", df.columns)

        for _, r in df.iterrows():
            d = parse_date_any(r.get(date_col))
            if not d:
                continue
            if window_start <= d <= window_end:
                events.append((str(r.get(name_col, "")), d))

# -------- Calendar URL mode --------
if mode == "Calendar URL":
    url = st.text_input("Paste ICS URL (subscription link ending in .ics)")
    c1, c2 = st.columns([1, 1])
    with c1:
        sync_now = st.button("Sync Now", disabled=not bool(url))
    with c2:
        st.caption("Auto-refresh: every 1 hour")

    if url and sync_now:
        fetch_ics_text.clear()
        st.rerun()

    if url:
        try:
            text, last_sync_label = fetch_ics_text(url)
            st.caption(f"Last sync: {last_sync_label}")
            events = parse_ics_events_expand(text, window_start, window_end)
        except Exception as e:
            st.error(f"Failed to fetch/parse calendar: {e}")

# -------- Preview --------
if events:
    df_events = pd.DataFrame(events, columns=["Title", "Date"])
    st.subheader("Preview (first 100)")
    st.dataframe(df_events.head(100), use_container_width=True)

    # -------- Process --------
    results = []
    rejected = []

    for _, r in df_events.iterrows():
        title_raw = str(r["Title"])
        d = r["Date"]
        if not isinstance(d, date):
            rejected.append((title_raw, "Invalid date"))
            continue

        shift = calc_shift(title_raw, d, role)
        if not shift:
            rejected.append((title_raw, "Unrecognized format/rules"))
            continue

        start, end = shift_start_end(shift)
        if not start or not end:
            rejected.append((title_raw, "Could not map shift to time"))
            continue

        results.append(
            {
                "Event": normalize_title(title_raw),
                "Shift": shift,
                "Start": start,
                "End": end,
                "Date": d,
            }
        )

    out = pd.DataFrame(results).sort_values(["Date", "Start", "Event"]) if results else pd.DataFrame(
        columns=["Event", "Shift", "Start", "End", "Date"]
    )

    st.subheader("Processed")
    st.dataframe(out, use_container_width=True)

    # Downloads
    st.subheader("Exports")
    st.download_button("Download processed CSV", out.to_csv(index=False).encode("utf-8"), "processed.csv", "text/csv")

    out_ics = build_output_ics(out)
    st.download_button("Download Outlook ICS", out_ics, "schedule.ics", "text/calendar")

    # Publish to S3 (stable subscription URL)
    st.subheader("Subscription feed (S3)")
    try:
        ics_hash = hashlib.sha256(out_ics.encode("utf-8")).hexdigest()
        last_hash = st.session_state.get("last_uploaded_ics_hash")

        # Upload only when changed (or first time)
        if last_hash != ics_hash:
            sub_url = upload_ics_to_s3(out_ics)
            st.session_state["last_uploaded_ics_hash"] = ics_hash
            st.success("Published updated ICS feed to S3.")
        else:
            sub_url = f"https://{st.secrets['S3_BUCKET']}.s3.{st.secrets['AWS_REGION']}.amazonaws.com/{st.secrets.get('S3_KEY','hmu-shifts.ics')}"
            st.info("No changes since last publish; feed URL unchanged.")

        st.write("Paste this URL into Outlook/Apple/Google calendar **once** to subscribe:")
        st.code(sub_url)
    except ClientError as e:
        st.error(f"S3 upload failed: {e}")

    # Rejected
    if rejected:
        st.subheader("Rejected")
        rej_df = pd.DataFrame(rejected, columns=["Event", "Reason"])
        st.dataframe(rej_df, use_container_width=True)
        st.download_button("Download rejected rows (CSV)", rej_df.to_csv(index=False).encode("utf-8"), "rejected.csv", "text/csv")
else:
    st.info("Provide a CSV or an ICS calendar URL to begin.")
