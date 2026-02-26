import re
import secrets
import hashlib
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


# =====================================================
# CONFIG
# =====================================================

ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2  # 7/1/2025 is Day 2

SPECIAL_BLUE_PERIODS = [
    (date(2025, 7, 7),  date(2025, 8, 3)),
    (date(2025, 9, 29), date(2025, 10, 26)),
    (date(2026, 1, 5),  date(2026, 2, 1)),
    (date(2026, 4, 6),  date(2026, 5, 3)),
]

FIXED_MD_COLORS = {"yellow", "purple", "blue", "bronze", "green", "orange"}

# Yellow, Purple, Blue (standard), Bronze, Orange share this table
YPBB_SUFFIX_ROTATION = {
    "1-1": {1: "Early", 2: "Middle", 3: "Late", 4: "Middle"},
    "1-2": {1: "Middle", 2: "Late", 3: "Middle", 4: "Early"},
    "2-1": {1: "Late", 2: "Middle", 3: "Early", 4: "Middle"},
    "2-2": {1: "Middle", 2: "Early", 3: "Middle", 4: "Late"},
    "3":   {1: "Middle", 2: "Early", 3: "Middle", 4: "Late"},
}

GREEN_SUFFIX_ROTATION = {
    "1": {1: "Early", 2: "Middle", 3: "Late", 4: "Middle"},
    "2": {1: "Middle", 2: "Late", 3: "Middle", 4: "Early"},
    "3": {1: "Late", 2: "Middle", 3: "Early", 4: "Middle"},
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
    """Remove symbols like ($). Keep only letters/numbers/spaces/hyphens. Collapse spaces."""
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
# SHIFT RULE ENGINE (FULL)
# =====================================================

def calc_shift_type(title: str, target_date: date, role: str) -> Optional[str]:
    """
    Returns one of: Early, Middle, Late, Fixed, or None (meaning: leave untouched).
    """
    clean_shift = clean_input(normalize_title(title))
    if not clean_shift:
        return None

    # Gray collaborator correction when MD is selected
    if role == "MD":
        clean_shift = clean_shift.replace("gray 1 collaborator", "gray 1 md")
        clean_shift = clean_shift.replace("gray 2 collaborator", "gray 2 md")

    day_num = get_day_number(target_date)

    # MD fixed override for certain colors
    if role == "MD":
        first = clean_shift.split(" ")[0] if clean_shift.split(" ") else ""
        if first in FIXED_MD_COLORS:
            return "Fixed"

    # Gray / MIST transplant roles (explicit multi-word roles)
    if clean_shift in ALL_GRAY_MIST_ROLES:
        if clean_shift in GRAY_MIST_EARLY_ON_1_3:
            return "Early" if day_num in (1, 3) else "Middle"
        if clean_shift in GRAY_MIST_MIDDLE_ON_1_3:
            return "Middle" if day_num in (1, 3) else "Early"
        return None

    if clean_shift.startswith("mist scu"):
        return MIST_SCU_ROTATION.get(day_num)

    parts = clean_shift.split()
    shift_name = parts[0]
    suffix = parts[1] if len(parts) > 1 else ""
    num_str = parts[1] if len(parts) > 1 else ""

    # GOLD (2025-2026 rules you provided)
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

    # BLUE
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

    # YELLOW / PURPLE / BRONZE / ORANGE
    if shift_name in ("yellow", "purple", "bronze", "orange"):
        if not suffix:
            return None
        if suffix in YPBB_SUFFIX_ROTATION:
            return YPBB_SUFFIX_ROTATION[suffix].get(day_num)
        return None

    # GREEN
    if shift_name == "green":
        if not suffix:
            return None
        if suffix in GREEN_SUFFIX_ROTATION:
            return GREEN_SUFFIX_ROTATION[suffix].get(day_num)
        return None

    # If not matched: leave untouched
    return None

def shift_times(shift_type: str) -> Tuple[time, time]:
    """
    Map shift type -> start/end.
    Late = 8:00 start -> 18:45 end (per request).
    """
    if shift_type == "Early":
        return time(6, 45), time(17, 0)
    if shift_type == "Middle":
        return time(8, 0), time(17, 0)
    if shift_type == "Late":
        return time(8, 0), time(18, 45)
    if shift_type == "Fixed":
        return time(8, 0), time(17, 0)
    # fallback (shouldn't happen)
    return time(8, 0), time(17, 0)


# =====================================================
# INPUT: FETCH URL / READ UPLOADED ICS
# =====================================================

@st.cache_data(ttl=3600)
def fetch_ics_text(url: str) -> Tuple[str, str]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def coerce_dt(x: Any) -> Optional[datetime]:
    """Return datetime if possible, else None."""
    if isinstance(x, datetime):
        return x
    return None

def coerce_date(x: Any) -> Optional[date]:
    """Return date if possible, else None."""
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    if isinstance(x, datetime):
        return x.date()
    return None

def collect_exdates(comp) -> set:
    ex = set()
    exprop = comp.get("EXDATE")
    if not exprop:
        return ex
    if not isinstance(exprop, list):
        exprop = [exprop]
    for item in exprop:
        for dtv in getattr(item, "dts", []):
            d = coerce_date(getattr(dtv, "dt", None))
            if d:
                ex.add(d)
    return ex

def collect_rdates(comp) -> set:
    rd = set()
    rdprop = comp.get("RDATE")
    if not rdprop:
        return rd
    if not isinstance(rdprop, list):
        rdprop = [rdprop]
    for item in rdprop:
        for dtv in getattr(item, "dts", []):
            d = coerce_date(getattr(dtv, "dt", None))
            if d:
                rd.add(d)
    return rd

def expand_events(cal_text: str, window_start: date, window_end: date) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
    """
    Returns (events, errors)

    events: list of dicts containing:
      - title, start_dt, end_dt, all_day(bool), uid(str), tzinfo
    errors: list of (event_title, reason)
    """
    errors = []
    out: List[Dict[str, Any]] = []

    cal = Calendar.from_ical(cal_text)

    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue

        title = str(comp.get("SUMMARY", "")).strip()
        uid = str(comp.get("UID", "")).strip() or None

        dtstart_raw = comp.get("DTSTART")
        if not dtstart_raw:
            errors.append((title or "(missing title)", "Missing DTSTART"))
            continue
        dtstart_val = dtstart_raw.dt

        dtend_val = comp.get("DTEND").dt if comp.get("DTEND") else None

        # all-day event
        all_day = isinstance(dtstart_val, date) and not isinstance(dtstart_val, datetime)

        if all_day:
            d = coerce_date(dtstart_val)
            if not d:
                errors.append((title or "(missing title)", "Invalid DTSTART date"))
                continue

            if not (window_start <= d <= window_end):
                continue

            out.append({
                "title": title,
                "start_dt": d,   # store as date for all-day
                "end_dt": None,  # optional
                "all_day": True,
                "uid": uid,
                "tzinfo": None,
            })
            continue

        # timed event
        dtstart = coerce_dt(dtstart_val)
        if not dtstart:
            errors.append((title or "(missing title)", "Invalid DTSTART datetime"))
            continue

        # if no DTEND, default 1 hour
        if isinstance(dtend_val, datetime):
            dtend = dtend_val
        else:
            dtend = dtstart + timedelta(hours=1)

        tzinfo = dtstart.tzinfo

        # Handle recurrence expansion (process each occurrence independently)
        rrule_prop = comp.get("RRULE")
        exdates = collect_exdates(comp)
        rdates = collect_rdates(comp)

        if rrule_prop:
            # dateutil needs DTSTART; rrulestr parses string like "FREQ=WEEKLY;BYDAY=MO"
            rrule_str = rrule_prop.to_ical().decode("utf-8") if isinstance(rrule_prop.to_ical(), (bytes, bytearray)) else str(rrule_prop.to_ical())
            rule = rrulestr(rrule_str, dtstart=dtstart)

            # Expand within window (inclusive)
            ws = datetime.combine(window_start, time.min).replace(tzinfo=tzinfo) if tzinfo else datetime.combine(window_start, time.min)
            we = datetime.combine(window_end, time.max).replace(tzinfo=tzinfo) if tzinfo else datetime.combine(window_end, time.max)

            duration = dtend - dtstart

            for occ in rule.between(ws, we, inc=True):
                occ_date = occ.date()
                if occ_date in exdates:
                    continue
                if not (window_start <= occ_date <= window_end):
                    continue
                occ_end = occ + duration

                out.append({
                    "title": title,
                    "start_dt": occ,
                    "end_dt": occ_end,
                    "all_day": False,
                    "uid": uid,
                    "tzinfo": tzinfo,
                })

            # Include explicit RDATEs too
            for rd in rdates:
                if window_start <= rd <= window_end and rd not in exdates:
                    occ = datetime.combine(rd, dtstart.timetz())
                    if tzinfo:
                        occ = occ.replace(tzinfo=tzinfo)
                    occ_end = occ + (dtend - dtstart)
                    out.append({
                        "title": title,
                        "start_dt": occ,
                        "end_dt": occ_end,
                        "all_day": False,
                        "uid": uid,
                        "tzinfo": tzinfo,
                    })
        else:
            # Non-recurring: keep if in window by start date
            d = dtstart.date()
            if not (window_start <= d <= window_end):
                continue
            out.append({
                "title": title,
                "start_dt": dtstart,
                "end_dt": dtend,
                "all_day": False,
                "uid": uid,
                "tzinfo": tzinfo,
            })

    # Deduplicate exact same title + start_dt + end_dt
    seen = set()
    uniq = []
    for e in out:
        key = (e["title"], str(e["start_dt"]), str(e["end_dt"]))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)

    return uniq, errors


# =====================================================
# OUTPUT ICS BUILD (preserve TZ)
# =====================================================

def stable_uid(feed_id: str, title: str, start_dt: Any) -> str:
    raw = f"{feed_id}|{title}|{start_dt}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:20] + "@hmu-shifts"

def build_output_ics(
    processed_rows: List[Dict[str, Any]],
    untouched_rows: List[Dict[str, Any]],
    feed_id_for_uid: str = "download"
) -> bytes:
    cal = Calendar()
    cal.add("prodid", "-//HMU Shift Processor//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")

    def add_event_row(row: Dict[str, Any]) -> None:
        title = row["title"]
        all_day = row["all_day"]
        uid = row.get("uid") or stable_uid(feed_id_for_uid, title, row["start_dt"])

        ev = Event()
        ev.add("summary", title)
        ev.add("uid", uid)
        ev.add("dtstamp", datetime.now())

        if all_day:
            ev.add("dtstart", row["start_dt"])  # date
            # (optional) no dtend for all-day; many clients accept dtstart only
        else:
            ev.add("dtstart", row["start_dt"])  # datetime with tzinfo preserved if present
            ev.add("dtend", row["end_dt"])

        cal.add_component(ev)

    for r in processed_rows:
        add_event_row(r)
    for r in untouched_rows:
        add_event_row(r)

    return cal.to_ical()


# =====================================================
# S3 PUBLISH WITH OWNERSHIP TOKEN
# =====================================================

def s3_client():
    return boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )

def get_existing_token(bucket: str, key: str) -> Optional[str]:
    s3 = s3_client()
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        return resp.get("Metadata", {}).get("owner-token")
    except ClientError:
        return None

def upload_feed(bucket: str, key: str, ics_bytes: bytes, token: str) -> None:
    s3 = s3_client()
    s3.put_object(
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
# STREAMLIT APP
# =====================================================

st.set_page_config(page_title="HMU Shift Processor", layout="centered")
st.title("HMU Shift Processor")

# Default role = MD
role = st.selectbox("Role", ["MD", "APP"], index=0)

# Auto-rerun hourly
st_autorefresh(interval=3600 * 1000, key="auto_refresh_hourly")

# Input method
input_mode = st.radio("Input method", ["Subscription URL", "Upload ICS file"], horizontal=True)

# Default window: today → June 30 of next calendar year
today = date.today()
default_end = date(today.year + 1, 6, 30)

st.subheader("Date window")
window_start = st.date_input("Start date", value=today)
window_end = st.date_input("End date", value=default_end)

if window_end < window_start:
    st.error("End date must be on/after start date.")
    st.stop()

cal_text = None
last_sync = None

if input_mode == "Subscription URL":
    url = st.text_input("Paste ICS subscription URL (.ics)")
    colA, colB = st.columns([1, 1])
    with colA:
        sync_now = st.button("Sync Now", disabled=not bool(url))
    with colB:
        st.caption("Auto-refresh: every 1 hour")

    if url and sync_now:
        fetch_ics_text.clear()
        st.rerun()

    if url:
        try:
            cal_text, last_sync = fetch_ics_text(url)
            st.caption(f"Last sync: {last_sync}")
        except Exception as e:
            st.error(f"Failed to fetch calendar: {e}")

else:
    up = st.file_uploader("Upload an .ics file", type=["ics"])
    if up is not None:
        try:
            cal_text = up.getvalue().decode("utf-8", errors="replace")
            st.caption("Loaded uploaded ICS file.")
        except Exception as e:
            st.error(f"Failed to read uploaded file: {e}")

if not cal_text:
    st.info("Provide an ICS subscription URL or upload an ICS file to begin.")
    st.stop()

events, data_errors = expand_events(cal_text, window_start, window_end)

# Build processed / untouched / rejected lists for display and output
processed_rows: List[Dict[str, Any]] = []
untouched_rows: List[Dict[str, Any]] = []
rejected_rows: List[Tuple[str, str]] = list(data_errors)

for e in events:
    title = e["title"]
    all_day = e["all_day"]

    if all_day:
        # leave all-day events untouched
        untouched_rows.append(e)
        continue

    start_dt: datetime = e["start_dt"]
    end_dt: datetime = e["end_dt"]
    tzinfo = e.get("tzinfo")

    shift_type = calc_shift_type(title, start_dt.date(), role)

    if shift_type:
        # Recalculate start/end but preserve timezone
        start_t, end_t = shift_times(shift_type)

        new_start = datetime.combine(start_dt.date(), start_t)
        new_end = datetime.combine(start_dt.date(), end_t)

        if tzinfo is not None:
            new_start = new_start.replace(tzinfo=tzinfo)
            new_end = new_end.replace(tzinfo=tzinfo)

        processed_rows.append({
            "title": title,
            "start_dt": new_start,
            "end_dt": new_end,
            "all_day": False,
            "uid": e.get("uid"),
            "tzinfo": tzinfo,
        })
    else:
        # Keep untouched (title, time, timezone)
        untouched_rows.append(e)

# ---------------------------
# Display tables (sorted, formatted)
# ---------------------------

def to_display_df(rows: List[Dict[str, Any]], include_reason: bool = False) -> pd.DataFrame:
    if include_reason:
        df = pd.DataFrame(rows, columns=["Event", "Reason"])
        return df

    # Rows may include all-day (start_dt is date) and timed (datetime)
    disp = []
    for r in rows:
        title = r["title"]
        if r["all_day"]:
            d = r["start_dt"]
            disp.append({
                "Event": title,
                "Date": d,
                "Start": "(all-day)",
                "End": "",
            })
        else:
            sd: datetime = r["start_dt"]
            ed: datetime = r["end_dt"]
            disp.append({
                "Event": title,
                "Date": sd.date(),
                "Start": sd.strftime("%H:%M"),
                "End": ed.strftime("%H:%M"),
            })

    df = pd.DataFrame(disp, columns=["Event", "Date", "Start", "End"])
    if not df.empty:
        df = df.sort_values(["Date", "Start", "Event"], kind="mergesort")
        df["Date"] = df["Date"].apply(lambda x: x.strftime("%m-%d-%Y"))
    return df

processed_df = to_display_df(processed_rows)
untouched_df = to_display_df(untouched_rows)
rejected_df = pd.DataFrame(rejected_rows, columns=["Event", "Reason"]) if rejected_rows else pd.DataFrame(columns=["Event", "Reason"])

# Counts summary
total_count = len(processed_rows) + len(untouched_rows) + len(rejected_rows)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Events", total_count)
c2.metric("Processed", len(processed_rows))
c3.metric("Left Untouched", len(untouched_rows))
c4.metric("Rejected (Errors)", len(rejected_rows))

filter_option = st.selectbox(
    "Filter View",
    ["Show All", "Processed Only", "Left Untouched Only", "Errors Only"]
)

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
# Publish subscription feed (S3) with token ownership
# ---------------------------

st.subheader("Publish Subscription Feed (S3)")

st.markdown(
    "You can publish a **subscription URL** (paste into Outlook once). "
    "A token is only needed if you later republish/update the same Feed ID. "
    "It is **not** needed for auto-refresh or Sync Now."
)

feed_id = st.text_input("Feed ID (3–40 letters/numbers/_/-)")
valid_id = bool(re.match(r"^[A-Za-z0-9_-]{3,40}$", feed_id or ""))

publish_clicked = st.button("Publish / Update feed", disabled=not valid_id)

if publish_clicked and valid_id:
    bucket = st.secrets["S3_BUCKET"]
    region = st.secrets["AWS_REGION"]
    key = f"feeds/{feed_id}.ics"

    existing_token = get_existing_token(bucket, key)

    if existing_token:
        st.warning("This Feed ID already exists.")
        entered = st.text_input("Enter ownership token to update this feed", type="password")

        # If token prompt empty, stop here (so user sees prompt)
        if not entered:
            st.stop()

        if entered != existing_token:
            st.error("Incorrect ownership token. Cannot overwrite this feed.")
        else:
            upload_feed(bucket, key, output_ics_bytes, existing_token)
            st.success(f"Feed updated. Last published: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
            st.write("Subscription URL:")
            st.code(sub_url)
    else:
        token = generate_token()
        upload_feed(bucket, key, output_ics_bytes, token)
        st.success(f"Feed created. Last published: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
        st.write("Subscription URL:")
        st.code(sub_url)

        st.error("IMPORTANT: Save this ownership token. You will need it to update this feed later.")
        st.code(token)
