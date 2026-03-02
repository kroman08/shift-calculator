import re
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
    Returns: Early / Middle / Late / Fixed, or None (leave untouched)
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
# ICS INPUT: FETCH / READ + EXPAND
# =====================================================
@st.cache_data(ttl=3600)
def fetch_ics_text(url: str) -> Tuple[str, str]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _is_all_day(dt_val: Any) -> bool:
    return isinstance(dt_val, date) and not isinstance(dt_val, datetime)

def _as_eastern(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=APP_TZ)
    return dt.astimezone(APP_TZ)

def _clamp_same_day(start_dt: datetime, end_dt: datetime) -> datetime:
    if end_dt.date() != start_dt.date():
        end_dt = datetime.combine(start_dt.date(), end_dt.timetz()).replace(tzinfo=start_dt.tzinfo)
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

def expand_events(cal_text: str, window_start: date, window_end: date) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
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
            if window_start <= d <= window_end:
                events.append({"title": title, "start_dt": d, "end_dt": None, "all_day": True, "uid": uid})
            continue

        if not isinstance(dtstart_val, datetime):
            errors.append((title, "Invalid DTSTART"))
            continue

        dtstart = _as_eastern(dtstart_val)
        dtend = _as_eastern(dtend_val) if isinstance(dtend_val, datetime) else (dtstart + timedelta(hours=1))
        dtend = _clamp_same_day(dtstart, dtend)

        rrule_prop = comp.get("RRULE")
        exdates = _collect_exdates(comp)

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
                occ_end = _clamp_same_day(occ, occ + duration)
                events.append({"title": title, "start_dt": occ, "end_dt": occ_end, "all_day": False, "uid": uid})
        else:
            d = dtstart.date()
            if window_start <= d <= window_end:
                events.append({"title": title, "start_dt": dtstart, "end_dt": dtend, "all_day": False, "uid": uid})

    # De-dupe
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
# OUTPUT ICS BUILD + MARKER
# =====================================================
def build_output_ics(processed_rows: List[Dict[str, Any]],
                     untouched_rows: List[Dict[str, Any]],
                     feed_id_for_uid: str) -> bytes:
    cal = Calendar()
    cal.add("prodid", "-//HMU Shift Processor//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("X-WR-TIMEZONE", "America/New_York")

    # 1-minute transparent "Last Updated" marker @ 12:01 AM
    updated_now = datetime.now(APP_TZ)
    meta = Event()
    meta.add("summary", f"HMU Shifts — Last Updated: {updated_now.strftime('%Y-%m-%d %H:%M')}")
    meta.add("uid", f"hmushifts-last-updated@{feed_id_for_uid}")
    meta.add("dtstamp", updated_now)
    start_marker = datetime.combine(updated_now.date(), time(0, 1)).replace(tzinfo=APP_TZ)
    meta.add("dtstart", start_marker)
    meta.add("dtend", start_marker + timedelta(minutes=1))
    meta.add("transp", "TRANSPARENT")
    meta.add("description", "System-generated update marker. Safe to ignore.")
    cal.add_component(meta)

    def add_row(row: Dict[str, Any]) -> None:
        ev = Event()
        ev.add("summary", row["title"])
        ev.add("uid", row.get("uid") or f"{hashlib.sha256((row['title']+str(row['start_dt'])).encode()).hexdigest()[:16]}@hmu-shifts")
        ev.add("dtstamp", datetime.now(APP_TZ))

        if row["all_day"]:
            ev.add("dtstart", row["start_dt"])
        else:
            ev.add("dtstart", _as_eastern(row["start_dt"]))
            ev.add("dtend", _as_eastern(row["end_dt"]))

        cal.add_component(ev)

    for r in processed_rows:
        add_row(r)
    for r in untouched_rows:
        add_row(r)

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

def head_feed(bucket: str, key: str) -> Optional[dict]:
    try:
        return s3_client().head_object(Bucket=bucket, Key=key)
    except ClientError:
        return None

def put_feed(bucket: str, key: str, body: bytes, metadata: Dict[str, str]) -> None:
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/calendar; charset=utf-8",
        CacheControl="no-cache, max-age=0",
        Metadata=metadata,
    )

# =====================================================
# STREAMLIT UI
# =====================================================
st.set_page_config(page_title="HMU Shift Processor", layout="centered")
st.title("HMU Shift Processor")

# hourly rerun while open
st_autorefresh(interval=3600 * 1000, key="auto_refresh_hourly")

st.info("Returning users: go to the bottom **Publish Subscription Feed** section and click **Restore Feed** (next to Publish).")

# ---- Defaults
today = date.today()
default_end = date(today.year + 1, 6, 30)

# ---- Role + Date window (original structure)
role = st.selectbox("Role", ["MD", "APP"], index=0, key="role_select")

st.subheader("Date window")
window_start = st.date_input("Start date", value=today, key="window_start")
window_end = st.date_input("End date", value=st.session_state.get("window_end_override", default_end), key="window_end")

if window_end < window_start:
    st.error("End date must be on/after start date.")
    st.stop()

# ---- Input
input_mode = st.radio("Input method", ["Subscription URL", "Upload ICS file"], horizontal=True, key="input_mode")

cal_text: Optional[str] = None
url: Optional[str] = None

if input_mode == "Subscription URL":
    url = st.text_input("Paste ICS subscription URL (.ics)", value=st.session_state.get("source_url_override", ""), key="source_url")
    colA, colB = st.columns([1, 1])
    with colA:
        sync_now = st.button("Sync Now", disabled=not bool(url), key="sync_now")
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
    up = st.file_uploader("Upload an .ics file", type=["ics"], key="ics_uploader")
    if up is not None:
        cal_text = up.getvalue().decode("utf-8", errors="replace")
        st.caption("Loaded uploaded ICS file.")

# ---- Parse
events, data_errors = expand_events(cal_text, window_start, window_end)

processed_rows: List[Dict[str, Any]] = []
untouched_rows: List[Dict[str, Any]] = []
rejected_rows: List[Tuple[str, str]] = list(data_errors)

for e in events:
    if e["all_day"]:
        untouched_rows.append(e)
        continue

    sd = _as_eastern(e["start_dt"])
    ed = _clamp_same_day(sd, _as_eastern(e["end_dt"]))

    shift_type = calc_shift_type(e["title"], sd.date(), role)

    if shift_type:
        st_t, en_t = shift_times(shift_type)
        new_start = datetime.combine(sd.date(), st_t).replace(tzinfo=APP_TZ)
        new_end = datetime.combine(sd.date(), en_t).replace(tzinfo=APP_TZ)
        processed_rows.append({"title": e["title"], "start_dt": new_start, "end_dt": new_end, "all_day": False, "uid": e.get("uid")})
    else:
        untouched_rows.append({"title": e["title"], "start_dt": sd, "end_dt": ed, "all_day": False, "uid": e.get("uid")})

# ---- Display
def to_display_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    disp = []
    for r in rows:
        if r.get("all_day"):
            d = r["start_dt"]
            disp.append({"Event": r["title"], "Date": d, "Start": "(all-day)", "End": ""})
        else:
            sd = r["start_dt"]
            ed = r["end_dt"]
            disp.append({"Event": r["title"], "Date": sd.date(), "Start": sd.strftime("%H:%M"), "End": ed.strftime("%H:%M")})
    df = pd.DataFrame(disp, columns=["Event", "Date", "Start", "End"])
    if not df.empty:
        df = df.sort_values(["Date", "Start", "Event"], kind="mergesort")
        df["Date"] = df["Date"].apply(lambda x: x.strftime("%m-%d-%Y"))
    return df

processed_df = to_display_df(processed_rows)
untouched_df = to_display_df(untouched_rows)
rejected_df = pd.DataFrame(rejected_rows, columns=["Event", "Reason"]) if rejected_rows else pd.DataFrame(columns=["Event", "Reason"])

total_count = len(processed_rows) + len(untouched_rows) + len(rejected_rows)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Events", total_count)
c2.metric("Processed", len(processed_rows))
c3.metric("Left Untouched", len(untouched_rows))
c4.metric("Rejected (Errors)", len(rejected_rows))

filter_option = st.selectbox("Filter View", ["Show All", "Processed Only", "Left Untouched Only", "Errors Only"], key="filter_view")

if filter_option in ("Show All", "Processed Only"):
    badge("Processed", "#2E8B57")
    st.dataframe(processed_df, use_container_width=True)

if filter_option in ("Show All", "Left Untouched Only"):
    badge("Left Untouched", "#F4A300")
    st.dataframe(untouched_df, use_container_width=True)

if filter_option in ("Show All", "Errors Only"):
    badge("Data Errors", "#B22222")
    st.dataframe(rejected_df, use_container_width=True)

# ---- Download output
output_ics_bytes_preview = build_output_ics(processed_rows, untouched_rows, feed_id_for_uid="download")
st.subheader("Download output")
st.download_button(
    "Download output as ICS",
    data=output_ics_bytes_preview,
    file_name="hmu_output.ics",
    mime="text/calendar",
    key="download_btn"
)

# =====================================================
# PUBLISH + RESTORE AT THE BOTTOM (SIDE-BY-SIDE BUTTONS)
# =====================================================
st.subheader("Publish Subscription Feed")

st.markdown(
    "You can publish a subscription URL (paste into Outlook/Apple Calendar once). "
    "A token is needed if you later republish/update the calendar subscription URL "
    "associated with this Feed ID. This prevents different people from accidentally "
    "overwriting each other’s calendars."
)

st.caption(
    "*Ownership token format: First initial + Middle initial + Last initial + Birth month (MM). Example: JMS01*"
)
st.caption(
    "*Forgot your Feed ID? In your calendar app, open the subscribed calendar info. "
    "In the URL it appears as .../feeds/yourFeedID.ics*"
)

feed_id = st.text_input("Feed ID", key="publish_feed_id")
token_in = st.text_input("Ownership Token", type="password", key="publish_token")

col_pub, col_restore = st.columns(2)

bucket = st.secrets["S3_BUCKET"]
region = st.secrets["AWS_REGION"]

def show_post_publish_messages(sub_url: str):
    st.success("Success.")
    st.info(
        "Reminder: this is your existing subscription URL. "
        "If you are already subscribed to it in your calendar app, no further action is needed."
    )
    st.code(sub_url)
    st.caption("To confirm the last calendar update, search your calendar for 'HMU Shifts — Last Updated'.")
    st.warning(
        "Important: The subscription URL does NOT automatically update in the background. "
        "If your source calendar changes, you must reopen this app and click Sync Now to re-parse and republish. "
        "Otherwise, the subscription calendar will remain unchanged."
    )

with col_pub:
    if st.button("Publish Feed", key="publish_btn"):
        if not feed_id or not token_in:
            st.error("Feed ID and Ownership Token are required.")
        else:
            key = f"feeds/{feed_id}.ics"
            sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

            existing = head_feed(bucket, key)
            if existing:
                stored = (existing.get("Metadata", {}) or {}).get("owner-token", "")
                if token_in.strip().upper() != stored.upper():
                    st.error("Incorrect ownership token. Publish blocked.")
                else:
                    final_ics = build_output_ics(processed_rows, untouched_rows, feed_id_for_uid=feed_id)
                    put_feed(
                        bucket, key, final_ics,
                        metadata={
                            "owner-token": stored,
                            "source-url": url or "",
                            "role": role,
                            "window-end": str(window_end),
                        }
                    )
                    show_post_publish_messages(sub_url)
            else:
                # create new
                final_ics = build_output_ics(processed_rows, untouched_rows, feed_id_for_uid=feed_id)
                put_feed(
                    bucket, key, final_ics,
                    metadata={
                        "owner-token": token_in.strip().upper(),
                        "source-url": url or "",
                        "role": role,
                        "window-end": str(window_end),
                    }
                )
                show_post_publish_messages(sub_url)

with col_restore:
    if st.button("Restore Feed", key="restore_btn"):
        if not feed_id or not token_in:
            st.error("Feed ID and Ownership Token are required.")
        else:
            key = f"feeds/{feed_id}.ics"
            sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

            existing = head_feed(bucket, key)
            if not existing:
                st.error("Feed ID not found.")
            else:
                md = existing.get("Metadata", {}) or {}
                stored_token = md.get("owner-token", "")
                if token_in.strip().upper() != stored_token.upper():
                    st.error("Incorrect ownership token.")
                else:
                    restored_url = md.get("source-url", "")
                    restored_role = md.get("role", "MD")
                    restored_end = md.get("window-end", "")

                    if not restored_url:
                        st.error("No source URL is stored for this Feed ID.")
                    else:
                        # Repopulate UI fields (and re-run immediately)
                        st.session_state["input_mode"] = "Subscription URL"
                        st.session_state["source_url_override"] = restored_url
                        st.session_state["role_select"] = restored_role

                        # Start date stays visible and resets to today
                        st.session_state["window_start"] = date.today()
                        try:
                            st.session_state["window_end_override"] = date.fromisoformat(restored_end) if restored_end else window_end
                        except Exception:
                            st.session_state["window_end_override"] = window_end

                        # Re-fetch + re-parse + republish now
                        try:
                            cal_text_restore, _ = fetch_ics_text(restored_url)
                            ws = date.today()
                            we = st.session_state["window_end_override"]

                            evs, errs = expand_events(cal_text_restore, ws, we)

                            proc2, unt2 = [], []
                            for e in evs:
                                if e["all_day"]:
                                    unt2.append(e)
                                    continue
                                sd = _as_eastern(e["start_dt"])
                                ed = _clamp_same_day(sd, _as_eastern(e["end_dt"]))
                                s_type = calc_shift_type(e["title"], sd.date(), restored_role)
                                if s_type:
                                    st_t, en_t = shift_times(s_type)
                                    ns = datetime.combine(sd.date(), st_t).replace(tzinfo=APP_TZ)
                                    ne = datetime.combine(sd.date(), en_t).replace(tzinfo=APP_TZ)
                                    proc2.append({"title": e["title"], "start_dt": ns, "end_dt": ne, "all_day": False, "uid": e.get("uid")})
                                else:
                                    unt2.append({"title": e["title"], "start_dt": sd, "end_dt": ed, "all_day": False, "uid": e.get("uid")})

                            final_ics = build_output_ics(proc2, unt2, feed_id_for_uid=feed_id)

                            put_feed(
                                bucket, key, final_ics,
                                metadata={
                                    "owner-token": stored_token,
                                    "source-url": restored_url,
                                    "role": restored_role,
                                    "window-end": str(st.session_state["window_end_override"]),
                                }
                            )

                            last_mod = existing.get("LastModified")
                            if last_mod:
                                st.info(f"Previous publish timestamp: {last_mod.astimezone(APP_TZ).strftime('%Y-%m-%d %H:%M')}")

                            show_post_publish_messages(sub_url)
                            st.success("Fields were restored and the feed was republished.")

                            # refresh UI with restored values
                            st.rerun()

                        except Exception as e:
                            st.error(f"Restore+republish failed: {e}")
if not cal_text:
    st.info("Provide an ICS subscription URL or upload an ICS file to begin.")
    st.stop()
