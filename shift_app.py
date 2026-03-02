# ============================================================
# HMU SHIFT PROCESSOR – FINAL VERSION
# ============================================================

import re
from datetime import datetime, date, time, timedelta
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError
from icalendar import Calendar, Event
from zoneinfo import ZoneInfo

APP_TZ = ZoneInfo("America/New_York")

# ============================================================
# S3 CLIENT
# ============================================================

def s3_client():
    return boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )

# ============================================================
# SHIFT RULES
# ============================================================

ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2

def get_day_number(d):
    delta = (d - ANCHOR_DATE).days
    return (delta + ANCHOR_DAY_NUM - 1) % 4 + 1

def shift_times(shift_type):
    if shift_type == "Early":
        return time(6,45), time(17,0)
    if shift_type == "Middle":
        return time(8,0), time(17,0)
    if shift_type == "Late":
        return time(8,0), time(18,45)
    return None

def normalize_title(title):
    return re.sub(r"[^A-Za-z0-9\s\-]", "", title or "").strip()

def calc_shift_type(title, d, role):
    t = normalize_title(title).lower()
    day_num = get_day_number(d)

    if t.startswith("gold"):
        parts = t.split()
        if len(parts) >= 2 and parts[1].isdigit():
            n = int(parts[1])
            if n == 1:
                return "Early"
            if n >= 6:
                return "Middle"
            if n in [3,5]:
                return "Early" if day_num in [1,3] else "Middle"
            if n in [2,4]:
                return "Middle" if day_num in [1,3] else "Early"

    if t.startswith("silver"):
        parts = t.split()
        if len(parts) == 1:
            return "Early"
        if len(parts) >= 2 and parts[1].isdigit():
            return "Early" if int(parts[1]) == 1 else "Middle"

    return None

# ============================================================
# BUILD ICS
# ============================================================

def build_output_ics(processed, untouched, feed_id):
    cal = Calendar()
    cal.add("prodid", "-//HMU Shift Processor//EN")
    cal.add("version", "2.0")
    cal.add("X-WR-TIMEZONE", "America/New_York")

    updated_now = datetime.now(APP_TZ)

    # --- Last Updated Marker ---
    meta_event = Event()
    meta_event.add(
        "summary",
        f"HMU Shifts — Last Updated: {updated_now.strftime('%Y-%m-%d %H:%M')}"
    )
    meta_event.add("uid", f"hmushifts-last-updated@{feed_id}")
    meta_event.add("dtstamp", updated_now)

    start_marker = datetime.combine(
        updated_now.date(),
        time(0,1)
    ).replace(tzinfo=APP_TZ)

    end_marker = start_marker + timedelta(minutes=1)

    meta_event.add("dtstart", start_marker)
    meta_event.add("dtend", end_marker)
    meta_event.add("transp", "TRANSPARENT")
    cal.add_component(meta_event)

    for e in processed + untouched:
        ev = Event()
        ev.add("summary", e["title"])
        ev.add("uid", e["uid"])
        ev.add("dtstart", e["start_dt"])
        ev.add("dtend", e["end_dt"])
        ev.add("dtstamp", updated_now)
        cal.add_component(ev)

    return cal.to_ical()

# ============================================================
# STREAMLIT UI
# ============================================================

st.set_page_config(page_title="HMU Shift Processor")
st.title("HMU Shift Processor")

# ============================================================
# RESTORE SECTION
# ============================================================

st.info(
    "Returning users: If you previously created a subscription feed, "
    "enter your Feed ID and Ownership Token below to restore saved settings."
)

st.markdown("### Restore Existing Feed Settings")

restore_feed = st.text_input("Feed ID", key="restore_feed_id")
restore_token = st.text_input("Ownership Token", type="password", key="restore_token")

st.caption(
    "*Ownership token format: First initial + Middle initial + Last initial "
    "+ Birth month (MM). Example: JMS01*"
)

st.caption(
    "*Forgot your Feed ID? In your calendar app, open subscribed calendar settings. "
    "Your Feed ID appears in the URL as .../feeds/yourFeedID.ics*"
)

if st.button("Restore Saved Settings"):
    try:
        bucket = st.secrets["S3_BUCKET"]
        key = f"feeds/{restore_feed}.ics"
        response = s3_client().head_object(Bucket=bucket, Key=key)
        metadata = response.get("Metadata", {})

        stored_token = metadata.get("owner-token", "")

        if restore_token.strip().upper() != stored_token.upper():
            st.error("Invalid ownership token.")
        else:
            st.session_state["source_url"] = metadata.get("source-url", "")
            st.session_state["role"] = metadata.get("role", "MD")
            st.session_state["window_end"] = date.fromisoformat(
                metadata.get("window-end")
            ) if metadata.get("window-end") else None

            # 🔥 AUTO-FILL PUBLISH FIELDS
            st.session_state["publish_feed_id"] = restore_feed
            st.session_state["publish_token"] = restore_token

            st.success("Settings restored. Review and click Publish to update.")

    except Exception:
        st.error("Feed ID not found.")

# ============================================================
# ROLE + DATE
# ============================================================

role = st.selectbox(
    "Role",
    ["MD", "APP"],
    index=0 if st.session_state.get("role","MD") == "MD" else 1
)

today = date.today()
window_start = today
window_end = st.date_input(
    "End Date",
    value=st.session_state.get(
        "window_end",
        date(today.year + 1, 6, 30)
    )
)

# ============================================================
# INPUT SOURCE
# ============================================================

input_mode = st.radio("Input Method", ["Subscription URL","Upload ICS"])

cal_text = None
url = None

if input_mode == "Subscription URL":
    url = st.text_input(
        "ICS Subscription URL",
        value=st.session_state.get("source_url",""),
        key="source_url_input"
    )
    if url:
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            cal_text = r.text
        except Exception as e:
            st.error(f"Unable to fetch calendar: {e}")
else:
    upload = st.file_uploader("Upload ICS file", type=["ics"])
    if upload:
        cal_text = upload.read().decode("utf-8")

# ============================================================
# PROCESS EVENTS
# ============================================================

processed = []
untouched = []

if cal_text:
    cal = Calendar.from_ical(cal_text)

    for comp in cal.walk("VEVENT"):
        summary = str(comp.get("summary",""))
        start = comp.decoded("dtstart")
        end = comp.decoded("dtend")

        if isinstance(start, datetime):
            start = start.astimezone(APP_TZ)
        if isinstance(end, datetime):
            end = end.astimezone(APP_TZ)

        shift_type = calc_shift_type(summary, start.date(), role)

        if shift_type:
            st_time, en_time = shift_times(shift_type)
            new_start = datetime.combine(start.date(), st_time).replace(tzinfo=APP_TZ)
            new_end = datetime.combine(start.date(), en_time).replace(tzinfo=APP_TZ)

            processed.append({
                "title": summary,
                "start_dt": new_start,
                "end_dt": new_end,
                "uid": comp.get("uid","")
            })
        else:
            untouched.append({
                "title": summary,
                "start_dt": start,
                "end_dt": end,
                "uid": comp.get("uid","")
            })

    st.success(f"{len(processed)} processed | {len(untouched)} untouched")

# ============================================================
# DOWNLOAD
# ============================================================

if processed or untouched:
    output_ics = build_output_ics(processed, untouched, "preview")

    st.download_button(
        "Download Output ICS",
        output_ics,
        file_name="hmu-shifts-output.ics",
        mime="text/calendar"
    )

# ============================================================
# PUBLISH SECTION
# ============================================================

st.markdown("## Publish Subscription Feed")

feed_id = st.text_input(
    "Feed ID",
    key="publish_feed_id"
)

token = st.text_input(
    "Ownership Token",
    key="publish_token"
)

if st.button("Publish Feed"):
    if not feed_id or not token:
        st.error("Feed ID and Ownership Token required.")
    elif not (processed or untouched):
        st.error("Nothing to publish.")
    else:
        bucket = st.secrets["S3_BUCKET"]
        region = st.secrets["AWS_REGION"]
        key = f"feeds/{feed_id}.ics"
        sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

        final_ics = build_output_ics(processed, untouched, feed_id)

        s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=final_ics,
            ContentType="text/calendar; charset=utf-8",
            CacheControl="no-cache, max-age=0",
            Metadata={
                "owner-token": token.strip().upper(),
                "source-url": url or "",
                "role": role,
                "window-end": str(window_end),
            }
        )

        st.success("Feed successfully published.")

        st.info(
            "This is a reminder of your existing subscription URL. "
            "If you are already subscribed in your calendar app, "
            "no further action is needed — it will update automatically."
        )

        st.code(sub_url)

        st.caption(
            "To confirm the last calendar update, search your calendar for "
            "'HMU Shifts — Last Updated'."
        )

        st.warning(
            "Important: The subscription URL does NOT automatically update in the background. "
            "If your source calendar changes, reopen this app and republish."
        )
