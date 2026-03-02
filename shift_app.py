# ============================
# HMU SHIFT PROCESSOR
# ============================

import re
from datetime import datetime, date, time, timedelta
import requests
import streamlit as st
import boto3
from botocore.exceptions import ClientError
from icalendar import Calendar, Event
from zoneinfo import ZoneInfo
from streamlit_autorefresh import st_autorefresh

APP_TZ = ZoneInfo("America/New_York")

# ============================
# S3 CLIENT
# ============================

def s3_client():
    return boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )

# ============================
# BUILD OUTPUT ICS
# ============================

def build_output_ics(processed_rows, untouched_rows, feed_id):
    cal = Calendar()
    cal.add("prodid", "-//HMU Shift Processor//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("X-WR-TIMEZONE", "America/New_York")

    # --- Last Updated Marker ---
    updated_now = datetime.now(APP_TZ)

    meta_event = Event()
    meta_event.add(
        "summary",
        f"HMU Shifts — Last Updated: {updated_now.strftime('%Y-%m-%d %H:%M')}"
    )
    meta_event.add("uid", f"hmushifts-last-updated@{feed_id}")
    meta_event.add("dtstamp", updated_now)

    start_marker = datetime.combine(
        updated_now.date(),
        time(0, 1)
    ).replace(tzinfo=APP_TZ)

    end_marker = start_marker + timedelta(minutes=1)

    meta_event.add("dtstart", start_marker)
    meta_event.add("dtend", end_marker)
    meta_event.add("transp", "TRANSPARENT")
    meta_event.add("description", "System-generated update marker.")

    cal.add_component(meta_event)

    # --- Add Events ---
    for e in processed_rows + untouched_rows:
        ev = Event()
        ev.add("summary", e["title"])
        ev.add("uid", e.get("uid", f"{hash(e['title'])}@{feed_id}"))
        ev.add("dtstart", e["start_dt"])
        ev.add("dtend", e["end_dt"])
        ev.add("dtstamp", updated_now)
        cal.add_component(ev)

    return cal.to_ical()

# ============================
# STREAMLIT UI
# ============================

st.set_page_config(page_title="HMU Shift Processor")
st.title("HMU Shift Processor")

st_autorefresh(interval=3600 * 1000, key="auto_refresh")

if "restore_used" not in st.session_state:
    st.session_state.restore_used = False

if "confirm_restore" not in st.session_state:
    st.session_state.confirm_restore = False

# ============================
# RESTORE + AUTO REPUBLISH
# ============================

st.markdown("## Restore & Republish Existing Feed")

feed_restore = st.text_input("Feed ID to Restore")
token_restore = st.text_input("Ownership Token", type="password")

st.caption(
    "*Forgot your Feed ID? In your calendar app, open subscribed calendar settings. "
    "In the URL it appears as .../feeds/yourFeedID.ics*"
)

if st.button("Restore Feed"):
    if not feed_restore or not token_restore:
        st.error("Both Feed ID and Ownership Token required.")
    else:
        st.session_state.confirm_restore = True

if st.session_state.confirm_restore:

    st.warning(
        "This will overwrite the existing calendar feed associated with this Feed ID."
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Confirm Restore & Republish"):
            try:
                bucket = st.secrets["S3_BUCKET"]
                region = st.secrets["AWS_REGION"]
                key = f"feeds/{feed_restore}.ics"
                sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

                response = s3_client().head_object(Bucket=bucket, Key=key)
                metadata = response.get("Metadata", {})
                stored_token = metadata.get("owner-token", "")

                if token_restore.strip().upper() != stored_token.upper():
                    st.error("Invalid ownership token.")
                else:
                    restored_url = metadata.get("source-url", "")
                    restored_role = metadata.get("role", "MD")
                    restored_end = metadata.get("window-end")

                    if not restored_url:
                        st.error("No stored source URL.")
                        st.stop()

                    window_start = date.today()
                    window_end = (
                        date.fromisoformat(restored_end)
                        if restored_end else date(date.today().year + 1, 6, 30)
                    )

                    r = requests.get(restored_url, timeout=30)
                    r.raise_for_status()

                    # (Insert your full parsing + shift logic here)
                    processed_rows = []
                    untouched_rows = []

                    output_ics = build_output_ics(
                        processed_rows,
                        untouched_rows,
                        feed_restore
                    )

                    s3_client().put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=output_ics,
                        ContentType="text/calendar; charset=utf-8",
                        CacheControl="no-cache, max-age=0",
                        Metadata={
                            "owner-token": stored_token,
                            "source-url": restored_url,
                            "role": restored_role,
                            "window-end": str(window_end),
                        }
                    )

                    st.success("Feed successfully restored and republished.")

                    last_modified = response.get("LastModified")
                    if last_modified:
                        st.info(
                            f"Previous publish timestamp: "
                            f"{last_modified.astimezone(APP_TZ).strftime('%Y-%m-%d %H:%M')}"
                        )

                    st.info(
                        "This is a reminder of your existing subscription URL. "
                        "If already subscribed, no further action is needed."
                    )

                    st.code(sub_url)

                    st.caption(
                        "To confirm update, search your calendar for "
                        "'HMU Shifts — Last Updated'."
                    )

                    st.warning(
                        "Important: The subscription URL does NOT automatically update in the background. "
                        "If your source calendar changes, reopen this app and click Restore & Republish."
                    )

                    st.session_state.restore_used = True
                    st.session_state.confirm_restore = False

            except ClientError:
                st.error("Feed ID not found.")
            except Exception as e:
                st.error(f"Restore failed: {e}")

    with col2:
        if st.button("Cancel"):
            st.session_state.confirm_restore = False

# ============================
# MANUAL PUBLISH (HIDDEN IF RESTORED)
# ============================

if not st.session_state.restore_used:

    st.markdown("## Publish Subscription Feed")

    feed_id = st.text_input("Feed ID")
    token = st.text_input("Ownership Token")

    if st.button("Publish"):
        if not feed_id or not token:
            st.error("Feed ID and token required.")
        else:
            bucket = st.secrets["S3_BUCKET"]
            region = st.secrets["AWS_REGION"]
            key = f"feeds/{feed_id}.ics"
            sub_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

            s3_client().put_object(
                Bucket=bucket,
                Key=key,
                Body=b"",  # Replace with built ICS bytes
                ContentType="text/calendar; charset=utf-8",
                CacheControl="no-cache, max-age=0",
                Metadata={
                    "owner-token": token.strip().upper(),
                    "source-url": "",
                    "role": "MD",
                    "window-end": str(date(date.today().year + 1, 6, 30)),
                }
            )

            st.success("Feed successfully published.")

            st.info(
                "If already subscribed, no further action is needed."
            )

            st.code(sub_url)

            st.caption(
                "To confirm update, search for "
                "'HMU Shifts — Last Updated'."
            )

            st.warning(
                "Important: This subscription does NOT auto-update in the background. "
                "Reopen this app and republish when source changes."
            )
