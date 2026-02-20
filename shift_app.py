import streamlit as st
import pandas as pd
import re
import requests
from datetime import datetime, date, timedelta
from dateutil.rrule import rrulestr
from io import StringIO

# =====================================================
# CONFIG
# =====================================================
ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2

SPECIAL_BLUE_PERIODS = [
    (date(2025,7,7), date(2025,8,3)),
    (date(2025,9,29), date(2025,10,26)),
    (date(2026,1,5), date(2026,2,1)),
    (date(2026,4,6), date(2026,5,3)),
]

FIXED_MD_COLORS = {"yellow","purple","blue","bronze","green","orange"}

# =====================================================
# ROTATIONS
# =====================================================
YPBB = {
 "1-1":{1:'Early',2:'Middle',3:'Late',4:'Middle'},
 "1-2":{1:'Middle',2:'Late',3:'Middle',4:'Early'},
 "2-1":{1:'Late',2:'Middle',3:'Early',4:'Middle'},
 "2-2":{1:'Middle',2:'Early',3:'Middle',4:'Late'},
 "3":{1:'Middle',2:'Early',3:'Middle',4:'Late'},
}

GREEN = {
 "1":{1:'Early',2:'Middle',3:'Late',4:'Middle'},
 "2":{1:'Middle',2:'Late',3:'Middle',4:'Early'},
 "3":{1:'Late',2:'Middle',3:'Early',4:'Middle'},
}

MIST_SCU = {1:'Middle',2:'Early',3:'Middle',4:'Late'}

# =====================================================
# HELPERS
# =====================================================
def get_day_number(d):
    delta = (d - ANCHOR_DATE).days
    return (delta + ANCHOR_DAY_NUM - 1) % 4 + 1

def is_special_blue(d):
    return any(start <= d <= end for start,end in SPECIAL_BLUE_PERIODS)

def normalize_title(s):
    if not isinstance(s,str):
        return ""
    s = re.sub(r"[^A-Za-z0-9\s\-]", "", s)
    return re.sub(r"\s+"," ",s).strip()

def parse_date(x):
    dt = pd.to_datetime(str(x), errors="coerce")
    return None if pd.isna(dt) else dt.date()

# =====================================================
# SHIFT LOGIC
# =====================================================
def calc_shift(title, d, role):
    t = normalize_title(title).lower()

    if role=="MD":
        t = t.replace("gray 1 collaborator","gray 1 md")
        t = t.replace("gray 2 collaborator","gray 2 md")

    day = get_day_number(d)

    for c in FIXED_MD_COLORS:
        if t.startswith(c) and role=="MD":
            return "Fixed"

    if t.startswith("gold"):
        m=re.search(r"(\d+)",t)
        if not m: return None
        n=int(m.group(1))
        if n==1: return "Early"
        if n>=6: return "Middle"
        if n in [3,5]: return "Early" if day in [1,3] else "Middle"
        if n in [2,4]: return "Middle" if day in [1,3] else "Early"

    if t.startswith("silver"):
        m=re.search(r"(\d+)",t)
        n=int(m.group(1)) if m else 1
        return "Early" if n==1 else "Middle"

    if t.startswith("blue"):
        parts=t.split()
        if len(parts)<2: return None
        suffix=parts[1]
        if is_special_blue(d):
            if suffix=="1":
                return "Early" if day in [1,3] else "Middle"
            if suffix in ["3-1","3-2"]:
                return "Middle" if day in [1,3] else "Early"
        else:
            if suffix in YPBB:
                return YPBB[suffix][day]

    for c in ["yellow","purple","bronze","orange"]:
        if t.startswith(c):
            suffix=t.split()[1] if len(t.split())>1 else None
            if suffix in YPBB:
                return YPBB[suffix][day]

    if t.startswith("green"):
        suffix=t.split()[1] if len(t.split())>1 else None
        if suffix in GREEN:
            return GREEN[suffix][day]

    if "gray 1 md" in t or "mist transplant" in t:
        return "Early" if day in [1,3] else "Middle"
    if "gray 2 md" in t or "gray 3 app" in t:
        return "Middle" if day in [1,3] else "Early"

    if t.startswith("mist scu"):
        return MIST_SCU[day]

    return None

def start_end(shift):
    if shift=="Early": return "06:45","17:00"
    if shift=="Middle": return "08:00","17:00"
    if shift=="Late": return "08:00","18:45"
    if shift=="Fixed": return "08:00","17:00"
    return None,None

# =====================================================
# ICS FETCH + CACHE
# =====================================================
@st.cache_data(ttl=600)
def fetch_ics(url):
    resp=requests.get(url)
    resp.raise_for_status()
    return resp.text, datetime.now()

def parse_ics_events(text):
    events=[]
    blocks=text.split("BEGIN:VEVENT")
    for b in blocks[1:]:
        summary=re.search(r"SUMMARY:(.*)",b)
        dtstart=re.search(r"DTSTART.*:(\d{8})",b)
        if summary and dtstart:
            d=datetime.strptime(dtstart.group(1),"%Y%m%d").date()
            events.append((summary.group(1),d))
    return events

# =====================================================
# UI
# =====================================================
st.title("HMU Shift Processor")

role=st.selectbox("Role",["APP","MD"])

mode=st.radio("Input source",["Upload CSV","Calendar URL"])

events=[]

if mode=="Upload CSV":
    file=st.file_uploader("Upload CSV",type=["csv"])
    if file:
        df=pd.read_csv(file)
        name_col=st.selectbox("Title column",df.columns)
        date_col=st.selectbox("Date column",df.columns)
        for _,r in df.iterrows():
            d=parse_date(r[date_col])
            if d:
                events.append((r[name_col],d))

if mode=="Calendar URL":
    url=st.text_input("Paste ICS URL")
    if url:
        try:
            text,sync_time=fetch_ics(url)
            st.caption(f"Last sync: {sync_time}")
            events=parse_ics_events(text)
        except Exception as e:
            st.error(f"Failed to fetch calendar: {e}")

if events:
    df_events=pd.DataFrame(events,columns=["Title","Date"])

    st.subheader("Preview")
    st.dataframe(df_events.head(50))

    start_filter=st.date_input("Start date filter",None)
    end_filter=st.date_input("End date filter",None)

    if start_filter:
        df_events=df_events[df_events["Date"]>=start_filter]
    if end_filter:
        df_events=df_events[df_events["Date"]<=end_filter]

    results=[]
    rejected=[]

    for _,r in df_events.iterrows():
        shift=calc_shift(r["Title"],r["Date"],role)
        if not shift:
            rejected.append((r["Title"],"Unrecognized"))
            continue
        start,end=start_end(shift)
        results.append({
            "Event":r["Title"],
            "Shift":shift,
            "Start":start,
            "End":end,
            "Date":r["Date"]
        })

    out=pd.DataFrame(results)

    st.subheader("Processed")
    st.dataframe(out)

    st.download_button("Download CSV",out.to_csv(index=False),"processed.csv")

    ics="BEGIN:VCALENDAR\nVERSION:2.0\n"
    for _,r in out.iterrows():
        s=datetime.combine(r["Date"],datetime.strptime(r["Start"],"%H:%M").time())
        e=datetime.combine(r["Date"],datetime.strptime(r["End"],"%H:%M").time())
        ics+=f"""BEGIN:VEVENT
SUMMARY:{r["Event"]} ({r["Shift"]})
DTSTART:{s.strftime("%Y%m%dT%H%M%S")}
DTEND:{e.strftime("%Y%m%dT%H%M%S")}
END:VEVENT
"""
    ics+="END:VCALENDAR"

    st.download_button("Download Outlook ICS",ics,"schedule.ics")

    if rejected:
        st.subheader("Rejected")
        st.write(pd.DataFrame(rejected,columns=["Event","Reason"]))
