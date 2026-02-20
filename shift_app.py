import streamlit as st
import pandas as pd
import re
from datetime import datetime, date, timedelta

# ============================================================
# CONFIG
# ============================================================
ANCHOR_DATE = date(2025, 7, 1)
ANCHOR_DAY_NUM = 2

SPECIAL_BLUE_PERIODS = [
    (date(2025,7,7), date(2025,8,3)),
    (date(2025,9,29), date(2025,10,26)),
    (date(2026,1,5), date(2026,2,1)),
    (date(2026,4,6), date(2026,5,3)),
]

FIXED_MD_COLORS = {"yellow","purple","blue","bronze","green","orange"}

# ============================================================
# HELPERS
# ============================================================
def get_day_number(d):
    delta = (d - ANCHOR_DATE).days
    return (delta + ANCHOR_DAY_NUM - 1) % 4 + 1

def is_special_blue(d):
    return any(start <= d <= end for start,end in SPECIAL_BLUE_PERIODS)

def normalize_title(s):
    if not isinstance(s,str):
        return ""
    s = re.sub(r"[^A-Za-z0-9\s\-]", "", s)  # remove symbols like ($)
    return re.sub(r"\s+"," ",s).strip()

def parse_date(x):
    dt = pd.to_datetime(str(x), errors="coerce")
    return None if pd.isna(dt) else dt.date()

# ============================================================
# ROTATIONS
# ============================================================
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

# ============================================================
# SHIFT LOGIC
# ============================================================
def calc_shift(title, d, role):
    t = normalize_title(title).lower()

    # Gray collaborator correction
    if role=="MD":
        t = t.replace("gray 1 collaborator","gray 1 md")
        t = t.replace("gray 2 collaborator","gray 2 md")

    day = get_day_number(d)

    # MD fixed override
    for c in FIXED_MD_COLORS:
        if t.startswith(c) and role=="MD":
            return "Fixed"

    # GOLD
    if t.startswith("gold"):
        m=re.search(r"(\d+)",t)
        if not m: return None
        n=int(m.group(1))
        if n==1: return "Early"
        if n>=6: return "Middle"
        if n in [3,5]: return "Early" if day in [1,3] else "Middle"
        if n in [2,4]: return "Middle" if day in [1,3] else "Early"

    # SILVER
    if t.startswith("silver"):
        m=re.search(r"(\d+)",t)
        n=int(m.group(1)) if m else 1
        return "Early" if n==1 else "Middle"

    # BLUE
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

    # YELLOW PURPLE BRONZE ORANGE
    for c in ["yellow","purple","bronze","orange"]:
        if t.startswith(c):
            suffix=t.split()[1] if len(t.split())>1 else None
            if suffix in YPBB:
                return YPBB[suffix][day]

    # GREEN
    if t.startswith("green"):
        suffix=t.split()[1] if len(t.split())>1 else None
        if suffix in GREEN:
            return GREEN[suffix][day]

    # GRAY
    if "gray 1 md" in t or "mist transplant" in t:
        return "Early" if day in [1,3] else "Middle"
    if "gray 2 md" in t or "gray 3 app" in t:
        return "Middle" if day in [1,3] else "Early"

    # MIST SCU
    if t.startswith("mist scu"):
        return MIST_SCU[day]

    return None

def start_end(shift):
    if shift=="Early":
        return "06:45","17:00"
    if shift=="Middle":
        return "08:00","17:00"
    if shift=="Late":
        return "08:00","18:45"
    if shift=="Fixed":
        return "08:00","17:00"
    return None,None

# ============================================================
# STREAMLIT UI
# ============================================================
st.title("HMU Shift Processor")

role = st.selectbox("Role",["APP","MD"])

file = st.file_uploader("Upload CSV",type=["csv"])

if file:
    df=pd.read_csv(file)

    name_col=st.selectbox("Title column",df.columns)
    date_col=st.selectbox("Date column",df.columns)

    df=df.rename(columns={name_col:"title",date_col:"date"})
    df["date_parsed"]=df["date"].apply(parse_date)

    results=[]
    rejected=[]

    for _,r in df.iterrows():
        if not r["date_parsed"]:
            rejected.append((r["title"],"Invalid date"))
            continue
        shift=calc_shift(r["title"],r["date_parsed"],role)
        if not shift:
            rejected.append((r["title"],"Unrecognized format"))
            continue
        start,end=start_end(shift)
        results.append({
            "Event":r["title"],
            "Shift":shift,
            "Start":start,
            "End":end,
            "Date":r["date_parsed"]
        })

    out=pd.DataFrame(results)
    st.dataframe(out)

    st.download_button("Download CSV",out.to_csv(index=False),"processed.csv")

    # ICS
    ics="BEGIN:VCALENDAR\nVERSION:2.0\n"
    for _,r in out.iterrows():
        start_dt=datetime.combine(r["Date"],datetime.strptime(r["Start"],"%H:%M").time())
        end_dt=datetime.combine(r["Date"],datetime.strptime(r["End"],"%H:%M").time())
        ics+=f"""BEGIN:VEVENT
SUMMARY:{r["Event"]} ({r["Shift"]})
DTSTART:{start_dt.strftime("%Y%m%dT%H%M%S")}
DTEND:{end_dt.strftime("%Y%m%dT%H%M%S")}
END:VEVENT
"""
    ics+="END:VCALENDAR"

    st.download_button("Download Outlook ICS",ics,"schedule.ics")

    if rejected:
        st.subheader("Rejected")
        st.write(pd.DataFrame(rejected,columns=["Event","Reason"]))
