import json
import os
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Tender Monitor",
    page_icon="📋",
    layout="wide",
)

st.markdown("""
<style>
/* Hide download button on all tables */
button[title="Download as CSV"] { display: none !important; }
[data-testid="stElementToolbarButton"]:has(svg[data-testid="stIconDownload"]) { display: none !important; }
</style>
""", unsafe_allow_html=True)

SOURCE_LABEL = {
    "gem":                 "GeM",
    "cppp_global_central": "CPPP Central Global",
    "cppp_global_state":   "CPPP State Global",
    "cppp_highvalue":      "CPPP High Value",
    "cppp_state":          "CPPP State",
    "cppp_central":        "CPPP Central",
    "cppp_gem":            "GeM-CPPP",
    "igl":       "IGL",
    "oil_india": "OIL India",
    "eil":       "EIL",
    "mrpl":      "MRPL",
    "ongc":      "ONGC",
    "gail":      "GAIL",
    "hpcl":      "HPCL",
}
LABEL_SOURCE = {v: k for k, v in SOURCE_LABEL.items()}

# ── Supabase connection ───────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_KEY"))

client = get_client()

# ── Load filter options ───────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_distinct(column: str) -> list[str]:
    """Fetch all distinct non-empty values for a column."""
    all_vals = set()
    page_size = 1000
    offset = 0
    while True:
        res = client.table("tenders").select(column).range(offset, offset + page_size - 1).execute()
        for r in res.data:
            if r[column]:
                all_vals.add(r[column])
        if len(res.data) < page_size:
            break
        offset += page_size
    return sorted(all_vals)

def _parse_keyword(q: str):
    """Parse keyword into (mode, terms). mode = 'and' | 'or'."""
    q = q.strip()
    if " OR " in q.upper():
        terms = [t.strip() for t in q.upper().split(" OR ") if t.strip()]
        # Preserve original case
        orig_terms = [t.strip() for t in q.split(" OR ") if t.strip()]
        return "or", orig_terms
    if " AND " in q.upper():
        orig_terms = [t.strip() for t in q.split(" AND ") if t.strip()]
        return "and", orig_terms
    if "+" in q:
        orig_terms = [t.strip() for t in q.split("+") if t.strip()]
        return "and", orig_terms
    return "and", [q]

# ── DB stats ticker ───────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def get_db_stats():
    from datetime import date
    today = date.today().isoformat()
    total_res  = client.table("tenders").select("id", count="exact").execute()
    live_res   = client.table("tenders").select("id", count="exact").gte("end_date", today).execute()
    latest_res = client.table("tenders").select("last_seen").order("last_seen", desc=True).limit(1).execute()
    total   = total_res.count or 0
    live    = live_res.count or 0
    expired = total - live
    last_updated = latest_res.data[0]["last_seen"] if latest_res.data else "—"
    return total, live, expired, last_updated

@st.cache_data(ttl=3600)
def get_source_stats():
    from datetime import date
    today = date.today().isoformat()
    # Fetch all records with just the fields needed for aggregation
    PAGE = 1000
    all_data = []
    offset = 0
    while True:
        res = client.table("tenders").select("source, end_date, first_seen, last_seen").range(offset, offset + PAGE - 1).execute()
        all_data.extend(res.data)
        if len(res.data) < PAGE:
            break
        offset += PAGE
    df = pd.DataFrame(all_data)
    if df.empty:
        return pd.DataFrame()
    rows = []
    for src, grp in df.groupby("source"):
        total   = len(grp)
        live    = (grp["end_date"] >= today).sum()
        expired = total - live
        new     = (grp["first_seen"] == today).sum()
        last_run = grp["last_seen"].max()
        rows.append({
            "Source":           SOURCE_LABEL.get(src, src.upper()),
            "# Tenders":        total,
            "# Live":           live,
            "# Expired":        expired,
            "# New Today":      new,
            "Last Run":         last_run,
        })
    return pd.DataFrame(rows).sort_values("Source").reset_index(drop=True)

# ── UI ────────────────────────────────────────────────────────────────────────
st.title("📋 Tender Monitor")
_total, _live, _expired, _last_updated = get_db_stats()
st.info(f"📥 Total {_total:,} Tenders  |  {_live:,} Live  |  {_expired:,} Expired  |  Last updated: {_last_updated}")
st.caption("Supports keyword operators: `AND`, `OR`, `+`  — e.g. `pump AND diesel` or `generator OR turbine`")

# Row 1: keyword
keyword = st.text_input("Search by keyword", placeholder="e.g. pipeline AND pump  |  generator OR turbine")

# Row 2: ministry, department, source
col1, col2, col3 = st.columns(3)

with col1:
    ministries = st.multiselect(
        "Ministry",
        options=get_distinct("ministry"),
        placeholder="All ministries",
    )

with col2:
    departments = st.multiselect(
        "Department / Organisation",
        options=get_distinct("department"),
        placeholder="All departments",
    )

with col3:
    all_source_labels = [SOURCE_LABEL.get(s, s.upper()) for s in sorted(SOURCE_LABEL.keys())]
    selected_source_labels = st.multiselect(
        "Source",
        options=all_source_labels,
        placeholder="All sources",
    )
    selected_sources = [LABEL_SOURCE[l] for l in selected_source_labels]

# Row 3: dates + limit
col4, col5, col6 = st.columns([2, 2, 1])
with col4:
    date_from = st.date_input("End Date from", value=None)
with col5:
    date_to = st.date_input("End Date to", value=None)
with col6:
    limit = st.selectbox("Max results", [100, 250, 500, 1000, 5000, 10000], index=2)

show_expired = st.checkbox("Include expired tenders", value=False)

st.divider()

# ── Query ─────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Searching...")
def search(keyword, ministries, departments, sources, show_expired, date_from, date_to, limit):
    from datetime import date
    today = date.today().isoformat()

    def _base_query():
        q = client.table("tenders").select(
            "bid_number, title, ministry, department, quantity, start_date, end_date, url, source, first_seen"
        )
        if keyword:
            mode, terms = _parse_keyword(keyword)
            if mode == "or":
                q = q.or_(",".join(f"title.ilike.%{t}%" for t in terms))
            else:
                for term in terms:
                    q = q.ilike("title", f"%{term}%")
        if ministries:
            q = q.in_("ministry", list(ministries))
        if departments:
            q = q.in_("department", list(departments))
        if sources:
            q = q.in_("source", list(sources))
        if not show_expired:
            q = q.gte("end_date", today)
        if date_from:
            q = q.gte("end_date", str(date_from))
        if date_to:
            q = q.lte("end_date", str(date_to))
        return q.order("end_date", desc=False)

    # Supabase caps each request at 1000 rows — paginate to reach the full limit
    PAGE = 1000
    all_data = []
    offset = 0
    while len(all_data) < limit:
        fetch = min(PAGE, limit - len(all_data))
        res = _base_query().range(offset, offset + fetch - 1).execute()
        all_data.extend(res.data)
        if len(res.data) < fetch:
            break
        offset += fetch
    return all_data


results = search(
    keyword,
    tuple(ministries),
    tuple(departments),
    tuple(selected_sources),
    show_expired,
    date_from,
    date_to,
    limit,
)

# ── Results table ─────────────────────────────────────────────────────────────
if results:
    # Placeholder for the header line — filled in below once we know the selection
    header = st.empty()

    df = pd.DataFrame(results)
    df["source"] = df["source"].map(lambda s: SOURCE_LABEL.get(s, s.upper()))
    df = df.rename(columns={
        "bid_number":  "Bid Number",
        "title":       "Title",
        "ministry":    "Ministry",
        "department":  "Department",
        "quantity":    "Qty",
        "start_date":  "Start Date",
        "end_date":    "End Date",
        "url":         "Link",
        "source":      "Source",
        "first_seen":  "First Seen",
    })
    df = df[["Bid Number", "Title", "Ministry", "Department", "Qty",
             "Start Date", "End Date", "Source", "First Seen", "Link"]]

    df.insert(0, "WA", False)

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        disabled=[c for c in df.columns if c != "WA"],
        column_config={
            "WA": st.column_config.CheckboxColumn("WA", width="small", help="Check to include in WhatsApp message"),
            "Link": st.column_config.LinkColumn("Link", display_text="View"),
            "Title": st.column_config.TextColumn("Title", width="large"),
            "Ministry": st.column_config.TextColumn("Ministry", width="medium"),
        },
        height=600,
    )

    # ── Build WhatsApp message from selected rows ─────────────────────────────
    selected = edited[edited["WA"] == True]

    if not selected.empty:
        parts = []
        for _, row in selected.iterrows():
            parts.append(
                f"\U0001f4cb *Tender Alert*\n"
                f"\U0001f522 *Bid:* {row['Bid Number']}\n"
                f"\U0001f4cc *Title:* {row['Title']}\n"
                f"\U0001f3db *Ministry:* {row['Ministry']}\n"
                f"\U0001f3e2 *Dept:* {row['Department']}\n"
                + (f"\U0001f4e6 *Qty:* {row['Qty']}\n" if row['Qty'] else "")
                + f"\U0001f4c5 *Closes:* {row['End Date']}\n"
                f"\U0001f516 *Source:* {row['Source']}\n"
                f"\U0001f517 {row['Link']}"
            )
        wa_text = "\n\n".join(parts)
        n_sel   = len(selected)

        with header:
            st.markdown(f"**{len(results)} tender(s) found**, check rows to share on WhatsApp ({n_sel} selected)")

        st.caption("Message preview:")
        st.code(wa_text, language=None)

    else:
        with header:
            st.markdown(f"**{len(results)} tender(s) found**, check rows to share on WhatsApp")

else:
    st.info("No tenders found. Try adjusting your filters.")

st.divider()

# ── Source summary table ───────────────────────────────────────────────────────
st.subheader("Summary by Source")
src_df = get_source_stats()
if not src_df.empty:
    st.dataframe(src_df, use_container_width=True, hide_index=True)

st.caption("Data refreshes daily. Supabase syncs in real-time during each run.")
