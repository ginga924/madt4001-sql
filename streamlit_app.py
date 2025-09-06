# streamlit.py — elegant edition (no hints)
import os
import re
import io
import glob
import sqlite3
from typing import List, Tuple, Dict

import pandas as pd
import streamlit as st

# --------------------------
# App Config
# --------------------------
st.set_page_config(
    page_title="Student SQL Lab",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------
# Minimalist Styling (CSS)
# --------------------------
st.markdown(
    """
    <style>
    /* Background gradient */
    .stApp {
        background: radial-gradient(1200px 800px at 10% 10%, rgba(180, 220, 255, .18), transparent 40%),
                    radial-gradient(1000px 600px at 90% 20%, rgba(200, 255, 210, .16), transparent 40%),
                    linear-gradient(180deg, #0b1220 0%, #0f1424 50%, #101626 100%);
        color: #EAF2FF;
    }
    /* Headings */
    h1, h2, h3 { letter-spacing: .3px; }
    /* Glass cards */
    .glass {
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 8px 28px rgba(0,0,0,.25);
        backdrop-filter: blur(10px);
        border-radius: 18px;
        padding: 18px 20px;
    }
    /* Dataframe container */
    [data-testid="stDataFrame"] {
        background: rgba(20,24,40,.55);
        border-radius: 14px;
        border: 1px solid rgba(255,255,255,0.06);
    }
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.02));
        border-right: 1px solid rgba(255,255,255,.08);
    }
    /* Buttons */
    .stButton>button {
        border-radius: 14px;
        padding: 10px 16px;
        border: 1px solid rgba(255,255,255,0.14);
        background: rgba(255,255,255,0.08);
        color: #EAF2FF;
    }
    .stButton>button:hover { background: rgba(255,255,255,0.14); }
    /* Text area (SQL) */
    textarea {
        background: rgba(255,255,255,0.06) !important;
        color: #DDE8FF !important;
        border-radius: 14px !important;
        border: 1px solid rgba(255,255,255,0.10) !important;
    }
    /* Pills */
    .pill {
        display:inline-block; margin: 4px 6px 0 0; padding: 6px 10px;
        border-radius: 999px; border: 1px solid rgba(255,255,255,.18);
        background: rgba(255,255,255,.08); font-size: 12px;
    }
    /* Subtle caption */
    .soft { opacity:.8; font-size: 0.88rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --------------------------
# Data locations
# --------------------------
DATA_DIRS = ["data", "/mnt/data"]

# Known names → clean table
TABLE_NAME_MAP = {
    # CT (transactions)
    "399394_AllTeam_MonsoonSIM_CT_cashOnHand.csv": "cash_on_hand",
    "399394_AllTeam_MonsoonSIM_CT_financials.csv": "financials",
    "399394_AllTeam_MonsoonSIM_CT_inventory.csv": "inventory",
    "399394_AllTeam_MonsoonSIM_CT_marketing.csv": "marketing",
    "399394_AllTeam_MonsoonSIM_CT_operating_expense.csv": "operating_expense",
    "399394_AllTeam_MonsoonSIM_CT_procurement.csv": "procurement",
    "399394_AllTeam_MonsoonSIM_CT_sales.csv": "sales",
    # COM (masters)
    "399394_Common_MonsoonSIM_COM_client.csv": "dim_client",
    "399394_Common_MonsoonSIM_COM_day.csv": "dim_day",
    "399394_Common_MonsoonSIM_COM_dept.csv": "dim_department",
    "399394_Common_MonsoonSIM_COM_forecast_B2B.csv": "dim_forecast_b2b",
    "399394_Common_MonsoonSIM_COM_forecast_retail.csv": "dim_forecast_retail",
    "399394_Common_MonsoonSIM_COM_location.csv": "dim_location",
    "399394_Common_MonsoonSIM_COM_product.csv": "dim_product",
    "399394_Common_MonsoonSIM_COM_team.csv": "dim_team",
    "399394_Common_MonsoonSIM_COM_vendor.csv": "dim_vendor",
}

def classify_by_filename(fname: str) -> str:
    if "_CT_" in fname: return "transaction"
    if "_COM_" in fname: return "master"
    return "unknown"

# --------------------------
# Helpers
# --------------------------
def sanitize_name(path_or_name: str) -> str:
    base = os.path.splitext(os.path.basename(path_or_name))[0]
    base = re.sub(r"[^A-Za-z0-9_]", "_", base)
    if re.match(r"^\d", base): base = "_" + base
    return base.lower()

@st.cache_data(show_spinner=False)
def list_csv_files() -> List[str]:
    paths = []
    for d in DATA_DIRS:
        if os.path.isdir(d):
            paths.extend(glob.glob(os.path.join(d, "*.csv")))
    seen, out = set(), []
    for p in sorted(paths):
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def read_csv_any(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8-sig")

def _ratio_not_na(series: pd.Series) -> float:
    total = len(series);  return 0.0 if total == 0 else series.notna().sum() / total

def smart_cast_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
            continue
        num_try = pd.to_numeric(s, errors="coerce")
        if _ratio_not_na(num_try) >= 0.9:
            try:
                if num_try.dropna().apply(lambda x: float(x).is_integer()).all():
                    out[col] = num_try.astype("Int64")
                else:
                    out[col] = num_try.astype("float64")
                continue
            except Exception:
                pass
        dt_try = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
        if _ratio_not_na(dt_try) >= 0.9:
            out[col] = dt_try;  continue
        if s.dtype == object:
            low = s.astype(str).str.strip().str.lower()
            bool_map = {"true": True,"t": True,"yes": True,"y": True,"1": True,
                        "false": False,"f": False,"no": False,"n": False,"0": False}
            mapped = low.map(bool_map).where(~low.isna(), other=pd.NA)
            if _ratio_not_na(mapped) >= 0.9:
                out[col] = mapped.astype("boolean");  continue
    return out

def build_schema_df(conn: sqlite3.Connection, tables: List[str]) -> pd.DataFrame:
    rows, cur = [], conn.cursor()
    for t in tables:
        try:
            cur.execute(f"PRAGMA table_info({t});")
            for cid, name, coltype, notnull, dflt, pk in cur.fetchall():
                rows.append({"table_name": t, "column_name": name, "data_type": coltype or "", "pk": pk})
        except sqlite3.Error:
            pass
    return pd.DataFrame(rows).sort_values(["table_name", "pk", "column_name"]).reset_index(drop=True)

def load_csvs_into_sqlite(csv_paths: List[str]) -> Tuple[sqlite3.Connection, List[str], List[str], pd.DataFrame, Dict[str, str]]:
    conn = sqlite3.connect(":memory:")
    txn_loaded, master_loaded, table_category_map = [], [], {}
    for f in csv_paths:
        fname = os.path.basename(f)
        tbl = TABLE_NAME_MAP.get(fname, sanitize_name(fname))
        df = smart_cast_df(read_csv_any(f))
        df.to_sql(tbl, conn, if_exists="replace", index=False)

        cat = classify_by_filename(fname)
        table_category_map[tbl] = cat
        (txn_loaded if cat == "transaction" else master_loaded).append(tbl) if cat in {"transaction","master"} else master_loaded.append(tbl)
    schema_df = build_schema_df(conn, txn_loaded + master_loaded)
    return conn, txn_loaded, master_loaded, schema_df, table_category_map

def is_select_only(sql: str) -> bool:
    forbidden = r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|ATTACH|DETACH|VACUUM|PRAGMA)\b"
    return bool(re.match(r"^\s*(WITH|SELECT)\b", sql.strip(), re.IGNORECASE)) and not re.search(forbidden, sql, re.IGNORECASE)

def enforce_limit(sql: str, max_rows: int = 5000) -> str:
    return sql if re.search(r"\bLIMIT\s+\d+\b", sql, re.IGNORECASE) else f"SELECT * FROM ({sql}) AS t LIMIT {int(max_rows)}"

# --------------------------
# Load
# --------------------------
csv_files = list_csv_files()
if csv_files:
    conn, txn_tables, master_tables, schema_df, table_category_map = load_csvs_into_sqlite(csv_files)
else:
    conn, txn_tables, master_tables, schema_df, table_category_map = None, [], [], pd.DataFrame(), {}

# --------------------------
# Sidebar
# --------------------------
st.sidebar.markdown("### ✨ Student SQL Lab")
page = st.sidebar.segmented_control("View", ["Schema", "Query"], default="Schema")
st.sidebar.divider()

# Compact table badges
if conn:
    st.sidebar.markdown(f'<span class="pill">Masters: {len(master_tables)}</span>', unsafe_allow_html=True)
    st.sidebar.markdown(f'<span class="pill">Transactions: {len(txn_tables)}</span>', unsafe_allow_html=True)
    st.sidebar.divider()
    # Quick filter
    filter_text = st.sidebar.text_input("Filter tables", value="")
else:
    filter_text = ""

# --------------------------
# Schema Page
# --------------------------
def render_table_block(title: str, tables: List[str], schema_df: pd.DataFrame, conn: sqlite3.Connection):
    st.markdown(f"### {title}")
    if filter_text:
        tables = [t for t in tables if filter_text.lower() in t.lower()]
    cols = st.columns(3) if len(tables) >= 3 else st.columns(max(1, len(tables)))
    for i, t in enumerate(tables):
        with cols[i % len(cols)].container():
            with st.container():
                st.markdown(f'<div class="glass"><h4 style="margin-top:0">{t}</h4>', unsafe_allow_html=True)
                try:
                    prev = pd.read_sql_query(f"SELECT * FROM {t} LIMIT 5;", conn)
                    st.dataframe(prev, use_container_width=True, height=220)
                except Exception as e:
                    st.warning(str(e))
                if not schema_df.empty:
                    cols_df = schema_df[schema_df["table_name"] == t][["column_name","data_type","pk"]]
                    st.dataframe(cols_df.reset_index(drop=True), use_container_width=True, height=220)
                st.markdown("</div>", unsafe_allow_html=True)

if page == "Schema":
    st.markdown("<h1>📚 Schema</h1>", unsafe_allow_html=True)
    if not conn:
        st.error("No CSVs found.")
    else:
        if master_tables:
            render_table_block("🔷 Masters", master_tables, schema_df, conn)
        if txn_tables:
            st.markdown("---")
            render_table_block("🧾 Transactions", txn_tables, schema_df, conn)

# --------------------------
# Query Page
# --------------------------
elif page == "Query":
    st.markdown("<h1>📝 Query</h1>", unsafe_allow_html=True)

    # Result first
    if "last_df" in st.session_state and st.session_state["last_df"] is not None:
        df = st.session_state["last_df"]
        st.markdown('<div class="glass">', unsafe_allow_html=True)
        st.dataframe(df, use_container_width=True, height=420)
        r, c = df.shape
        st.markdown(f'<div class="soft">{r:,} rows × {c:,} columns</div>', unsafe_allow_html=True)
        buf = io.StringIO();  df.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button("⬇ Download CSV", data=buf.getvalue(), file_name="query_result.csv", mime="text/csv")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="glass soft">No results yet.</div>', unsafe_allow_html=True)

    # Editor
    example_sql = "SELECT 1 AS hello;"
    available_tables = (txn_tables or []) + (master_tables or [])
    if available_tables:
        example_sql = f"SELECT * FROM {available_tables[0]} LIMIT 100;"

    sql = st.text_area("SQL", value=example_sql, height=180, key="sql_box")

    c1, c2, c3 = st.columns([1,1,6])
    with c1:
        run = st.button("▶ Run", type="primary")
    with c2:
        clear = st.button("🧹 Clear")

    if clear:
        st.session_state["last_df"] = None
        st.rerun()

    if run:
        if not conn:
            st.error("No database loaded.")
        else:
            sql_clean = sql.strip().rstrip(";").strip()
            if not is_select_only(sql_clean):
                st.error("Only SELECT / WITH.")
            else:
                safe_sql = enforce_limit(sql_clean, 5000)
                try:
                    df = pd.read_sql_query(safe_sql, conn)
                    st.session_state["last_df"] = df
                    st.rerun()
                except Exception as e:
                    st.error(f"Execution error: {e}")
