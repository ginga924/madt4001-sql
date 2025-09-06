# streamlit.py — polished edition (Schema + Query, no hints)
import os
import re
import io
import glob
import time
import sqlite3
from typing import List, Tuple, Dict

import pandas as pd
import streamlit as st

# ──────────────────────────────────────────────────────────────────────────────
# App Config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Student SQL Lab",
    page_icon="🚄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# Aesthetic
# ──────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    :root {
      --glass-bg: rgba(255,255,255,.06);
      --glass-bd: rgba(255,255,255,.10);
      --ink: #EAF2FF;
      --ink-soft: #CFE0FF;
    }
    .stApp {
        background:
          radial-gradient(1200px 800px at 12% 8%, rgba(136,195,255,.12), transparent 40%),
          radial-gradient(1000px 600px at 90% 20%, rgba(197,255,221,.12), transparent 40%),
          linear-gradient(180deg, #0a0f1f 0%, #0e1324 50%, #101628 100%);
        color: var(--ink);
    }
    .glass {
        background: var(--glass-bg);
        border: 1px solid var(--glass-bd);
        backdrop-filter: blur(10px);
        border-radius: 18px;
        padding: 18px 18px;
        box-shadow: 0 10px 30px rgba(0,0,0,.25);
    }
    .badge {
        display:inline-flex; align-items:center; gap:.5rem;
        padding:6px 10px; border-radius:999px; font-size:.85rem;
        background: rgba(255,255,255,.08); border: 1px solid rgba(255,255,255,.14);
        margin-right: .5rem; margin-bottom:.5rem;
    }
    .metric {
        display:flex; flex-direction:column; padding:10px 14px;
        border-radius:14px; background: rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.10);
        min-width: 140px;
    }
    .metric .k { font-size:1.35rem; font-weight:700; color:#fff; }
    .metric .l { font-size:.85rem; opacity:.8 }
    .headbar { display:flex; align-items:center; justify-content:space-between; gap:1rem; margin-bottom:10px; }
    .head-title { font-size:1.6rem; font-weight:700; letter-spacing:.3px; }
    .grid { display:grid; gap:14px }
    .grid.cols-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .grid.cols-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .pill {
        display:inline-block; padding:6px 10px; border-radius:999px;
        border:1px solid rgba(255,255,255,.18); background: rgba(255,255,255,.08);
        font-size:.78rem; margin:0 6px 6px 0;
    }
    .sqlbox textarea {
        background: rgba(255,255,255,.06) !important; color: #DDE8FF !important;
        border-radius: 14px !important; border: 1px solid rgba(255,255,255,.10) !important;
    }
    .stButton > button {
        border-radius:14px; padding:10px 16px;
        border:1px solid rgba(255,255,255,.18);
        background: rgba(255,255,255,.09); color: var(--ink);
    }
    .stButton > button:hover { background: rgba(255,255,255,.17); }
    [data-testid="stDataFrame"] {
        background: rgba(20,24,40,.55);
        border-radius: 14px;
        border: 1px solid rgba(255,255,255,.06);
    }
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.02));
        border-right: 1px solid rgba(255,255,255,.08);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# Data locations
# ──────────────────────────────────────────────────────────────────────────────
DATA_DIRS = ["data", "/mnt/data"]

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

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# Load
# ──────────────────────────────────────────────────────────────────────────────
csv_files = list_csv_files()
if csv_files:
    conn, txn_tables, master_tables, schema_df, table_category_map = load_csvs_into_sqlite(csv_files)
else:
    conn, txn_tables, master_tables, schema_df, table_category_map = None, [], [], pd.DataFrame(), {}

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.markdown("### 🚄 Student SQL Lab")
page = st.sidebar.segmented_control("View", ["Schema", "Query"], default="Schema")
st.sidebar.divider()

if conn:
    st.sidebar.markdown(f'<span class="badge">Masters {len(master_tables)}</span>', unsafe_allow_html=True)
    st.sidebar.markdown(f'<span class="badge">Transactions {len(txn_tables)}</span>', unsafe_allow_html=True)
    st.sidebar.divider()
    filter_text = st.sidebar.text_input("Filter tables", value="")
    limit_default = st.sidebar.slider("Limit", 100, 20000, 5000, 100)
else:
    filter_text, limit_default = "", 5000

# ──────────────────────────────────────────────────────────────────────────────
# Header bar
# ──────────────────────────────────────────────────────────────────────────────
left, right = st.columns([5,3])
with left:
    st.markdown(
        f"""
        <div class="headbar">
          <div class="head-title">Student SQL Lab</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with right:
    if conn:
        colA, colB, colC = st.columns(3)
        with colA:
            st.markdown(f'<div class="metric"><div class="k">{len(master_tables)}</div><div class="l">Masters</div></div>', unsafe_allow_html=True)
        with colB:
            st.markdown(f'<div class="metric"><div class="k">{len(txn_tables)}</div><div class="l">Transactions</div></div>', unsafe_allow_html=True)
        with colC:
            all_cols = schema_df["column_name"].nunique() if not schema_df.empty else 0
            st.markdown(f'<div class="metric"><div class="k">{all_cols}</div><div class="l">Columns</div></div>', unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────────────────
def render_table_cards(title: str, tables: List[str]):
    st.markdown(f"#### {title}")
    if filter_text:
        tables = [t for t in tables if filter_text.lower() in t.lower()]
    if not tables:
        st.empty()
        return
    cols = st.columns(3) if len(tables) >= 3 else st.columns(max(1, len(tables)))
    for i, t in enumerate(tables):
        with cols[i % len(cols)]:
            st.markdown('<div class="glass">', unsafe_allow_html=True)
            st.markdown(f"**{t}**")
            try:
                prev = pd.read_sql_query(f"SELECT * FROM {t} LIMIT 5;", conn)
                st.dataframe(prev, use_container_width=True, height=220)
            except Exception as e:
                st.write(f"{e}")
            if not schema_df.empty:
                cols_df = schema_df[schema_df["table_name"] == t][["column_name","data_type","pk"]]
                st.dataframe(cols_df.reset_index(drop=True), use_container_width=True, height=220)
            st.markdown('</div>', unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Query helpers (history, quick insert)
# ──────────────────────────────────────────────────────────────────────────────
if "query_history" not in st.session_state:
    st.session_state.query_history = []  # list of dicts {sql, rows, ms, at}

def add_history(sql: str, rows: int, ms: int):
    st.session_state.query_history.insert(0, {"sql": sql, "rows": rows, "ms": ms, "at": time.strftime("%Y-%m-%d %H:%M:%S")})
    st.session_state.query_history = st.session_state.query_history[:12]

def table_picker():
    if not conn: return ""
    all_tables = (txn_tables or []) + (master_tables or [])
    t = st.selectbox("Tables", options=all_tables) if all_tables else ""
    return f"SELECT * FROM {t} LIMIT 100;" if t else "SELECT 1"

def col_browser():
    if not conn or schema_df.empty: return []
    return sorted(schema_df["table_name"].unique().tolist())

# ──────────────────────────────────────────────────────────────────────────────
# Pages
# ──────────────────────────────────────────────────────────────────────────────
if page == "Schema":
    if not conn:
        st.markdown('<div class="glass">No data.</div>', unsafe_allow_html=True)
    else:
        render_table_cards("Masters", master_tables)
        st.markdown('---')
        render_table_cards("Transactions", txn_tables)

elif page == "Query":
    # Result first
    if "last_df" in st.session_state and st.session_state["last_df"] is not None:
        df = st.session_state["last_df"]
        st.markdown('<div class="glass">', unsafe_allow_html=True)
        st.dataframe(df, use_container_width=True, height=480)
        r, c = df.shape
        st.markdown(f'<div class="badge">Rows {r:,}</div><div class="badge">Cols {c:,}</div>', unsafe_allow_html=True)
        buf = io.StringIO();  df.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button("Download CSV", data=buf.getvalue(), file_name="query_result.csv", mime="text/csv")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="glass">No results.</div>', unsafe_allow_html=True)

    # Editor + tools row
    top_l, top_r = st.columns([3,2])
    with top_l:
        default_sql = table_picker()
    with top_r:
        cols_list = col_browser()
        if cols_list:
            st.multiselect("Columns", options=cols_list, default=[])

    st.markdown('<div class="glass sqlbox">', unsafe_allow_html=True)
    sql = st.text_area("SQL", value=default_sql, height=160, key="sql_box")
    run_col, clear_col, limit_col, _ = st.columns([1,1,2,6])
    with run_col:
        run = st.button("Run")
    with clear_col:
        clear = st.button("Clear")
    with limit_col:
        use_limit = st.checkbox("Auto LIMIT", value=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # History
    if st.session_state.query_history:
        st.markdown("##### History")
        st.markdown('<div class="grid cols-2">', unsafe_allow_html=True)
        for h in st.session_state.query_history:
            with st.container(border=False):
                st.markdown('<div class="glass">', unsafe_allow_html=True)
                st.code(h["sql"], language="sql")
                st.markdown(
                    f'<span class="badge">{h["rows"]:,} rows</span>'
                    f'<span class="badge">{h["ms"]} ms</span>'
                    f'<span class="badge">{h["at"]}</span>',
                    unsafe_allow_html=True
                )
                st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    if clear:
        st.session_state["last_df"] = None
        st.rerun()

    if run:
        if not conn:
            st.error("No database.")
        else:
            sql_clean = sql.strip().rstrip(";").strip()
            if not is_select_only(sql_clean):
                st.error("Only SELECT / WITH.")
            else:
                q = enforce_limit(sql_clean, limit_default) if use_limit else sql_clean
                t0 = time.time()
                try:
                    df = pd.read_sql_query(q, conn)
                    ms = int((time.time() - t0) * 1000)
                    st.session_state["last_df"] = df
                    add_history(sql_clean, len(df), ms)
                    st.rerun()
                except Exception as e:
                    st.error(f"Execution error: {e}")
