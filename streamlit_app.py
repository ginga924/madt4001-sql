import os
import re
import io
import glob
import sqlite3
import pandas as pd
import streamlit as st
from typing import List, Tuple, Dict

# =======================
# App Config
# =======================
st.set_page_config(page_title="Student SQL Lab (SQLite)", layout="wide")

# Search both local ./data and /mnt/data (for uploaded files)
DATA_DIRS = ["data", "/mnt/data"]  # both are scanned

# =======================
# Clean table name mapping (MonsoonSIM)
# =======================
# Known CT_ (transaction) tables
TABLE_NAME_MAP = {
    "399394_AllTeam_MonsoonSIM_CT_cashOnHand.csv": "cash_on_hand",
    "399394_AllTeam_MonsoonSIM_CT_financials.csv": "financials",
    "399394_AllTeam_MonsoonSIM_CT_inventory.csv": "inventory",
    "399394_AllTeam_MonsoonSIM_CT_marketing.csv": "marketing",
    "399394_AllTeam_MonsoonSIM_CT_operating_expense.csv": "operating_expense",
    "399394_AllTeam_MonsoonSIM_CT_procurement.csv": "procurement",
    "399394_AllTeam_MonsoonSIM_CT_sales.csv": "sales",

    # Known COM_ (master) tables
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

# Simple classifier by filename
def classify_by_filename(fname: str) -> str:
    if "_CT_" in fname:
        return "transaction"
    if "_COM_" in fname:
        return "master"
    return "unknown"

# =======================
# Sidebar (minimal)
# =======================
st.sidebar.title("SQL Lab")
page = st.sidebar.radio("Menu", ["Schema", "Query"])
st.sidebar.caption("CSV files in ./data and /mnt/data are loaded into an in-memory SQLite DB with clean table names.")

# =======================
# Helpers
# =======================
def sanitize_name(path_or_name: str) -> str:
    base = os.path.splitext(os.path.basename(path_or_name))[0]
    base = re.sub(r"[^A-Za-z0-9_]", "_", base)
    if re.match(r"^\d", base):
        base = "_" + base
    return base.lower()

@st.cache_data(show_spinner=False)
def list_csv_files() -> List[str]:
    paths = []
    for d in DATA_DIRS:
        if os.path.isdir(d):
            paths.extend(glob.glob(os.path.join(d, "*.csv")))
    # de-duplicate while keeping order
    seen = set()
    out = []
    for p in sorted(paths):
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

def is_select_only(sql: str) -> bool:
    forbidden = r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|ATTACH|DETACH|VACUUM|PRAGMA)\b"
    starts_ok = bool(re.match(r"^\s*(WITH|SELECT)\b", sql.strip(), re.IGNORECASE))
    return starts_ok and not re.search(forbidden, sql, re.IGNORECASE)

def enforce_limit(sql: str, max_rows: int = 5000) -> str:
    if re.search(r"\bLIMIT\s+\d+\b", sql, re.IGNORECASE):
        return sql
    return f"SELECT * FROM ({sql}) AS t LIMIT {int(max_rows)}"

def read_csv_any(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8-sig")

def _ratio_not_na(series: pd.Series) -> float:
    total = len(series)
    return 0.0 if total == 0 else series.notna().sum() / total

def smart_cast_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make dtypes as real as possible for SQLite:
    - numeric (int/float)
    - datetime (stored as ISO text in SQLite, but typed as datetime64 in pandas)
    - boolean (True/False)
    """
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
            continue

        # numeric
        num_try = pd.to_numeric(s, errors="coerce")
        if _ratio_not_na(num_try) >= 0.9:
            # int-like?
            try:
                if num_try.dropna().apply(lambda x: float(x).is_integer()).all():
                    out[col] = num_try.astype("Int64")
                else:
                    out[col] = num_try.astype("float64")
                continue
            except Exception:
                pass

        # datetime
        dt_try = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
        if _ratio_not_na(dt_try) >= 0.9:
            out[col] = dt_try
            continue

        # boolean
        if s.dtype == object:
            low = s.astype(str).str.strip().str.lower()
            bool_map = {
                "true": True, "t": True, "yes": True, "y": True, "1": True,
                "false": False, "f": False, "no": False, "n": False, "0": False
            }
            mapped = low.map(bool_map).where(~low.isna(), other=pd.NA)
            if _ratio_not_na(mapped) >= 0.9:
                out[col] = mapped.astype("boolean")
                continue
    return out

def build_schema_df(conn: sqlite3.Connection, tables: List[str]) -> pd.DataFrame:
    rows = []
    cur = conn.cursor()
    for t in tables:
        try:
            cur.execute(f"PRAGMA table_info({t});")
            for cid, name, coltype, notnull, dflt, pk in cur.fetchall():
                rows.append({"table_name": t, "column_name": name, "data_type": coltype or "", "pk": pk})
        except sqlite3.Error:
            pass
    return pd.DataFrame(rows).sort_values(["table_name", "pk", "column_name"]).reset_index(drop=True)

def load_csvs_into_sqlite(csv_paths: List[str]) -> Tuple[sqlite3.Connection, List[str], List[str], pd.DataFrame, Dict[str, str]]:
    """
    Loads CSVs -> pandas (smart cast) -> SQLite :memory:
    Returns (conn, txn_tables, master_tables, schema_df, table_category_map)
    """
    conn = sqlite3.connect(":memory:")
    txn_loaded, master_loaded = [], []
    table_category_map = {}
    for f in csv_paths:
        fname = os.path.basename(f)
        tbl = TABLE_NAME_MAP.get(fname, sanitize_name(fname))
        df = read_csv_any(f)
        df = smart_cast_df(df)
        df.to_sql(tbl, conn, if_exists="replace", index=False)

        category = classify_by_filename(fname)
        table_category_map[tbl] = category
        if category == "transaction":
            txn_loaded.append(tbl)
        elif category == "master":
            master_loaded.append(tbl)
        else:
            # default: treat unknowns as master to be safe
            master_loaded.append(tbl)

    schema_df = build_schema_df(conn, txn_loaded + master_loaded)
    return conn, txn_loaded, master_loaded, schema_df, table_category_map

# =======================
# Load data into SQLite
# =======================
csv_files = list_csv_files()
if not csv_files:
    st.warning("No CSVs found in ./data or /mnt/data. Put your files there.")
    conn, txn_tables, master_tables, schema_df, table_category_map = None, [], [], pd.DataFrame(), {}
else:
    conn, txn_tables, master_tables, schema_df, table_category_map = load_csvs_into_sqlite(csv_files)
    if txn_tables or master_tables:
        st.sidebar.success(
            f"Loaded {len(txn_tables)} transaction table(s) and {len(master_tables)} master table(s)."
        )
        if txn_tables:
            st.sidebar.caption("Transactions: " + ", ".join(txn_tables))
        if master_tables:
            st.sidebar.caption("Masters: " + ", ".join(master_tables))
    else:
        st.sidebar.info("CSV files loaded, but no tables classified. They will appear under 'Masters' by default.")

# =======================
# Schema Page
# =======================
def render_table_block(title: str, tables: List[str], schema_df: pd.DataFrame, conn: sqlite3.Connection):
    st.subheader(title)
    if not tables:
        st.info("None found.")
        return
    for t in tables:
        with st.expander(f"📄 {t}", expanded=False):
            if not schema_df.empty:
                cols_df = schema_df[schema_df["table_name"] == t]
                st.write("**Columns** (name : type)")
                st.markdown("\n".join(
                    f"- `{r.column_name}` : `{r.data_type or 'TEXT'}`"
                    for r in cols_df.itertuples(index=False)
                ))
            try:
                preview = pd.read_sql_query(f"SELECT * FROM {t} LIMIT 5;", conn)
                st.dataframe(preview, use_container_width=True)
            except Exception as e:
                st.warning(f"Preview failed: {e}")

if page == "Schema":
    st.header("📚 Database Schema — SQLite")
    if conn is None:
        st.info("Place CSV files in ./data or /mnt/data.")
    else:
        # Show master and transaction sections separately
        render_table_block("🔷 Master (Common / Dimensions)", master_tables, schema_df, conn)
        render_table_block("🧾 Transactions (CT_)", txn_tables, schema_df, conn)