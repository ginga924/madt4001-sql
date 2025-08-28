import os
import re
import io
import glob
import sqlite3
import pandas as pd
import streamlit as st

# =======================
# App Config
# =======================
st.set_page_config(page_title="Student SQL Lab (SQLite)", layout="wide")

DATA_DIR = "data"  # auto-load from ./data (no UI control)

# =======================
# Clean table name mapping (MonsoonSIM)
# =======================
TABLE_NAME_MAP = {
    "399394_AllTeam_MonsoonSIM_CT_cashOnHand.csv": "cash_on_hand",
    "399394_AllTeam_MonsoonSIM_CT_financials.csv": "financials",
    "399394_AllTeam_MonsoonSIM_CT_inventory.csv": "inventory",
    "399394_AllTeam_MonsoonSIM_CT_marketing.csv": "marketing",
    "399394_AllTeam_MonsoonSIM_CT_operating_expense.csv": "operating_expense",
    "399394_AllTeam_MonsoonSIM_CT_procurement.csv": "procurement",
    "399394_AllTeam_MonsoonSIM_CT_sales.csv": "sales",
}

# =======================
# Sidebar (minimal)
# =======================
st.sidebar.title("SQL Lab")
page = st.sidebar.radio("Menu", ["Schema", "Query"])
st.sidebar.caption("CSV files in ./data are loaded into an in-memory SQLite DB with clean table names.")

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
def list_csv_files():
    return sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))

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
            if num_try.dropna().apply(float.is_integer).all():
                out[col] = num_try.astype("Int64")
            else:
                out[col] = num_try.astype("float64")
            continue

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

def build_schema_df(conn: sqlite3.Connection, tables: list[str]) -> pd.DataFrame:
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

def load_csvs_into_sqlite(csv_paths):
    """
    Loads CSVs -> pandas (smart cast) -> SQLite :memory:
    Returns (conn, loaded_tables, schema_df)
    """
    conn = sqlite3.connect(":memory:")
    loaded = []
    for f in csv_paths:
        fname = os.path.basename(f)
        tbl = TABLE_NAME_MAP.get(fname, sanitize_name(fname))
        df = read_csv_any(f)
        df = smart_cast_df(df)
        df.to_sql(tbl, conn, if_exists="replace", index=False)
        loaded.append(tbl)
    schema_df = build_schema_df(conn, loaded)
    return conn, loaded, schema_df

# =======================
# Load data into SQLite
# =======================
csv_files = list_csv_files()
if not csv_files:
    st.warning("No CSVs found in ./data. Put your files there.")
    conn, loaded_tables, schema_df = None, [], pd.DataFrame()
else:
    conn, loaded_tables, schema_df = load_csvs_into_sqlite(csv_files)
    st.sidebar.info(f"Loaded {len(loaded_tables)} transaction table(s): " + ", ".join(loaded_tables))

# =======================
# Schema Page
# =======================
if page == "Schema":
    st.header("📚 Database Schema — Transaction Tables (SQLite)")
    if not loaded_tables:
        st.info("Place CSV files in ./data.")
    else:
        for t in loaded_tables:
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

# =======================
# Query Page (chat-like: output above editor; .shape under table)
# =======================
elif page == "Query":
    st.header("📝 Run SQL Query (SQLite)")
    st.caption("Only **SELECT / WITH** are allowed. A `LIMIT 5000` is added if missing. Trailing `;` is optional.")

    # Output area FIRST (like chat)
    if "last_df" in st.session_state and st.session_state["last_df"] is not None:
        df = st.session_state["last_df"]
        st.dataframe(df, use_container_width=True)           # <- table first
        rows, cols = df.shape
        st.caption(f"Result shape: {rows:,} rows × {cols:,} columns")  # <- .shape UNDER the table

        # Download last result
        buf = io.StringIO()
        df.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button(
            "⬇ Download CSV of last result",
            data=buf.getvalue(),
            file_name="query_result.csv",
            mime="text/csv",
        )
    else:
        st.info("Run a query to see results here.")

    # Editor below (chat-like)
    example_sql = "SELECT 1 AS hello;"
    if loaded_tables:
        first = loaded_tables[0]
        example_sql = f"-- Example: first 100 rows\nSELECT * FROM {first} LIMIT 100;"

    sql = st.text_area("SQL", value=example_sql, height=180, key="sql_box")

    c1, c2 = st.columns([1,1])
    with c1:
        run = st.button("▶ Run Query", type="primary")
    with c2:
        clear = st.button("🧹 Clear")

    if clear:
        st.session_state["last_df"] = None
        st.rerun()

    if run:
        if conn is None:
            st.error("No database loaded.")
        else:
            sql_clean = sql.strip().rstrip(";").strip()
            if not is_select_only(sql_clean):
                st.error("Only SELECT / WITH statements are allowed.")
            else:
                safe_sql = enforce_limit(sql_clean, 5000)
                try:
                    df = pd.read_sql_query(safe_sql, conn)
                    st.session_state["last_df"] = df  # store for display above
                    st.rerun()  # refresh to render output above editor
                except Exception as e:
                    st.error(f"Execution error: {e}")
