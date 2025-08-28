import os
import re
import io
import glob
import sqlite3
import pandas as pd
import streamlit as st

# -----------------------
# App Config
# -----------------------
st.set_page_config(page_title="Student SQL Lab (SQLite)", layout="wide")
DATA_DIR_DEFAULT = "data"  # put your CSVs here

# -----------------------
# Sidebar
# -----------------------
st.sidebar.title("SQL Lab")
page = st.sidebar.radio("Menu", ["Schema", "Query"])
DATA_DIR = st.sidebar.text_input("Data folder", value=DATA_DIR_DEFAULT)
st.sidebar.caption("CSV files are loaded into an in-memory SQLite database with clean table names.")

# -----------------------
# Clean table name mapping for your MonsoonSIM CSVs
# (falls back to sanitized filename if not in the map)
# -----------------------
TABLE_NAME_MAP = {
    "399394_AllTeam_MonsoonSIM_CT_cashOnHand.csv": "cash_on_hand",
    "399394_AllTeam_MonsoonSIM_CT_financials.csv": "financials",
    "399394_AllTeam_MonsoonSIM_CT_inventory.csv": "inventory",
    "399394_AllTeam_MonsoonSIM_CT_marketing.csv": "marketing",
    "399394_AllTeam_MonsoonSIM_CT_operating_expense.csv": "operating_expense",
    "399394_AllTeam_MonsoonSIM_CT_procurement.csv": "procurement",
    "399394_AllTeam_MonsoonSIM_CT_sales.csv": "sales",
}

# -----------------------
# Helpers
# -----------------------
def sanitize_name(path_or_name: str) -> str:
    base = os.path.splitext(os.path.basename(path_or_name))[0]
    base = re.sub(r"[^A-Za-z0-9_]", "_", base)
    if re.match(r"^\d", base):
        base = "_" + base
    return base.lower()

@st.cache_data(show_spinner=False)
def list_csv_files(data_dir: str):
    return sorted(glob.glob(os.path.join(data_dir, "*.csv")))

def is_select_only(sql: str) -> bool:
    forbidden = r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|ATTACH|DETACH|VACUUM|PRAGMA)\b"
    starts_ok = bool(re.match(r"^\s*(WITH|SELECT)\b", sql.strip(), re.IGNORECASE))
    return starts_ok and not re.search(forbidden, sql, re.IGNORECASE)

def enforce_limit(sql: str, max_rows: int = 5000) -> str:
    if re.search(r"\bLIMIT\s+\d+\b", sql, re.IGNORECASE):
        return sql
    return f"SELECT * FROM ({sql}) AS t LIMIT {int(max_rows)}"

def read_csv_any(path: str, nrows=None) -> pd.DataFrame:
    try:
        return pd.read_csv(path, nrows=nrows)
    except UnicodeDecodeError:
        return pd.read_csv(path, nrows=nrows, encoding="utf-8-sig")

def load_csvs_into_sqlite(csv_paths):
    """
    Return: (conn, loaded_tables, schema_df)
    - conn: sqlite3.Connection
    - loaded_tables: list[str] of table names
    - schema_df: DataFrame columns: table_name, column_name, data_type, pk
    """
    conn = sqlite3.connect(":memory:")
    loaded = []

    for f in csv_paths:
        fname = os.path.basename(f)
        tbl = TABLE_NAME_MAP.get(fname, sanitize_name(fname))

        df = read_csv_any(f)  # load whole CSV (SQLite needs a DataFrame to write)
        # Write to SQLite; infer types from pandas
        df.to_sql(tbl, conn, if_exists="replace", index=False)
        loaded.append(tbl)

    # Build schema dataframe using PRAGMA
    rows = []
    cur = conn.cursor()
    for t in loaded:
        try:
            cur.execute(f"PRAGMA table_info({t});")
            for cid, name, coltype, notnull, dflt, pk in cur.fetchall():
                rows.append({"table_name": t, "column_name": name, "data_type": coltype or "", "pk": pk})
        except sqlite3.Error:
            pass
    schema_df = pd.DataFrame(rows).sort_values(["table_name", "pk", "column_name"]).reset_index(drop=True)
    return conn, loaded, schema_df

# -----------------------
# Load CSVs -> SQLite
# -----------------------
csv_files = list_csv_files(DATA_DIR)
if not csv_files:
    st.warning(f"No CSVs found in `{DATA_DIR}`. Put your files there.")
    conn, loaded_tables, schema_df = None, [], pd.DataFrame()
else:
    conn, loaded_tables, schema_df = load_csvs_into_sqlite(csv_files)
    st.sidebar.info(f"Loaded {len(loaded_tables)} transaction table(s): " + ", ".join(loaded_tables))

# -----------------------
# UI — Pages
# -----------------------
if page == "Schema":
    st.header("📚 Database Schema — Transaction Tables (SQLite)")
    if not loaded_tables:
        st.info(f"Place CSV files in `{DATA_DIR}`.")
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
                # tiny preview
                try:
                    preview = pd.read_sql_query(f"SELECT * FROM {t} LIMIT 5;", conn)
                    st.dataframe(preview, use_container_width=True)
                except Exception as e:
                    st.warning(f"Preview failed: {e}")

elif page == "Query":
    st.header("📝 Run SQL Query (SQLite)")
    st.caption("Safety: only **SELECT / WITH** statements are allowed. A LIMIT 5000 is added if missing. Trailing `;` is optional.")

    example_sql = "SELECT 1 AS hello;"
    if loaded_tables:
        first = loaded_tables[0]
        example_sql = f"-- Example: first 100 rows\nSELECT * FROM {first} LIMIT 100;"

    sql = st.text_area("SQL", value=example_sql, height=180)

    c1, c2 = st.columns([1,1])
    with c1:
        run = st.button("▶ Run Query", type="primary")
    with c2:
        clear = st.button("🧹 Clear")

    if clear:
        st.experimental_rerun()

    if run:
        if conn is None:
            st.error("No database loaded.")
        else:
            # accept queries with or without trailing semicolon
            sql_clean = sql.strip().rstrip(";").strip()

            if not is_select_only(sql_clean):
                st.error("Only SELECT / WITH statements are allowed.")
            else:
                safe_sql = enforce_limit(sql_clean, 5000)
                try:
                    df = pd.read_sql_query(safe_sql, conn)
                    rows, cols = df.shape
                    st.success(f"Result: {rows:,} rows × {cols:,} columns")
                    st.dataframe(df, use_container_width=True)

                    buf = io.StringIO()
                    df.to_csv(buf, index=False, encoding="utf-8-sig")
                    st.download_button(
                        "⬇ Download CSV",
                        data=buf.getvalue(),
                        file_name="query_result.csv",
                        mime="text/csv",
                    )
                except Exception as e:
                    st.error(f"Execution error: {e}")