import os
import re
import io
import glob
import duckdb
import pandas as pd
import streamlit as st

# -----------------------
# App Config
# -----------------------
st.set_page_config(page_title="Student SQL Lab", layout="wide")

DATA_DIR_DEFAULT = "data"  # put your game CSVs here

# -----------------------
# Sidebar
# -----------------------
st.sidebar.title("SQL Lab")
page = st.sidebar.radio("Menu", ["Schema", "Query"])
DATA_DIR = st.sidebar.text_input("Data folder", value=DATA_DIR_DEFAULT)
MAX_PREVIEW_COLS = st.sidebar.number_input("Max columns to preview", 5, 1000, 50, 5)
st.sidebar.caption("CSV files will be loaded as read-only DuckDB views.")

# -----------------------
# Helpers
# -----------------------
def to_table_name(path_or_name: str) -> str:
    base = os.path.splitext(os.path.basename(path_or_name))[0]
    base = re.sub(r"[^A-Za-z0-9_]", "_", base)
    if re.match(r"^\d", base):
        base = "_" + base
    return base.lower()

@st.cache_data(show_spinner=False)
def list_csv_files(data_dir: str):
    return sorted(glob.glob(os.path.join(data_dir, "*.csv")))

def group_hint(table_name: str) -> str:
    n = table_name.lower()
    # heuristics; adjust for your dataset naming
    if any(k in n for k in ["trans", "txn", "sale", "order", "fact", "event", "log", "detail"]):
        return "Transaction"
    if any(k in n for k in ["master", "dim", "lookup", "ref", "dict"]):
        return "Master"
    # fallback: short names = master, long/verb-ish = transaction (very rough)
    return "Master" if len(n) <= 12 else "Transaction"

def is_select_only(sql: str) -> bool:
    forbidden = r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|ATTACH|DETACH|COPY|PRAGMA|CALL|VACUUM)\b"
    starts_ok = bool(re.match(r"^\s*(WITH|SELECT)\b", sql.strip(), re.IGNORECASE))
    return starts_ok and not re.search(forbidden, sql, re.IGNORECASE)

def enforce_limit(sql: str, max_rows: int) -> str:
    # if user didn't add LIMIT, wrap and limit
    if re.search(r"\bLIMIT\s+\d+\b", sql, re.IGNORECASE):
        return sql
    return f"SELECT * FROM ({sql}) AS t LIMIT {int(max_rows)}"

# -----------------------
# Load CSVs into DuckDB
# -----------------------
csv_files = list_csv_files(DATA_DIR)
conn = duckdb.connect(database=":memory:")
conn.execute("PRAGMA disable_progress_bar;")

loaded_tables = []
for f in csv_files:
    tbl = to_table_name(f)
    # read_csv_auto will infer schema; view keeps it read-only
    conn.execute(f"""
        CREATE OR REPLACE VIEW {tbl} AS
        SELECT * FROM read_csv_auto('{f}', IGNORE_ERRORS=TRUE);
    """)
    loaded_tables.append(tbl)

# Precompute schema info
schema_df = None
if loaded_tables:
    schema_df = conn.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='main'
        ORDER BY table_name, ordinal_position
    """).fetchdf()

# -----------------------
# UI — Pages
# -----------------------
if page == "Schema":
    st.header("📚 Database Schema")
    if not loaded_tables:
        st.info(f"Place CSV files in `{DATA_DIR}`.")
    else:
        # Split into Master / Transaction columns (like your sketch)
        col_master, col_txn = st.columns(2)

        with col_master:
            st.subheader("Master")
            master_tables = [t for t in loaded_tables if group_hint(t) == "Master"]
            if not master_tables:
                st.caption("No master tables detected.")
            for t in master_tables:
                with st.expander(f"🗂️ {t}"):
                    if schema_df is not None:
                        cols_df = schema_df[schema_df["table_name"] == t].copy()
                        # show column list in a compact way
                        st.write("**Columns** (name : type)")
                        col_text = "\n".join(
                            f"- `{r.column_name}` : `{r.data_type}`"
                            for r in cols_df.itertuples(index=False)
                        )
                        st.markdown(col_text)
                        # optional tiny preview
                        try:
                            prev = conn.execute(f"SELECT * FROM {t} LIMIT 5").fetchdf()
                            st.dataframe(prev.iloc[:, :MAX_PREVIEW_COLS], use_container_width=True)
                        except Exception as e:
                            st.warning(f"Preview failed: {e}")

        with col_txn:
            st.subheader("Transaction")
            txn_tables = [t for t in loaded_tables if group_hint(t) == "Transaction"]
            if not txn_tables:
                st.caption("No transaction tables detected.")
            for t in txn_tables:
                with st.expander(f"📄 {t}"):
                    if schema_df is not None:
                        cols_df = schema_df[schema_df["table_name"] == t].copy()
                        st.write("**Columns** (name : type)")
                        col_text = "\n".join(
                            f"- `{r.column_name}` : `{r.data_type}`"
                            for r in cols_df.itertuples(index=False)
                        )
                        st.markdown(col_text)
                        try:
                            prev = conn.execute(f"SELECT * FROM {t} LIMIT 5").fetchdf()
                            st.dataframe(prev.iloc[:, :MAX_PREVIEW_COLS], use_container_width=True)
                        except Exception as e:
                            st.warning(f"Preview failed: {e}")

elif page == "Query":
    st.header("📝 Run SQL Query")
    st.caption("Safety: only **SELECT / WITH** statements are allowed. A LIMIT is added automatically if missing.")

    # Build a small example menu using first table (if exists)
    example_sql = "SELECT 1 AS hello;"
    if loaded_tables:
        first = loaded_tables[0]
        example_sql = f"-- Example: first 100 rows\nSELECT * FROM {first} LIMIT 100;"

    sql = st.text_area("SQL", value=example_sql, height=180)

    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        max_rows = st.number_input("Max result rows", 100, 200000, 5000, 100)
    with c2:
        run = st.button("▶ Run Query", type="primary")
    with c3:
        clear = st.button("🧹 Clear")

    if clear:
        st.experimental_rerun()

    if run:
        if not is_select_only(sql):
            st.error("Only SELECT / WITH statements are allowed.")
        else:
            safe_sql = enforce_limit(sql, int(max_rows))
            try:
                df = conn.execute(safe_sql).fetchdf()

                # ✅ Output size using .shape
                rows, cols = df.shape
                st.success(f"Result: {rows:,} rows × {cols:,} columns")

                st.dataframe(df, use_container_width=True)

                # Download CSV
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

# -----------------------
# Footer Tips
# -----------------------
with st.expander("Tips"):
    st.markdown("""
- Join example: `SELECT * FROM table_a a JOIN table_b b ON a.key = b.key`
- Aggregate example: `SELECT col, COUNT(*) FROM table GROUP BY col`
- Date parsing: DuckDB can parse common date strings automatically.
- Keep CSV headers clean (no duplicates) for best results.
""")
    