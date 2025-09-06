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
available_tables = (txn_tables or []) + (master_tables or [])
if available_tables:
    first = available_tables[0]
    example_sql = f"SELECT * FROM {first} LIMIT 100;"

sql = st.text_area("SQL", value=example_sql, height=200, key="sql_box")

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
