import streamlit as st
import pandas as pd
import datetime
from snowflake.snowpark.context import get_active_session
import altair as alt

session = get_active_session()

# ウェアハウス一覧を取得
@st.cache_data
def show_warehouses():
    query = "show warehouses"
    exe = session.sql(query).collect()
    return exe

# フィルター条件入力UI
def get_filter_inputs(warehouse_name, key_suffix):
    today = datetime.date.today()
    first_day = today.replace(day=1)
    next_month = (today.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    last_day = next_month - datetime.timedelta(days=1)

    col1, col2, col3 = st.columns(3)

    with col1:
        warehouse = st.selectbox(
            "ウェアハウスを選択",
            warehouse_name,
            key=f'warehouse_selectbox_sql_{key_suffix}'
        )
    with col2:
        begin_date = st.date_input("開始日", value=first_day, key=f"begin_date_{key_suffix}")
        begin_time = st.time_input("開始時刻", value=datetime.time(0, 0), key=f"begin_time_{key_suffix}")

    with col3:
        end_date = st.date_input("終了日", value=last_day, key=f"end_date_{key_suffix}")
        end_time = st.time_input("終了時刻", value=datetime.time(23, 59), key=f"end_time_{key_suffix}")

    begin_dt = datetime.datetime.combine(begin_date, begin_time)
    end_dt = datetime.datetime.combine(end_date, end_time)

    begin_str = begin_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    return warehouse, begin_str, end_str

# クエリ実行 sql4
def execute_query4(warehouse, begin_str, end_str):    
    sql4 = f"""
    with sqlcnt_per_rspilled as (
    select * from
        (
            select
               warehouse_name,
               warehouse_size,
               COUNT(*) total_count_sql,
               COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                = 0  THEN 1 ELSE NULL END)                                                                 AS "0: REMOTE_SPILLED_SIZE = 0B", 
               COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                > 0  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)       <= 1 THEN 1 ELSE NULL END)      AS "1: 0B < REMOTE_SPILLED_SIZE <= 1MB",   
               COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)      > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 1 THEN 1 ELSE NULL END)      AS "2: 1MB < REMOTE_SPILLED_SIZE <= 1GB", 
               COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 10 THEN 1 ELSE NULL END)     AS "3: 1GB < REMOTE_SPILLED_SIZE <= 10GB", 
               COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 10  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) <= 100 THEN 1 ELSE NULL END)    AS "4: 10GB < REMOTE_SPILLED_SIZE <= 100GB", 
               COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 100 and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) <= 1 THEN 1 ELSE NULL END) AS "5: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
               COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) > 1 THEN 1 ELSE NULL END)                                                             AS "6: 1TB < REMOTE_SPILLED_SIZE"
            from
                snowflake.account_usage.query_history
            where
                execution_status = 'SUCCESS'
            and warehouse_name = '{warehouse}'
            and warehouse_size is not null
            and BYTES_SCANNED > 0
            and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
            group by all
        )
    unpivot (sql_count for REMOTE_SPILLED_SIZE_RANGE in (
        "0: REMOTE_SPILLED_SIZE = 0B", 
        "1: 0B < REMOTE_SPILLED_SIZE <= 1MB",   
        "2: 1MB < REMOTE_SPILLED_SIZE <= 1GB", 
        "3: 1GB < REMOTE_SPILLED_SIZE <= 10GB", 
        "4: 10GB < REMOTE_SPILLED_SIZE <= 100GB", 
        "5: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
        "6: 1TB < REMOTE_SPILLED_SIZE"
    ))
    )
    select *, round(sql_count / total_count_sql * 100,2) ||'%' as "%SQL_COUNT" from sqlcnt_per_rspilled;
    """    
    
    query_result = session.sql(sql4).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return

    st.write(df)

    df['SQL_COUNT'] = df['%SQL_COUNT'].str.rstrip('%').astype(float)

    bar_order = [
        "6: 1TB < REMOTE_SPILLED_SIZE",
        "5: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
        "4: 10GB < REMOTE_SPILLED_SIZE <= 100GB", 
        "3: 1GB < REMOTE_SPILLED_SIZE <= 10GB", 
        "2: 1MB < REMOTE_SPILLED_SIZE <= 1GB", 
        "1: 0B < REMOTE_SPILLED_SIZE <= 1MB",   
        "0: REMOTE_SPILLED_SIZE = 0B"
    ]

    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('REMOTE_SPILLED_SIZE_RANGE', sort=bar_order),
        x=alt.X('SQL_COUNT'),
        color='REMOTE_SPILLED_SIZE_RANGE',
        tooltip=['REMOTE_SPILLED_SIZE_RANGE', 'SQL_COUNT']
    ).properties(
        title="リモートスピルサイズ範囲ごとのSQL数"
    )
    
    st.altair_chart(bar_chart, use_container_width=True)

    with st.expander("実行したSQL",expanded=False):
        st.code(sql4,language='sql')


# クエリ実行 sql5
def execute_query5(warehouse, begin_str, end_str):    
    sql5 = f"""
    select 
       warehouse_name,
       warehouse_size,
       query_id,
       query_text,
       CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
       BYTES_SPILLED_TO_LOCAL_STORAGE,
       round(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_LOCAL_STORAGE_GB,
       BYTES_SPILLED_TO_REMOTE_STORAGE,
       round(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_REMOTE_STORAGE_GB
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = '{warehouse}'
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
    and BYTES_SPILLED_TO_REMOTE_STORAGE > 0
    order by BYTES_SPILLED_TO_REMOTE_STORAGE desc;
    """    
    
    query_result = session.sql(sql5).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    
    st.write(df)
    with st.expander("実行したSQL",expanded=False):
        st.code(sql5,language='sql')

# リモートスピルサイズ範囲ごとのSQL数 sql4
def main4():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab4")

    if st.button("クエリ実行", key="execute_button_tab4"):
        execute_query4(warehouse, begin_str, end_str)

# リモートスピルが多いSQL sql5
def main5():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab5")

    if st.button("クエリ実行", key="execute_button_tab5"):
        execute_query5(warehouse, begin_str, end_str)


# タイトル表示
st.markdown("<h1 style='color:teal;'>リモートスピリング</h1>",unsafe_allow_html=True)
# タブUI
tab4,tab5 = st.tabs(["リモートスピルサイズ範囲ごとのSQL数","リモートスピルが多いSQL"])
with tab4:
    st.markdown("### リモートスピルサイズ範囲ごとのSQL数")
    main4()
with tab5:
    st.markdown("### リモートスピルが多いSQL")
    main5()

