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

# クエリ実行 sql8
def execute_query8(warehouse, begin_str, end_str):    
    sql8 = f"""
    with sqlcnt_per_txblocked_percent as (
    select * from
        (
            select
               warehouse_name,
               warehouse_size,
               COUNT(*) total_count_sql,
               COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                                   AS "0: ELAPSED_TIME_TXBLOCKED% = 0%",
               COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0      and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "1: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
               COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.01   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "2: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
               COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.05   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "3: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
               COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.2    and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "4: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
               COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                                 AS "5: 50% < ELAPSED_TIME_TXBLOCKED%" 
            from
                snowflake.account_usage.query_history
            where
                execution_status = 'SUCCESS'
            and warehouse_name = '{warehouse}'
            and warehouse_size is not null
            and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
            group by all
        )
    unpivot (sql_count for txblocked_percent_range in (
        "0: ELAPSED_TIME_TXBLOCKED% = 0%",
        "1: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
        "2: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
        "3: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
        "4: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
        "5: 50% < ELAPSED_TIME_TXBLOCKED%" 
    ))
    )
    select *, round(sql_count / total_count_sql * 100,2) ||'%' as "%SQL_COUNT" from sqlcnt_per_txblocked_percent;
    """    
    
    query_result = session.sql(sql8).collect()
    df = pd.DataFrame(query_result)



    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    
    st.write(df)

    df['SQL_COUNT'] = df['%SQL_COUNT'].str.rstrip('%').astype(float)

    bar_order = [
        "5: 50% < ELAPSED_TIME_TXBLOCKED%", 
        "4: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
        "3: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
        "2: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
        "1: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
        "0: ELAPSED_TIME_TXBLOCKED% = 0%"
    ]

    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('TXBLOCKED_PERCENT_RANGE', sort=bar_order),
        x=alt.X('SQL_COUNT'),
        color='TXBLOCKED_PERCENT_RANGE',
        tooltip=['TXBLOCKED_PERCENT_RANGE', 'SQL_COUNT']
    ).properties(
        title="TXブロック待ち発生状況"
    )
    
    st.altair_chart(bar_chart, use_container_width=True)
    with st.expander("実行したSQL",expanded=False):
        st.code(sql8,language='sql')

# クエリ実行 sql9
def execute_query9(warehouse, begin_str, end_str):    
    sql9 = f"""
    select 
       warehouse_name,
       warehouse_size,
       query_id,
       query_text,
       CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
       round(total_elapsed_time/1000,2) elapsed_time_s,
       round(TRANSACTION_BLOCKED_TIME/1000,2) txblocked_time_s,
       round(TRANSACTION_BLOCKED_TIME/total_elapsed_time * 100,2) "TXBLOCKED%",
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = '{warehouse}'
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
    and txblocked_time_s > 0
    order by txblocked_time_s desc
    ;
    """    
    
    query_result = session.sql(sql9).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return

    st.write(df)
    with st.expander("実行したSQL",expanded=False):
        st.code(sql9,language='sql')


# TXブロック待ち発生状況 sql8
def main8():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab8")

    if st.button("クエリ実行", key="execute_button_tab8"):
        execute_query8(warehouse, begin_str, end_str)

# TXブロック待ち時間が長いSQL sql9
def main9():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab9")

    if st.button("クエリ実行", key="execute_button_tab9"):
        execute_query9(warehouse, begin_str, end_str)


# タイトル表示
st.markdown("<h1 style='color:teal;'>TXブロック</h1>",unsafe_allow_html = True)
# タブUI
tab8, tab9 = st.tabs(["TXブロック待ち発生状況", "TXブロック待ち時間が長いSQL"])
with tab8:
    st.markdown("### TXブロック待ち発生状況",
                unsafe_allow_html = True)
    main8()
                            
with tab9:
    st.markdown("### TXブロック待ち時間が長いSQL",
                unsafe_allow_html = True)
    main9()

