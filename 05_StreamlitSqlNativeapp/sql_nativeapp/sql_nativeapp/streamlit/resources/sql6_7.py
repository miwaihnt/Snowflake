import streamlit as st
import pandas as pd
import datetime
from snowflake.snowpark.context import get_active_session
import altair as alt

session = get_active_session()

# ウェアハウス一覧を取得
@st.cache_data(show_spinner=False)
def show_warehouses():
    # ストアドプロシージャの呼び出し
    df = session.call("code_schema.show_warehouse_proc")  # Snowpark DataFrame
    rows = df.collect()                         # ⬅️ クエリを実行して行を取得
    rows_as_dict = [row.as_dict() for row in rows]  # ⬅️ 各行をdictに変換
    return pd.DataFrame(rows_as_dict)  # Pandas DataFrame に変換

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

def execute_query5(warehouse,begin_str, end_str):
    
    df_query5 = session.call(
        "code_schema.localSpill5",
        warehouse,
        begin_str,
        end_str
    )

    rows = df_query5.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)

    df['SQL_COUNT'] = df['PERCENT_SQL_COUNT'].str.rstrip('%').astype(float)

    bar_order = [
        "5: 50% < ELAPSED_TIME_QUEUED%", 
        "4: 20% < ELAPSED_TIME_QUEUED% <= 50%",
        "3: 5% < ELAPSED_TIME_QUEUED% <= 20%",
        "2: 1% < ELAPSED_TIME_QUEUED% <= 5%",
        "1: 0% < ELAPSED_TIME_QUEUED% <= 1%",
        "0: ELAPSED_TIME_QUEUED% = 0%"
    ]

    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('QUEUED_PERCENT_RANGE', sort=bar_order),
        x=alt.X('SQL_COUNT'),
        color='QUEUED_PERCENT_RANGE',
        tooltip=['QUEUED_PERCENT_RANGE', 'SQL_COUNT']
    ).properties(
        title="キュー待ち発生状況"
    )
    
    st.altair_chart(bar_chart, use_container_width=True)

    query_text_sql5 = """
    with sqlcnt_per_queued_percent as (
    select * from
        (
            select
               warehouse_name,
               warehouse_size,
               COUNT(*) total_count_sql,
               COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                               AS "0: ELAPSED_TIME_QUEUED% = 0%",
               COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0      and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "1: 0% < ELAPSED_TIME_QUEUED% <= 1%",
               COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.01   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "2: 1% < ELAPSED_TIME_QUEUED% <= 5%",
               COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.05   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "3: 5% < ELAPSED_TIME_QUEUED% <= 20%",
               COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.2    and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "4: 20% < ELAPSED_TIME_QUEUED% <= 50%",
               COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                             AS "5: 50% < ELAPSED_TIME_QUEUED%" 
            from
                snowflake.account_usage.query_history
            where
                execution_status = 'SUCCESS'
            and warehouse_name = '{warehouse}'
            and warehouse_size is not null
            and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
            group by all
        )
    unpivot (sql_count for queued_percent_range in (
        "0: ELAPSED_TIME_QUEUED% = 0%",
        "1: 0% < ELAPSED_TIME_QUEUED% <= 1%",
        "2: 1% < ELAPSED_TIME_QUEUED% <= 5%",
        "3: 5% < ELAPSED_TIME_QUEUED% <= 20%",
        "4: 20% < ELAPSED_TIME_QUEUED% <= 50%",
        "5: 50% < ELAPSED_TIME_QUEUED%" 
    ))
    )
    select *, round(sql_count / total_count_sql * 100,1) ||'%' as "%SQL_COUNT" from sqlcnt_per_queued_percent;
    """.format(warehouse=warehouse, begin_str=begin_str, end_str=end_str)

    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql5, language="sql")


def execute_query6(warehouse,begin_str, end_str):

    df_query6 = session.call(
        "code_schema.localSpill6",
        warehouse,
        begin_str,
        end_str
    ) 

    rows = df_query6.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)


    query_text_sql6 = """
    select 
       warehouse_name,
       warehouse_size,
       query_id,
       query_text,
       CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
       round(total_elapsed_time/1000,2) elapsed_time_s,
       round(QUEUED_OVERLOAD_TIME/1000,2) queued_time_s,
       round(QUEUED_OVERLOAD_TIME/total_elapsed_time * 100,2) "QUEUED%",
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = '{warehouse}'
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) between '{begin_str}' AND '{end_str}'
    and queued_time_s > 0
    order by queued_time_s desc
    ;
""".format(warehouse=warehouse, begin_str=begin_str, end_str=end_str)


    with st.expander("実行されたクエリを表示", expanded=False):
        st.code(query_text_sql6, language="sql")

  

def main6():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab6")
    
    if st.button("クエリ実行", key="execute_query6"):
        execute_query5(warehouse, begin_str, end_str)

def main7():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab7")   
    if st.button("クエリ実行", key="execute_query7"):
        execute_query6(warehouse, begin_str, end_str)

# タイトル表示
st.markdown("<h1 style='color:teal;'>キュー待ち</h1>",unsafe_allow_html = True)
# タブUI
tab6, tab7 = st.tabs(["キュー待ち発生状況", "キュー待ち時間が長いSQL"])
with tab6:
    st.markdown("### キュー待ち発生状況")
    main6()
with tab7:
    st.markdown("### キュー待ち時間が長いSQL")
    main7()