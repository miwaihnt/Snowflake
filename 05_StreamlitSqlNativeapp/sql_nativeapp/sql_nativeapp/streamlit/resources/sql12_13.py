import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import altair as alt
import datetime as dt


st.markdown("<h1 style='color:teal;'>クエリスキャンサイズ</h1>",unsafe_allow_html = True)
st.write("")
session = get_active_session()



#クエリ定義 sql12,13共通
@st.cache_data
def show_warehouses():

    query = '''
        show warehouses
    '''
    exe = session.sql(query).collect()
    return exe

# クエリ定義　sql13
def input_scan_range():
    
    minNumber = st.number_input("スキャンサイズの下限(GB)", value=0, placeholder="Type a number...")
    maxNumber = st.number_input("スキャンサイズの上限(GB)", value=10, placeholder="Type a number...")


    return minNumber,maxNumber



#クエリ実行時間の特定
def long_runnning_query(warehouse_name,key_prefix):

    today = dt.date.today()
    first_day = today.replace(day=1)
    next_month = (today.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    last_day = next_month - dt.timedelta(days=1)
    
    col1,col2,col3 = st.columns(3)
    
    with col1:
        warehouse = st.selectbox(
            "ウェアハウスを選択",
            warehouse_name,
            key=f'{key_prefix}warehouse_selectboxs'
        )
    with col2:
        begin_date = st.date_input("開始日", value=first_day,key=f'{key_prefix}_date_begin_input')
        begin_time = st.time_input("開始時刻",value=dt.time(0, 0),key=f'{key_prefix}_time_begin_input')

    with col3:
        end_date = st.date_input("終了日", value=last_day,key=f'{key_prefix}_date_end_input')
        end_time = st.time_input("終了時刻",dt.time(23,59),key=f'{key_prefix}_time_end_input')

    begin_datetime = dt.datetime.combine(begin_date, begin_time)
    end_datetime = dt.datetime.combine(end_date, end_time)

    begin_str = begin_datetime.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_datetime.strftime('%Y-%m-%d %H:%M:%S')    
 
    return warehouse,begin_str, end_str



#sql12 クエリの実行
def execute_query12(warehouse,begin_time,end_time):    

    query12 = f'''
        with sqlcnt_per_scansize as (					
            select * from					
                (					
                    select
                       warehouse_name,					
                       warehouse_size,					
                       COUNT(*) total_count_sql,					
                       COUNT(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN 1 ELSE NULL END) AS "1: 0B < SCAN_SIZE <= 1GB",					
                       COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN 1 ELSE NULL END) AS "2: 1GB < SCAN_SIZE <= 20GB",   					
                       COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN 1 ELSE NULL END) AS "3: 20GB < SCAN_SIZE <= 50GB",					
                       COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN 1 ELSE NULL END)                                           AS "4: 50GB < SCAN_SIZE"					
                    from					
                        snowflake.account_usage.query_history					
                    where					
                        execution_status = 'SUCCESS'					
                    and warehouse_name = '{warehouse}'				
                    and warehouse_size is not null					
                    and BYTES_SCANNED > 0
                    and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_time}' AND '{end_time}'
                    group by all					
                )					
            unpivot (sql_count for scan_size_range in (					
                "1: 0B < SCAN_SIZE <= 1GB",					
                "2: 1GB < SCAN_SIZE <= 20GB", 					
                "3: 20GB < SCAN_SIZE <= 50GB",					
                "4: 50GB < SCAN_SIZE"					
            ))					
            )					
        select *, round(sql_count / total_count_sql * 100,1) ||'%' as "%SQL_COUNT" from sqlcnt_per_scansize;					
    
    '''
    
    
    #クエリの実行
    query_result = session.sql(query12).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
       
    #表の結果表示
    st.write(df)

    #データの準備
    df['SQL_COUNT'] = df['%SQL_COUNT'].str.rstrip('%').astype(float)

    scan_size_range = [
        "1: 0B < SCAN_SIZE <= 1GB",					
        "2: 1GB < SCAN_SIZE <= 20GB", 					
        "3: 20GB < SCAN_SIZE <= 50GB",					
        "4: 50GB < SCAN_SIZE"					
    ]

    #bar_chartの描画
    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('SCAN_SIZE_RANGE',sort=scan_size_range),
        x=alt.X('SQL_COUNT'),
        color = 'SCAN_SIZE_RANGE',
        tooltip=['SCAN_SIZE_RANGE','SQL_COUNT']
    ).properties(
        title='クエリスキャンサイズ範囲ごとのSQL数'
    )

    st.altair_chart(bar_chart,use_container_width=True) 
    
    with st.expander("実行したSQL",expanded=False):
        st.code(query12,language='sql')


#クエリ定義 sql13


def execute_query13(warehouse,begin_time,end_time,minNumber,maxNumber):    

    query13 = f'''
        select 
            warehouse_name,
            warehouse_size,
            query_id,
            query_text,
            round(BYTES_SCANNED/1024/1024/1024,2) BYTES_SCANNED_GB,
            round(total_elapsed_time / 1000,2) total_elapsed_time_s
        from
            snowflake.account_usage.query_history
        where
            execution_status = 'SUCCESS'
            and warehouse_name = '{warehouse}'
            and warehouse_size is not null
            and BYTES_SCANNED_GB > '{minNumber}'
            and BYTES_SCANNED_GB <= '{maxNumber}'
            and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_time}' AND '{end_time}'       
        order by BYTES_SCANNED_GB desc  
        ;

    '''
    
    
    #クエリの実行
    query_result = session.sql(query13).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return

    #表の結果表示
    st.write(df)

    with st.expander("実行したSQL",expanded=False):
        st.code(query13,language='sql')
    

def main12():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse,begin_str,end_str = long_runnning_query(name,key_prefix='sql12')

    if st.button("クエリ実行",key='execute_query12_button'):
        execute_query12(warehouse, begin_str,end_str)


def main13():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse,begin_time,end_time = long_runnning_query(name,key_prefix='sql11')
    minNumber,maxNumber = input_scan_range()
    if st.button("クエリ実行",key='execute_query13_button'):
        execute_query13(warehouse, begin_time, end_time,minNumber,maxNumber)





##UI定義
tab1, tab2 = st.tabs(["クエリスキャンサイズ範囲ごとのSQL数", "クエリスキャンサイズが多いSQL"])
with tab1:
    st.write("")
    st.markdown("### クエリスキャンサイズ範囲ごとのSQL数")
    main12()
with tab2:
    st.write("")
    st.markdown("### クエリスキャンサイズが多いSQL")
    main13()
