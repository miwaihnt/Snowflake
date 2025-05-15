import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import altair as alt
import datetime as dt

st.markdown("<h1 style='color:teal;'>スキャンパーティション割合</h1>",unsafe_allow_html = True)
st.write("")
session = get_active_session()


#クエリ定義 sql14,15共通
@st.cache_data
def show_warehouses():

    query = '''
        show warehouses
    '''
    exe = session.sql(query).collect()
    return exe


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
    
#sql14 クエリの実行
def execute_query14(warehouse,begin_time,end_time):    

    query14 = f'''
        with query as (					
            select * from					
                (					
                    select					
                        warehouse_name,					
                        warehouse_size,					
                        COUNT(*) total_count_sql,					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 0  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 1   THEN 1 ELSE NULL END) AS "1: 0 < SCAN_P_RATIO <= 1%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 1  and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 10  THEN 1 ELSE NULL END) AS "2: 1 < SCAN_P_RATIO <= 10%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 10 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 30  THEN 1 ELSE NULL END) AS "3: 10 < SCAN_P_RATIO <= 30%",   					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 30 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 60  THEN 1 ELSE NULL END) AS "4: 30 < SCAN_P_RATIO <= 60%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 60 and PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 <= 90  THEN 1 ELSE NULL END) AS "5: 60 < SCAN_P_RATIO <= 90%",					
                        COUNT(CASE WHEN PARTITIONS_SCANNED/PARTITIONS_TOTAL*100 > 90                                                    THEN 1 ELSE NULL END) AS "6: 90% < SCAN_P_RATIO"					
                    from					
                        snowflake.account_usage.query_history					
                    where					
                        execution_status = 'SUCCESS'					
                    and warehouse_name = '{warehouse}'					
                    and warehouse_size is not null					
                    and PARTITIONS_TOTAL > 0
                    and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_time}' AND '{end_time}'
                    group by all					
                )					
            unpivot (sql_cnt for SCAN_PARTITION_RATO_RANGE in (					
                "1: 0 < SCAN_P_RATIO <= 1%",					
                "2: 1 < SCAN_P_RATIO <= 10%",					
                "3: 10 < SCAN_P_RATIO <= 30%", 					
                "4: 30 < SCAN_P_RATIO <= 60%",					
                "5: 60 < SCAN_P_RATIO <= 90%",					
                "6: 90% < SCAN_P_RATIO"					
            ))					
        )					
        select *,round(sql_cnt / total_count_sql * 100,1) ||'%' as "%SCAN_PARTITION_RATIO" from query;					

    '''
    
    
    #クエリの実行
    query_result = session.sql(query14).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return

    #表の結果表示
    st.write(df)

    #データの準備
    df['SCAN_PARTITION_RATIO'] = df['%SCAN_PARTITION_RATIO'].str.rstrip('%').astype(float)

    #bar_chartの描画
    bar_chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('SCAN_PARTITION_RATIO'),
        y=alt.Y('SCAN_PARTITION_RATO_RANGE'),
        tooltip=['SCAN_PARTITION_RATIO']
    ).properties(
        title='クエリの実行時間の特定'
    )

    st.altair_chart(bar_chart,use_container_width=True) 
    
    with st.expander("実行したSQL",expanded=False):
        st.code(query14,language='sql')
    


#クエリ定義 sql15

def execute_query15(warehouse,begin_time,end_time):    

    query15 = f'''
        select 			
           warehouse_name,			
           warehouse_size,			
           query_id,			
           query_text,			
           round(PARTITIONS_SCANNED/PARTITIONS_TOTAL*100,2) "%SCAN_PARTITION_RATIO" ,			
           PARTITIONS_SCANNED ,			
           PARTITIONS_TOTAL   ,			
           round(BYTES_SCANNED/1024/1024/1024,2) BYTES_SCANNED_GB,			
           round(total_elapsed_time / 1000,2) total_elapsed_time_s			
        from			
            snowflake.account_usage.query_history			
        where			
            execution_status = 'SUCCESS'			
        and warehouse_name = '{warehouse}'			
        and warehouse_size is not null
        and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_time}' AND '{end_time}'
        and (PARTITIONS_TOTAL > 0 and "%SCAN_PARTITION_RATIO" >= 90 and "%SCAN_PARTITION_RATIO" <= 100)			
        order by "%SCAN_PARTITION_RATIO" desc, PARTITIONS_SCANNED desc			
	
;				

    '''
    
    
    #クエリの実行
    query_result = session.sql(query15).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return

    
    #表の結果表示
    st.write(df)


    with st.expander("実行したSQL",expanded=False):
        st.code(query15,language='sql')
    

def main14():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse,begin_time,end_time = long_runnning_query(name,key_prefix='sql10')

    if st.button("クエリ実行",key='execute_query10_button'):
        execute_query14(warehouse, begin_time, end_time)


def main15():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse,begin_time,end_time = long_runnning_query(name,key_prefix='sql11')
    if st.button("クエリ実行",key='execute_query11_button'):
        execute_query15(warehouse, begin_time, end_time)





##UI定義
tab1, tab2 = st.tabs(["クエリスキャンサイズ範囲ごとのSQL数", "クエリスキャンサイズが多いSQL"])
with tab1:
    st.write("")
    st.markdown("### クエリスキャンサイズ範囲ごとのSQL数")
    main14()
with tab2:
    st.write("")
    st.markdown("### クエリスキャンサイズが多いSQL")
    main15()
