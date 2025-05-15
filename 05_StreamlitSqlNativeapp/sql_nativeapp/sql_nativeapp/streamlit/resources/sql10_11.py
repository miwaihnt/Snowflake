import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import altair as alt
import datetime as dt



st.markdown("<h1 style='color:teal;'>クエリ実行時間</h1>",unsafe_allow_html = True)
st.write("")

session = get_active_session()

# ウェアハウス一覧を取得
@st.cache_data
def show_warehouses():

    query = "show warehouses"
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

#sql10 クエリの実行
def execute_query10(warehouse,begin_time,end_time):    

    query10 = f'''
        with sqlcnt_per_range as (
            select * from (
                select
                    warehouse_name,
                    warehouse_size,
                    count(*) total_count_sql,
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 0   and (total_elapsed_time / 1000) <= 1 THEN 1 ELSE NULL END)      AS "1: 0s < ELAPSED_TIME <= 1s", 
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 1   and (total_elapsed_time / 1000) <= 10 THEN 1 ELSE NULL END)     AS "2: 1s < ELAPSED_TIME <= 10s", 
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 10  and (total_elapsed_time / 1000) <= 60 THEN 1 ELSE NULL END)     AS "3: 10s < ELAPSED_TIME <= 60s",
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 60  and (total_elapsed_time / 1000) <= 600 THEN 1 ELSE NULL END)    AS "4: 60s < ELAPSED_TIME <= 600s",
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 600 and (total_elapsed_time / 1000) <= 3600 THEN 1 ELSE NULL END)   AS "5: 600s < ELAPSED_TIME <= 3600s",
                    COUNT(CASE WHEN (total_elapsed_time / 1000) > 3600 THEN 1 ELSE NULL END) AS "6: 3600s < ELAPSED_TIME", 
                from
                    snowflake.account_usage.query_history
                where
                    execution_status = 'SUCCESS'
                and warehouse_name = '{warehouse}'
                and warehouse_size is not null
                and total_elapsed_time > 0
                and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_time}' AND '{end_time}'
                group by all
            
            )
            unpivot (sql_count for elapsed_time_range in (
                "1: 0s < ELAPSED_TIME <= 1s",
                "2: 1s < ELAPSED_TIME <= 10s",
                "3: 10s < ELAPSED_TIME <= 60s",
                "4: 60s < ELAPSED_TIME <= 600s",
                "5: 600s < ELAPSED_TIME <= 3600s",
                "6: 3600s < ELAPSED_TIME"

            ))
        )

        select 
            *,
            round(sql_count / total_count_sql*100,3) || '%' as "%SQL_COUNT"
        from 
            sqlcnt_per_range;  
    
    '''

    #クエリの実行
    query_result = session.sql(query10).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return

    #表の結果表示
    st.write(df)

    #データの準備
    df['SQL_COUNT'] = df['%SQL_COUNT'].str.rstrip('%').astype(float)

    elapsed_time_range = [
        "1: 0s < ELAPSED_TIME <= 1s",
        "2: 1s < ELAPSED_TIME <= 10s",
        "3: 10s < ELAPSED_TIME <= 60s",
        "4: 60s < ELAPSED_TIME <= 600s",
        "5: 600s < ELAPSED_TIME <= 3600s",
        "6: 3600s < ELAPSED_TIME"
    ]

    

    #bar_chartの描画
    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('ELAPSED_TIME_RANGE',sort=elapsed_time_range),
        x=alt.X('SQL_COUNT',stack=True),
        color='ELAPSED_TIME_RANGE',
        tooltip=['ELAPSED_TIME_RANGE','SQL_COUNT']
    ).properties(
        title='クエリの実行時間の特定'
    )
    st.altair_chart(bar_chart,use_container_width=True) 
    
    with st.expander("実行したSQL",expanded=False):
        st.code(query10,language='sql')


#クエリ定義 sql11


def execute_query11(warehouse,begin_time,end_time):    

    query11 = f'''
        select
            warehouse_name,				
            warehouse_size,				
            query_id,				
            query_text,				
            CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,				
            round(total_elapsed_time/1000,1) total_elapsed_time_s				
        from
            snowflake.account_usage.query_history				
        where				
            execution_status = 'SUCCESS'				
            and warehouse_name = '{warehouse}'				
            and warehouse_size is not null				
            and total_elapsed_time > 0
            and CONVERT_TIMEZONE('Asia/Tokyo', to_timestamp_ntz(START_TIME)) between '{begin_time}' AND '{end_time}'
            and total_elapsed_time_s > 0				
        order by total_elapsed_time_s desc				
;				

    '''
    
    
    #クエリの実行
    query_result = session.sql(query11).collect()
    df = pd.DataFrame(query_result)

    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return

    
    #表の結果表示
    st.write(df)


    with st.expander("実行したSQL",expanded=False):
        st.code(query11,language='sql')
    

def main10():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse,begin_str,end_str = long_runnning_query(name,key_prefix='sql10')

    if st.button("クエリ実行",key='execute_query10_button'):
        execute_query10(warehouse, begin_str, end_str)



def main11():
    result = show_warehouses()
    df = pd.DataFrame(result)
    name = df[['name']]
    warehouse,begin_time,end_time = long_runnning_query(name,key_prefix='sql11')
    if st.button("クエリ実行",key='execute_query11_button'):
        execute_query11(warehouse, begin_time, end_time)


#UI
tab1, tab2 = st.tabs(["クエリ実行時間範囲ごとのSQL数", "クエリ実行時間が長いSQL"])
with tab1:
    st.write("")
    st.markdown("### クエリ実行時間範囲ごとのSQL数")
    main10()
with tab2:
    st.write("")
    st.markdown("### クエリ実行時間が長いSQL")
    main11()
