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

def execute_query2(warehouse,begin_str, end_str):
    df_query2 = session.call(
        "code_schema.localSpill1",
        warehouse,
        begin_str,
        end_str
    )

    rows = df_query2.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)
    df['SQL_COUNT'] = df['PERCENT_SQL_COUNT'].str.rstrip('%').astype(float)

    spilled_size_order = [
        "6: 1TB < LOCAL_SPILLED_SIZE", 
        "5: 100GB < LOCAL_SPILLED_SIZE <= 1TB",
        "4: 10GB < LOCAL_SPILLED_SIZE <= 100GB", 
        "3: 1GB < LOCAL_SPILLED_SIZE <= 10GB", 
        "2: 1MB < LOCAL_SPILLED_SIZE <= 1GB", 
        "1: 0B < LOCAL_SPILLED_SIZE <= 1MB",   
        "0: LOCAL_SPILLED_SIZE = 0B"
    ]

    bar_chart = alt.Chart(df).mark_bar().encode(
        y=alt.Y('LOCAL_SPILLED_SIZE_RANGE', sort=spilled_size_order),
        x=alt.X('SQL_COUNT'),
        color='LOCAL_SPILLED_SIZE_RANGE',
        tooltip=['LOCAL_SPILLED_SIZE_RANGE', 'SQL_COUNT']
    ).properties(
        title="ローカルスピルサイズ範囲ごとのSQL数"
    )
    
    st.altair_chart(bar_chart, use_container_width=True)

def execute_query3(warehouse,begin_str, end_str):

    df_query3 = session.call(
        "code_schema.localSpill2",
        warehouse,
        begin_str,
        end_str
    ) 

    rows = df_query3.collect()
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        st.warning("該当するデータが存在しませんでした。")
        return
    st.write(rows)
  

def main2():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab2")

    if st.button("クエリ実行", key="execute_query2"):
        execute_query2(warehouse, begin_str, end_str)

def main3():
    df = show_warehouses()
    name = df["name"].tolist()
    warehouse, begin_str, end_str = get_filter_inputs(name, key_suffix="tab3")   
    if st.button("クエリ実行", key="execute_query3"):
        execute_query3(warehouse, begin_str, end_str)



# タイトル表示
st.markdown("<h1 style='color:teal;'>ローカルスピリング</h1>", unsafe_allow_html=True)

# タブUI
tab2, tab3 = st.tabs(["ローカルスピルサイズ範囲ごとのSQL数", "ローカルスピルが多いSQL"])
with tab2:
    st.markdown("### ローカルスピルサイズ範囲ごとのSQL数")
    main2()
with tab3:
    st.markdown("### ローカルスピルが多いSQL")
    main3()