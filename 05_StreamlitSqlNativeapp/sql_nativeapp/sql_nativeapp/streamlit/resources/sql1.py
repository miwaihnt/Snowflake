# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# UI定義
st.markdown("<h1 style='color:teal;'>WH設定</h1>", unsafe_allow_html=True)
st.write("")

# セッション取得
session = get_active_session()

def execute_proc():
    # ストアドプロシージャの呼び出し
    df = session.call("code_schema.sql1_proc")
    
    # データ表示
    st.write(df)

    # 実行SQLの表示
    with st.expander("実行したSQL", expanded=False):
        st.code("show warehouses;")

def main():
    if st.button("クエリ実行"):
        execute_proc()

main()
