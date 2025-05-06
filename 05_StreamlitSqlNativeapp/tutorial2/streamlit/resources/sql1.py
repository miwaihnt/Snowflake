# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

#UI定義
st.markdown("<h1 style='color:teal;'>WH設定</h1>",unsafe_allow_html = True)
st.write("")

#ロジック定義
session = get_active_session()

def execute_query1():
    query1 = "show warehouses;"

    query_result1 = session.sql(query1).collect()
    df = pd.DataFrame(query_result1)
    st.write(df)
    
    with st.expander("実行したSQL",expanded=False):
        st.code(query1)
                     

def main1():
    if st.button("クエリ実行"):
        execute_query1()

main1()