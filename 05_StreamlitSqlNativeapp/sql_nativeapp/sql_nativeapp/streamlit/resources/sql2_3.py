import streamlit as st
import pandas as pd
import datetime
from snowflake.snowpark.context import get_active_session
import altair as alt

session = get_active_session()

st.write("Hello22")

# ウェアハウス一覧を取得
@st.cache_data
def show_warehouses():
    query = "show warehouses"
    exe = session.sql(query).collect()
    return exe
