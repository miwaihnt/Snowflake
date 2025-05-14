import streamlit as st
from snowflake.snowpark.context import get_active_session

# UI
st.title("PythonストアドプロシージャでSHOW WAREHOUSES")

# セッション取得
session = get_active_session()

def execute_proc():
    try:
        # Pythonストアドプロシージャを呼び出して結果をDataFrameで受け取る
        df = session.call("code_schema.show_warehouse_proc")
        st.dataframe(df)  # 横に広いので dataframe 推奨

        with st.expander("実行SQL", expanded=False):
            st.code("CALL code_schema.show_warehouse_proc();")
    except Exception as e:
        st.error(f"エラーが発生しました: {e}")

# ボタンで実行
if st.button("クエリ実行"):
    execute_proc()
