import streamlit as st

def page2():
  st.title("hello")


pages = {
    "WH設定":[
        st.Page("resources/sql1.py",title="WH設定")
      ],
      "スピリング":[
        st.Page("./resources/sql2_3.py",title="ローカルスピル"),
        st.Page("./resources/sql4_5.py",title="リモートスピル")
      ]
    
}

pg = st.navigation(pages)
pg.run()