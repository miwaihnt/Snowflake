CREATE APPLICATION ROLE IF NOT EXISTS sql_native_app;
CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE sql_native_app;

CREATE STREAMLIT IF NOT EXISTS code_schema.sql_native_streamlit
  FROM '/'
  MAIN_FILE = 'streamlit_app.py'
;
GRANT USAGE ON STREAMLIT code_schema.sql_native_streamlit TO APPLICATION ROLE sql_native_app;

//プロシージャの定義
CREATE OR REPLACE PROCEDURE code_schema.HELLO()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
  AS
  BEGIN
    RETURN 'Hello Snowflake!';
  END;

//プロシージャの権限付与
GRANT USAGE ON PROCEDURE code_schema.hello() TO APPLICATION ROLE sql_native_app;

//sql1プロシージャの定義
CREATE OR REPLACE PROCEDURE code_schema.sql1_proc()
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'main'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
def main(session):
    # SHOW WAREHOUSES を実行
    session.sql("SHOW WAREHOUSES").collect()

    # クエリIDを明示的に取得
    query_id_row = session.sql("SELECT LAST_QUERY_ID()").collect()
    query_id = query_id_row[0][0]  # クエリIDを取り出す

    # クエリIDをもとに結果をSELECTする
    df = session.sql(f'SELECT * FROM TABLE(RESULT_SCAN(\'{query_id}\'))')
    return df
$$;

//プロシージャsql1の権限付与
GRANT USAGE ON PROCEDURE code_schema.sql1_proc() TO APPLICATION ROLE sql_native_app;