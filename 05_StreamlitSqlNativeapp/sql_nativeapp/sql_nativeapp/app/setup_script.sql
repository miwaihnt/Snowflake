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
CREATE OR REPLACE PROCEDURE code_schema.show_warehouse_proc()
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

//プロシージャsshow_warehouseの権限付与
GRANT USAGE ON PROCEDURE code_schema.show_warehouse_proc() TO APPLICATION ROLE sql_native_app;

//プロシージャsql2(ローカルスピルサイズ範囲ごとのSQL数)
CREATE OR REPLACE PROCEDURE code_schema.localSpill1(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  CONV_STRTIME TIMESTAMP_TZ,
  START_TIME TIMESTAMP_NTZ,
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_lspilled AS (
      SELECT * FROM (
        SELECT
          START_TIME,
          CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) AS conv_strtime,
          warehouse_name,
          warehouse_size,
          COUNT(*) AS total_count_sql,
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE = 0 THEN 1 END) AS "0: LOCAL_SPILLED_SIZE = 0B", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE > 0 AND BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 <= 1 THEN 1 END) AS "1: 0B < LOCAL_SPILLED_SIZE <= 1MB",
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 > 1 AND BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 <= 1 THEN 1 END) AS "2: 1MB < LOCAL_SPILLED_SIZE <= 1GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 > 1 AND BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 <= 10 THEN 1 END) AS "3: 1GB < LOCAL_SPILLED_SIZE <= 10GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 > 10 AND BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 <= 100 THEN 1 END) AS "4: 10GB < LOCAL_SPILLED_SIZE <= 100GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 > 100 AND BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 / 1024 <= 1 THEN 1 END) AS "5: 100GB < LOCAL_SPILLED_SIZE <= 1TB",
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 / 1024 > 1 THEN 1 END) AS "6: 1TB < LOCAL_SPILLED_SIZE"
        FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND BYTES_SCANNED > 0
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR LOCAL_SPILLED_SIZE_RANGE IN (
          "0: LOCAL_SPILLED_SIZE = 0B", 
          "1: 0B < LOCAL_SPILLED_SIZE <= 1MB",   
          "2: 1MB < LOCAL_SPILLED_SIZE <= 1GB", 
          "3: 1GB < LOCAL_SPILLED_SIZE <= 10GB", 
          "4: 10GB < LOCAL_SPILLED_SIZE <= 100GB", 
          "5: 100GB < LOCAL_SPILLED_SIZE <= 1TB",
          "6: 1TB < LOCAL_SPILLED_SIZE"
        )
      )
    )
    SELECT 
      CONV_STRTIME TIMESTAMP_TZ,
      START_TIME TIMESTAMP_NTZ,
      WAREHOUSE_NAME STRING,
      WAREHOUSE_SIZE STRING,
      TOTAL_COUNT_SQL NUMBER
    FROM sqlcnt_per_lspilled
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill1(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;
