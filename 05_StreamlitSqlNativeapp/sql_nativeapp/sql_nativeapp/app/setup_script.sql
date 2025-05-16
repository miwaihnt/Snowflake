CREATE APPLICATION ROLE IF NOT EXISTS sql_native_app;
CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE sql_native_app;
GRANT CREATE TABLE ON SCHEMA code_schema TO APPLICATION ROLE sql_native_app;

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

//プロシージャ(ローカルスピルサイズ範囲ごとのSQL数)
CREATE OR REPLACE PROCEDURE code_schema.localSpill1(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  LOCAL_SPILLED_SIZE_RANGE STRING,
  SQL_COUNT NUMBER,
  PERCENT_SQL_COUNT STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_lspilled AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE = 0 THEN 1 END) AS "0: LOCAL_SPILLED_SIZE = 0B", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE > 0 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024 <= 1 THEN 1 END) AS "1: 0B < LOCAL_SPILLED_SIZE <= 1MB",
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024 > 1 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 <= 1 THEN 1 END) AS "2: 1MB < LOCAL_SPILLED_SIZE <= 1GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 > 1 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 <= 10 THEN 1 END) AS "3: 1GB < LOCAL_SPILLED_SIZE <= 10GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 > 10 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 <= 100 THEN 1 END) AS "4: 10GB < LOCAL_SPILLED_SIZE <= 100GB", 
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024 > 100 AND BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024/1024 <= 1 THEN 1 END) AS "5: 100GB < LOCAL_SPILLED_SIZE <= 1TB",
          COUNT(CASE WHEN BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024/1024 > 1 THEN 1 END) AS "6: 1TB < LOCAL_SPILLED_SIZE"
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
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      LOCAL_SPILLED_SIZE_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as PERCENT_SQL_COUNT
    FROM sqlcnt_per_lspilled
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill1(STRING,STRING,STRING) TO APPLICATION ROLE sql_native_app;

//プロシージャ定義ローカルスピル発生量が多いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill2(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  BYTES_SPILLED_TO_LOCAL_STORAGE NUMBER,
  BYTES_SPILLED_TO_LOCAL_STORAGE_GB NUMBER,
  BYTES_SPILLED_TO_REMOTE_STORAGE NUMBER,
  BYTES_SPILLED_TO_REMOTE_STORAGE_GB NUMBER
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        round(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_LOCAL_STORAGE_GB,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        round(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_REMOTE_STORAGE_GB
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and BYTES_SPILLED_TO_LOCAL_STORAGE > 0
    order by BYTES_SPILLED_TO_LOCAL_STORAGE desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill2(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;


//プロシージャリモートスピルサイズ発生状況
CREATE OR REPLACE PROCEDURE code_schema.localSpill3(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  REMOTE_SPILLED_SIZE_RANGE STRING,
  SQL_COUNT NUMBER,
  PERCENT_SQL_COUNT STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_rspilled AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                = 0  THEN 1 ELSE NULL END)                                                                 AS "0: REMOTE_SPILLED_SIZE = 0B", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE)                > 0  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)       <= 1 THEN 1 ELSE NULL END)      AS "1: 0B < REMOTE_SPILLED_SIZE <= 1MB",   
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024)      > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 1 THEN 1 ELSE NULL END)      AS "2: 1MB < REMOTE_SPILLED_SIZE <= 1GB", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 1  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024)  <= 10 THEN 1 ELSE NULL END)     AS "3: 1GB < REMOTE_SPILLED_SIZE <= 10GB", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 10  and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) <= 100 THEN 1 ELSE NULL END)    AS "4: 10GB < REMOTE_SPILLED_SIZE <= 100GB", 
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024) > 100 and (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) <= 1 THEN 1 ELSE NULL END) AS "5: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
          COUNT(CASE WHEN (BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024/1024) > 1 THEN 1 ELSE NULL END)                                                             AS "6: 1TB < REMOTE_SPILLED_SIZE"
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
        sql_count FOR REMOTE_SPILLED_SIZE_RANGE IN (
        "0: REMOTE_SPILLED_SIZE = 0B", 
        "1: 0B < REMOTE_SPILLED_SIZE <= 1MB",   
        "2: 1MB < REMOTE_SPILLED_SIZE <= 1GB", 
        "3: 1GB < REMOTE_SPILLED_SIZE <= 10GB", 
        "4: 10GB < REMOTE_SPILLED_SIZE <= 100GB", 
        "5: 100GB < REMOTE_SPILLED_SIZE <= 1TB",
        "6: 1TB < REMOTE_SPILLED_SIZE"          
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      REMOTE_SPILLED_SIZE_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as PERCENT_SQL_COUNT
    FROM sqlcnt_per_rspilled
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill3(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;


//プロシージャ定義リモートスピル発生量が多いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill4(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  BYTES_SPILLED_TO_LOCAL_STORAGE NUMBER,
  BYTES_SPILLED_TO_LOCAL_STORAGE_GB NUMBER,
  BYTES_SPILLED_TO_REMOTE_STORAGE NUMBER,
  BYTES_SPILLED_TO_REMOTE_STORAGE_GB NUMBER
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        round(BYTES_SPILLED_TO_LOCAL_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_LOCAL_STORAGE_GB,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        round(BYTES_SPILLED_TO_REMOTE_STORAGE/1024/1024/1024,2) BYTES_SPILLED_TO_REMOTE_STORAGE_GB
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and BYTES_SPILLED_TO_REMOTE_STORAGE > 0
    order by BYTES_SPILLED_TO_REMOTE_STORAGE desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill4(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;



//キュー待ち発生状況
CREATE OR REPLACE PROCEDURE code_schema.localSpill5(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  QUEUED_PERCENT_RANGE STRING,
  SQL_COUNT NUMBER,
  PERCENT_SQL_COUNT STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_queued_percent AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                               AS "0: ELAPSED_TIME_QUEUED% = 0%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0      and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "1: 0% < ELAPSED_TIME_QUEUED% <= 1%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.01   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "2: 1% < ELAPSED_TIME_QUEUED% <= 5%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.05   and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "3: 5% < ELAPSED_TIME_QUEUED% <= 20%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.2    and (QUEUED_OVERLOAD_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "4: 20% < ELAPSED_TIME_QUEUED% <= 50%",
          COUNT(CASE WHEN (QUEUED_OVERLOAD_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                             AS "5: 50% < ELAPSED_TIME_QUEUED%" 
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
        AND warehouse_name = :warehouse
        AND warehouse_size IS NOT NULL
        AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR queued_percent_range IN (
        "0: ELAPSED_TIME_QUEUED% = 0%",
        "1: 0% < ELAPSED_TIME_QUEUED% <= 1%",
        "2: 1% < ELAPSED_TIME_QUEUED% <= 5%",
        "3: 5% < ELAPSED_TIME_QUEUED% <= 20%",
        "4: 20% < ELAPSED_TIME_QUEUED% <= 50%",
        "5: 50% < ELAPSED_TIME_QUEUED%"        
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      QUEUED_PERCENT_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as PERCENT_SQL_COUNT
    FROM sqlcnt_per_queued_percent
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill5(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;



//キュー待ちが長いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill6(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  ELAPSED_TIME_S NUMBER,
  QUEUED_TIME_S NUMBER,
  PERCENT_QUEUED STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        round(total_elapsed_time/1000,2) elapsed_time_s,
        round(QUEUED_OVERLOAD_TIME/1000,2) queued_time_s,
        round(QUEUED_OVERLOAD_TIME/total_elapsed_time * 100,2) || '%' as "PERCENT_QUEUED",
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and queued_time_s > 0    
    order by queued_time_s desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill6(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;


//trxブロック発生状況
CREATE OR REPLACE PROCEDURE code_schema.localSpill7(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  TXBLOCKED_PERCENT_RANGE STRING,
  SQL_COUNT NUMBER,
  PERCENT_SQL_COUNT STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_txblocked_percent AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) = 0 THEN 1 ELSE NULL END)                                                                   AS "0: ELAPSED_TIME_TXBLOCKED% = 0%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0      and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.01 THEN 1 ELSE NULL END)  AS "1: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.01   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.05 THEN 1 ELSE NULL END)  AS "2: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.05   and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.2 THEN 1 ELSE NULL END)   AS "3: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.2    and (TRANSACTION_BLOCKED_TIME / total_elapsed_time) <= 0.5 THEN 1 ELSE NULL END)   AS "4: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
          COUNT(CASE WHEN (TRANSACTION_BLOCKED_TIME / total_elapsed_time) > 0.5 THEN 1 ELSE NULL END)                                                                 AS "5: 50% < ELAPSED_TIME_TXBLOCKED%"
       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR txblocked_percent_range IN (
        "0: ELAPSED_TIME_TXBLOCKED% = 0%",
        "1: 0% < ELAPSED_TIME_TXBLOCKED% <= 1%",
        "2: 1% < ELAPSED_TIME_TXBLOCKED% <= 5%",
        "3: 5% < ELAPSED_TIME_TXBLOCKED% <= 20%",
        "4: 20% < ELAPSED_TIME_TXBLOCKED% <= 50%",
        "5: 50% < ELAPSED_TIME_TXBLOCKED%"  
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      TXBLOCKED_PERCENT_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 2) || '%' as PERCENT_SQL_COUNT
    FROM sqlcnt_per_txblocked_percent
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill7(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;

//TXブロック待ち時間が長いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill8(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  ELAPSED_TIME_S NUMBER,
  TXBLOCKED_TIME_S NUMBER,
  PERCENT_TXBLOCKED STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) start_time,
        round(total_elapsed_time/1000,2) elapsed_time_s,
        round(TRANSACTION_BLOCKED_TIME/1000,2) txblocked_time_s,
        round(TRANSACTION_BLOCKED_TIME/total_elapsed_time * 100,2) || '%' as "PERCENT_TXBLOCKED",
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and txblocked_time_s > 0  
    order by txblocked_time_s desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill8(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;


//クエリ実行時間の傾向
CREATE OR REPLACE PROCEDURE code_schema.localSpill9(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  ELAPSED_TIME_RANGE STRING,
  SQL_COUNT NUMBER,
  PERCENT_SQL_COUNT STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_range AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 0   and (total_elapsed_time / 1000) <= 1 THEN 1 ELSE NULL END)      AS "1: 0s < ELAPSED_TIME <= 1s", 
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 1   and (total_elapsed_time / 1000) <= 10 THEN 1 ELSE NULL END)     AS "2: 1s < ELAPSED_TIME <= 10s", 
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 10  and (total_elapsed_time / 1000) <= 60 THEN 1 ELSE NULL END)     AS "3: 10s < ELAPSED_TIME <= 60s",
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 60  and (total_elapsed_time / 1000) <= 600 THEN 1 ELSE NULL END)    AS "4: 60s < ELAPSED_TIME <= 600s",
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 600 and (total_elapsed_time / 1000) <= 3600 THEN 1 ELSE NULL END)   AS "5: 600s < ELAPSED_TIME <= 3600s",
          COUNT(CASE WHEN (total_elapsed_time / 1000) > 3600 THEN 1 ELSE NULL END) AS "6: 3600s < ELAPSED_TIME", 

       FROM snowflake.account_usage.query_history
        WHERE execution_status = 'SUCCESS'
          AND warehouse_name = :warehouse
          AND warehouse_size IS NOT NULL
          AND total_elapsed_time > 0
          AND CONVERT_TIMEZONE('Asia/Tokyo', TO_TIMESTAMP_NTZ(START_TIME)) 
              BETWEEN :begin_str AND :end_str
        GROUP BY ALL
      )
      UNPIVOT (
        sql_count FOR elapsed_time_range IN (
              "1: 0s < ELAPSED_TIME <= 1s",
              "2: 1s < ELAPSED_TIME <= 10s",
              "3: 10s < ELAPSED_TIME <= 60s",
              "4: 60s < ELAPSED_TIME <= 600s",
              "5: 600s < ELAPSED_TIME <= 3600s",
              "6: 3600s < ELAPSED_TIME"

        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      ELAPSED_TIME_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 3) || '%' as PERCENT_SQL_COUNT
    FROM sqlcnt_per_range
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill9(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;



//クエリ実行時間が長いSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill10(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  START_TIME TIMESTAMP_TZ,
  TOTAL_ELAPSED_TIME_S NUMBER
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
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
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    and total_elapsed_time_s > 0 
    order by total_elapsed_time_s desc
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill10(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;


//クエリスキャンサイズの傾向
CREATE OR REPLACE PROCEDURE code_schema.localSpill11(
  warehouse STRING,
  begin_str STRING,
  end_str STRING
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  TOTAL_COUNT_SQL NUMBER,
  SCAN_SIZE_RANGE STRING,
  SQL_COUNT NUMBER,
  PERCENT_SQL_COUNT STRING
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
    WITH sqlcnt_per_scansize AS (
      SELECT * FROM (
        SELECT
          warehouse_name,
          warehouse_size,
          COUNT(*) total_count_sql,
          COUNT(CASE WHEN (BYTES_SCANNED)                > 0  and (BYTES_SCANNED/1024/1024/1024) <= 1   THEN 1 ELSE NULL END) AS "1: 0B < SCAN_SIZE <= 1GB",					
          COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 1  and (BYTES_SCANNED/1024/1024/1024) <= 20  THEN 1 ELSE NULL END) AS "2: 1GB < SCAN_SIZE <= 20GB",   					
          COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 20 and (BYTES_SCANNED/1024/1024/1024) <= 50  THEN 1 ELSE NULL END) AS "3: 20GB < SCAN_SIZE <= 50GB",					
          COUNT(CASE WHEN (BYTES_SCANNED/1024/1024/1024) > 50 THEN 1 ELSE NULL END)                                           AS "4: 50GB < SCAN_SIZE"				
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
        sql_count FOR scan_size_range IN (
            "1: 0B < SCAN_SIZE <= 1GB",					
            "2: 1GB < SCAN_SIZE <= 20GB", 					
            "3: 20GB < SCAN_SIZE <= 50GB",					
            "4: 50GB < SCAN_SIZE"					
        )
      )
    )
    SELECT 
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      TOTAL_COUNT_SQL,
      SCAN_SIZE_RANGE,
      SQL_COUNT,
      round(sql_count / total_count_sql * 100, 1) || '%' as PERCENT_SQL_COUNT
    FROM sqlcnt_per_scansize
  );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill11(STRING,STRING,STRING) TO APPLICATION ROLE func_cost_estimate;


//対象クエリスキャンサイズ範囲のSQL
CREATE OR REPLACE PROCEDURE code_schema.localSpill12(
  warehouse STRING,
  begin_str STRING,
  end_str STRING,
  minNumber NUMBER,
  maxNumber NUMBER
)
RETURNS TABLE(
  WAREHOUSE_NAME STRING,
  WAREHOUSE_SIZE STRING,
  QUERY_ID STRING,
  QUERY_TEXT STRING,
  BYTES_SCANNED_GB NUMBER(10,2),
  TOTAL_ELAPSED_TIME_S NUMBER(10,2)
)
LANGUAGE SQL
AS
DECLARE
  res RESULTSET DEFAULT (
     select
        warehouse_name,
        warehouse_size,
        query_id,
        query_text,
        round(BYTES_SCANNED/1024/1024/1024,2) AS BYTES_SCANNED_GB,
        round(total_elapsed_time / 1000,2) AS total_elapsed_time_s
    from
        snowflake.account_usage.query_history
    where
        execution_status = 'SUCCESS'
    and warehouse_name = :warehouse
    and warehouse_size is not null
    and ROUND(BYTES_SCANNED/1024/1024/1024,2) > :minNumber
    and ROUND(BYTES_SCANNED/1024/1024/1024,2) <= :maxNumber
    and CONVERT_TIMEZONE('Asia/Tokyo',to_timestamp_ntz(START_TIME)) BETWEEN :begin_str AND :end_str
    order by ROUND(BYTES_SCANNED/1024/1024/1024,2) DESC
    );
BEGIN
  RETURN TABLE(res);
END;

GRANT USAGE ON PROCEDURE code_schema.localSpill12(STRING,STRING,STRING,NUMBER,NUMBER) TO APPLICATION ROLE func_cost_estimate;
