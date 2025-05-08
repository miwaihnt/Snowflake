CREATE APPLICATION ROLE IF NOT EXISTS app_public;
CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_public;

CREATE STREAMLIT IF NOT EXISTS code_schema.hello_snowflake_streamlit
  FROM '/'
  MAIN_FILE = '/hello_snowflake.py'
;
GRANT USAGE ON STREAMLIT code_schema.hello_snowflake_streamlit TO APPLICATION ROLE app_public;

