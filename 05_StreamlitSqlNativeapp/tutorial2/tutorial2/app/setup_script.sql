CREATE APPLICATION ROLE IF NOT EXISTS teat_pac_app;
CREATE SCHEMA IF NOT EXISTS test1;
GRANT USAGE ON SCHEMA test1 TO teat_pac_app;
CREATE STREAMLIT IF NOT EXISTS teat_pac_app
  FROM '/'
  MAIN_FILE = 'streamlit_app.py';
GRANT USAGE ON STREAMLIT teat_pac_app.teat_pac_app TO APPLICATION ROLE teat_pac_app;

