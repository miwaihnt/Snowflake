DROP STREAMLIT IF EXISTS streamlit_app_schema.wh_cost_estimate;
CREATE APPLICATION ROLE IF NOT EXISTS app_streamlit_public;
CREATE SCHEMA IF NOT EXISTS streamlit_app_schema;
GRANT USAGE ON SCHEMA streamlit_app_schema TO APPLICATION ROLE app_streamlit_public;
CREATE STREAMLIT IF NOT EXISTS streamlit_app_schema.wh_cost_estimate
  FROM '/'
  MAIN_FILE = 'streamlit_app.py';
GRANT USAGE ON STREAMLIT streamlit_app_schema.wh_cost_estimate TO APPLICATION ROLE app_streamlit_public;
