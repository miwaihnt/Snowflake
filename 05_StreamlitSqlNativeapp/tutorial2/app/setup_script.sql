CREATE APPLICATION ROLE IF NOT EXISTS cost_streamlit_public;
CREATE SCHEMA IF NOT EXISTS cost_estimate.cost_estimate;
GRANT USAGE ON SCHEMA cost_estimate.cost_estimate TO APPLICATION ROLE cost_streamlit_public;
CREATE STREAMLIT IF NOT EXISTS cost_estimate.cost_estimate
  FROM '/'
  MAIN_FILE = 'streamlit_app.py';
GRANT USAGE ON STREAMLIT cost_estimate.cost_estimate TO APPLICATION ROLE cost_streamlit_public;
