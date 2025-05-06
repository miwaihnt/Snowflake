## ReadMe
(base) miwanoshuuhei@miwambp 05_StreamlitSqlNativeapp % snow init --template app_basic tutorial
Project identifier [my_native_app_project]: streamlit_sql_native_app
Initialized the new project in tutorial

- show warehousesやaccount_usageが見れない
  - account_usageを実行可能なroleにスイッチする？
  - application roleにaccount_usageの権限を付与する？
  - NativeAppのロール権限を確認
    - SHOW APPLICATION ROLES IN APPLICATION STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI;
    created_on	name	owner	comment	owner_role_type
    2025-04-30 13:26:35.756 +0000	APP_STREAMLIT_PUBLIC	STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI		APPLICATION
  
    - SHOW GRANTS TO APPLICATION ROLE APP_STREAMLIT_PUBLIC;
    created_on	privilege	granted_on	name	granted_to	grantee_name	grant_option	granted_by
    2025-04-30 13:26:35.765 +0000	USAGE	DATABASE	STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI	APPLICATION_ROLE	APP_STREAMLIT_PUBLIC	false	
    2025-04-30 13:26:36.080 +0000	USAGE	SCHEMA	STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI.STREAMLIT_APP_SCHEMA	APPLICATION_ROLE	APP_STREAMLIT_PUBLIC	false	STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI
    2025-05-06 01:38:36.392 +0000	USAGE	STREAMLIT	STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI.STREAMLIT_APP_SCHEMA.WH_COST_ESTIMATE	APPLICATION_ROLE	APP_STREAMLIT_PUBLIC	false	STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI

  - 現在、AccountAdminからは、STREAMLIT_APP_SCHEMA配下を閲覧不可（権限か？）

  - 解決方法
    - manifest.ymlにpriviledeを宣言
      IMPORTED PRIVILEGES ON SNOWFLAKE DB:
    - app run後に、ワークシートから以下を実行
      //公式ドキュメントにある通り、アプリケーションに対しての権限付与が必要
      https://docs.snowflake.com/en/developer-guide/native-apps/requesting-privs
      //権限セット
      USE ROLE ACCOUNTADMIN;
      //アプリケーションロールの権限を確認
      SHOW GRANTS TO APPLICATION ROLE APP_STREAMLIT_PUBLIC;
      //applicationにSNOWFLAKE DBの権限付与
      GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI;
      //Manage warehouse権限を割り当て
      GRANT MANAGE WAREHOUSES ON ACCOUNT TO APPLICATION STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI;
