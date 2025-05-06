## Welcome to WH_COST_ESTIMATE Native App!

このアプリケーションでは、ウェアハウスごとに過去のクエリを分析できます。
「ローカルスピル」「リモートスピル」「キュー待ち」「トランザクションブロック」などを分析対象として、
各閾値を超過した割合やそのクエリidを出力します。

## Using the application after installation
アプリケーションを正常にインストールした後、以下を実施してください。
- アプリケーションの所有者ロールに切り替える
- 本アプリケーションは以下のアカウントレベルの権限が必要です。  
  ワークシートから対象の権限を付与してください。
  - IMPORTED PRIVILEGES ON SNOWFLAKE DB
    ```
      GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI;
    ```
  - MANAGE WAREHOUSES
    ```
      GRANT MANAGE WAREHOUSES ON ACCOUNT TO APPLICATION STREAMLIT_SQL_NATIVE_APP_MIWANOSHUUHEI;
    ```