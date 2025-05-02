//必要リソースの準備
create database doc_ai_db;
CREATE SCHEMA doc_ai_db.doc_ai_schema;
create warehouse doc_ai_wh
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

//role作成
use role accountadmin;
CREATE ROLE doc_ai_role;

//db roleの権限を付与
GRANT DATABASE ROLE SNOWFLAKE.DOCUMENT_INTELLIGENCE_CREATOR TO ROLE doc_ai_role;
//whの権限をふよ
GRANT USAGE, OPERATE ON WAREHOUSE doc_ai_wh TO ROLE doc_ai_role;

//作成したデータベースとスキーマを使用する権限を に付与しますdoc_ai_role。
GRANT USAGE ON DATABASE doc_ai_db TO ROLE doc_ai_role;
GRANT USAGE ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;

//ステージ作成権限のふよ
grant create stage on schema doc_ai_db.doc_ai_schema to role doc_ai_role;

//モデル作成権限の付与
GRANT CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;
GRANT CREATE MODEL ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;

//タスク、ストリームを許可
GRANT CREATE STREAM, CREATE TABLE, CREATE TASK, CREATE VIEW ON SCHEMA doc_ai_db.doc_ai_schema TO ROLE doc_ai_role;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE doc_ai_role;

//userに付与
GRANT ROLE doc_ai_role TO USER miwasu;

//内部ステージ作成
create or replace stage resume_pdf_stage
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

//DocumentAIの実行
select
    RELATIVE_PATH as file_name,
    RESUME_EN!PREDICT(GET_PRESIGNED_URL('@resume_pdf_stage',RELATIVE_PATH),1) as json_content
from 
    DIRECTORY(@resume_pdf_stage);

