//リソース作成
use role securityadmin;
create role git_admin;
use role sysadmin;
create database git_DB;
create schema git_schema;

//権限付与
grant create secret on schema git_db.git_schema to role git_admin;
grant usage on database git_db to role git_admin;
grant usage on schema git_db.git_schema  to role git_admin;
show grants to role git_admin;
grant role git_admin to user miwasu;

use role git_admin;
use database git_db;
use schema git_db.git_schema;

//シークレットの作成
create or replace secret git_secret
    type = password
    username ='git_snow'
    password = '<シークレットの入力>';

//権限付与
use role securityadmin;
use role accountadmin;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE git_admin;
use role securityadmin;
use role git_admin;

//api統合を作成
create or replace api integration git_api_integration
    api_provider = git_https_api
    API_ALLOWED_PREFIXES = ('<リポジトリURL>')
    ALLOWED_AUTHENTICATION_SECRETS = ('git_secret')
    ENABLED = TRUE;

//権限付与
use role securityadmin;
grant create git repository on schema git_db.git_schema to role git_admin;
use role git_admin;

//内部リポジトリの作成
create or replace git repository snowflake_git 
    API_INTEGRATION = git_api_integration
    git_credentials = git_secret
    ORIGIN = '<リモートリポジトリ>';

alter git repository snowflake_git fetch;
    
