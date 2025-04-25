//managedGrants権限がroleの属性を変更できないのか？を検証

//準備用
use role securityadmin;
use role accountadmin;
use role sysadmin;
use warehouse compute_wh;

//各種roleの作成
create role managed_grants_role; //子ロール
create role terraform_role;　//操作用ロール
create role terraform_role2; //操作用ロール2

//ユーザに割り当て
grant role managed_grants_role to user miwasu;
grant role  terraform_role to user miwasu;
grant role  terraform_role2 to user miwasu;


//リソース作成（使わんかも
create database managed_grants_test;
show roles like 'managed_grants_role';
show grants to role securityadmin;

//managed grants権限なしでは属性を変更できないことを確認
show grants to role terraform_role;
use role terraform_role;
alter role managed_grants_role set comment = 'good';

//managed grants権限ありでも属性が変更できないことを確認
grant manage grants on account to role terraform_role;
grant manage grants on database to role terraform_role;
show grants to role terraform_role;
use role terraform_role;
alter role managed_grants_role set comment = 'good';
grant usage on database cortex_db to role managed_grants_role;


//ownerになれば属性が変更できることを確認
grant ownership on role managed_grants_role to role terraform_role;
use role terraform_role;
alter role managed_grants_role set comment = 'good';

//managed grantsを付与したロールを使って自身にDB使用ロールを付与
grant usage on database row_security; to role terraform_role;
use database row_security;


