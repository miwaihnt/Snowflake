SELECT '<% ctx.entities.hello_snowflake_package %>' AS debug_value;
USE APPLICATION PACKAGE <% ctx.entities.hello_snowflake_package.identifier %>;

CREATE SCHEMA IF NOT EXISTS shared_data;
USE SCHEMA shared_data;
CREATE TABLE IF NOT EXISTS accounts (ID INT, NAME VARCHAR, VALUE VARCHAR);
TRUNCATE TABLE accounts;
INSERT INTO accounts VALUES
  (1, 'Joe', 'Snowflake'),
  (2, 'Nima', 'Snowflake'),
  (3, 'Sally', 'Snowflake'),
  (4, 'Juan', 'Acme');
-- grant usage on the ``ACCOUNTS`` table
GRANT USAGE ON SCHEMA shared_data TO SHARE IN APPLICATION PACKAGE <% ctx.entities.hello_snowflake_package.identifier %>;
GRANT SELECT ON TABLE accounts TO SHARE IN APPLICATION PACKAGE <% ctx.entities.hello_snowflake_package.identifier %>;