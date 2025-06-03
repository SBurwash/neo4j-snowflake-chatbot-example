-- See https://github.com/neo4j-product-examples/snowflake-graph-analytics/blob/main/entity-resolution-fraud-detection/Communities_in_P2P_Quickstart_with_Louvain_and_PageRank.ipynb

use role accountadmin;

USE SCHEMA STEPHANE_SANDBOX.PUBLIC;

-- Create a consumer role for users and admins of the Neo4j Graph Analytics application
CREATE ROLE IF NOT EXISTS gds_user_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_user TO ROLE gds_user_role;

CREATE DATABASE ROLE IF NOT EXISTS gds_db_role;
GRANT DATABASE ROLE gds_db_role TO ROLE gds_user_role;
GRANT DATABASE ROLE gds_db_role TO APPLICATION neo4j_graph_analytics;

-- Grant access to consumer data
GRANT USAGE ON DATABASE STEPHANE_SANDBOX TO ROLE gds_user_role;
GRANT USAGE ON SCHEMA STEPHANE_SANDBOX.PUBLIC TO ROLE gds_user_role;

-- Required to read tabular data into a graph
GRANT SELECT ON ALL TABLES IN DATABASE STEPHANE_SANDBOX TO DATABASE ROLE gds_db_role;

-- Ensure the consumer role has access to created tables/views
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT CREATE TABLE ON SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT CREATE VIEW ON SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;

-- Compute and warehouse access
GRANT USAGE ON WAREHOUSE NEO4J_GRAPH_ANALYTICS_APP_WAREHOUSE TO APPLICATION neo4j_graph_analytics;

use role gds_user_role;


CREATE OR REPLACE TABLE STEPHANE_SANDBOX.public.P2P_AGG_TRANSACTIONS (
	SOURCENODEID NUMBER(38,0),
	TARGETNODEID NUMBER(38,0),
	TOTAL_AMOUNT FLOAT
) AS
SELECT sourceNodeId, targetNodeId, SUM(transaction_amount) AS total_amount
FROM STEPHANE_SANDBOX.public.P2P_TRANSACTIONS
GROUP BY sourceNodeId, targetNodeId;
SELECT * FROM STEPHANE_SANDBOX.public.P2P_AGG_TRANSACTIONS;

CREATE OR REPLACE VIEW p2p_users_vw (nodeId) AS
    SELECT DISTINCT p2p_demo.public.p2p_users.NODEID as nodeid
    FROM p2p_users;