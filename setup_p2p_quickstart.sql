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
GRANT CREATE STAGE ON SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA STEPHANE_SANDBOX.PUBLIC TO DATABASE ROLE gds_db_role;

GRANT USAGE ON WAREHOUSE NEO4J_GRAPH_ANALYTICS_APP_WAREHOUSE TO ROLE gds_user_role;
GRANT OPERATE ON WAREHOUSE NEO4J_GRAPH_ANALYTICS_APP_WAREHOUSE TO ROLE gds_user_role;


use role gds_user_role;
USE WAREHOUSE NEO4J_GRAPH_ANALYTICS_APP_WAREHOUSE;
USE DATABASE STEPHANE_SANDBOX;
USE SCHEMA STEPHANE_SANDBOX.public;

CREATE OR REPLACE STAGE raw_data DIRECTORY = (ENABLE = TRUE);



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
    SELECT DISTINCT STEPHANE_SANDBOX.public.p2p_users.NODEID as nodeid
    FROM p2p_users;


GRANT SELECT ON TABLE STEPHANE_SANDBOX.PUBLIC.p2p_users_vw TO APPLICATION neo4j_graph_analytics;
GRANT SELECT ON TABLE STEPHANE_SANDBOX.PUBLIC.P2P_AGG_TRANSACTIONS TO APPLICATION neo4j_graph_analytics;
GRANT SELECT ON TABLE STEPHANE_SANDBOX.PUBLIC.p2p_users_vw_lou TO APPLICATION neo4j_graph_analytics;

CALL neo4j_graph_analytics.graph.louvain('CPU_X64_XS', {
    'project': {
        'nodeTables': ['STEPHANE_SANDBOX.public.p2p_users_vw'],
        'relationshipTables': {
            'STEPHANE_SANDBOX.public.P2P_AGG_TRANSACTIONS': {
                'sourceTable': 'STEPHANE_SANDBOX.public.p2p_users_vw',
                'targetTable': 'STEPHANE_SANDBOX.public.p2p_users_vw',
                'orientation': 'NATURAL'
            }
        }
    },
    'compute': { 'consecutiveIds': true, 'relationshipWeightProperty':'TOTAL_AMOUNT'},
    'write': [{
        'nodeLabel': 'p2p_users_vw',
        'outputTable': 'STEPHANE_SANDBOX.public.p2p_users_vw_lou'
    }]
});

select community, COUNT(*) AS community_size, 
from STEPHANE_SANDBOX.public.p2p_users_vw_lou
group by community
order by community_size desc;