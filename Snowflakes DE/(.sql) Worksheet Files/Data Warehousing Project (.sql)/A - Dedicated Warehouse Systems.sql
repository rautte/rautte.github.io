// AU1 - CHANGE OR USE ROLE //
USE ROLE SYSADMIN;


// AU2 - WAREHOUSE // CREATE DEDICATED WAREHOUSE FOR DATA ANALYSTS
CREATE OR REPLACE WAREHOUSE SnowProject_DA
    WAREHOUSE_TYPE = STANDARD --'SNOWPARK-OPTIMIZED'
    WAREHOUSE_SIZE = XSMALL --'XS(1 credit) | S(2 credits) | M(4 credits) | L(8 credits) | XL(16 credits) ...'
    SCALING_POLICY = 'Standard' --'Economy'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5
    AUTO_SUSPEND = 300  -- automatically suspend the virtual warehouse after 600 seconds of not being used
    AUTO_RESUME = TRUE 
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'This is a virtual warehouse of size X-SMALL that can be used to process queries.';


// AU3 - USE | DESCRIBE - WAREHOUSE //

USE WAREHOUSE SnowProject_DA; 
DESC WAREHOUSE SnowProject_DA;


 
// AD1 - ALTER | SET | DROP | USE | DESC - COMMANDS FOR WAREHOUSE //

// SET WAREHOUSE PARAMETERS
-- ALTER WAREHOUSE SnowProject_DA 
--     SET SCALING_POLICY = 'Economy'
--     AUTO_SUSPEND = 600 ;

// ALTER WAREHOUSE OBJECT
-- ALTER WAREHOUSE SnowProject_DA SUSPEND; 
-- ALTER WAREHOUSE SnowProject_DA RESUME; 
-- ALTER WAREHOUSE SnowProject_DA ABORT ALL QUERIES; 
-- ALTER WAREHOUSE SnowProject_DA RENAME TO ProjectDE_01; 

// DROP WAREHOUSE
-- DROP WAREHOUSE SnowProject_DA; 