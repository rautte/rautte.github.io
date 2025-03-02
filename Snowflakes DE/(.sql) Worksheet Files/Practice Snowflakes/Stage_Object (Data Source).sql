// Database to manage stage objects, fileformats etc.

CREATE OR REPLACE DATABASE MANAGE_DB;
CREATE OR REPLACE SCHEMA external_stages;

-- ************  START - ACTUAL WITH PROPER PERMISSIONS IN PLACE - START  *************

-- // Creating external stage

-- CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
--     url='s3://bucketsnowflakes3'
--     credentials=(aws_key_id='ABCD_DUMMY_ID' aws_secret_key='1234abcd_key');


-- // Description of external stage

-- DESC STAGE MANAGE_DB.external_stages.aws_stage; 
    
    
-- // Alter external stage   

-- ALTER STAGE aws_stage
--     SET credentials=(aws_key_id='XYZ_DUMMY_ID' aws_secret_key='987xyz');

-- ************  END - ACTUAL WITH PROPER PERMISSIONS IN PLACE - END  *************
    
    
// Publicly accessible staging area    

CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3://bucketsnowflakes3';
    

// List files in stage

LIST @aws_stage;


// Creating ORDERS table

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));


//Load data using copy command

COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    -- FILES = ('OrderDetails.csv')
    pattern='.*Order.*';

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS;

