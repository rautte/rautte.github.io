// Create storage integration object with aws s3
CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::864981723182:role/aws_snowfloake_access'
    STORAGE_ALLOWED_LOCATIONS = ('s3://awsbucketsnowf/CSV_Files/', 's3://awsbucketsnowf/JSON_Files/')
    COMMENT = 'This an optional comment' ;

// See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int;

// Create table TEMP to feed in variant type data of 1 column
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.TEMP(
    RAW VARIANT);

// Check column names in the table
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
// Insert into table TEMP to feed in variant type [Column_Names] into 1 column RAW
INSERT INTO OUR_FIRST_DB.PUBLIC.TEMP (RAW)
SELECT TO_VARIANT('C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT');
// Insert into table TEMP to feed in variant type [Column_Data] into 1 column RAW
INSERT INTO OUR_FIRST_DB.PUBLIC.TEMP (RAW)
SELECT TO_VARIANT(CONCAT_WS(',', C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT)) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
// Check final table to 1 column with comma seperation
SELECT * FROM OUR_FIRST_DB.PUBLIC.TEMP;


// Create stage object with integration object & file format object
CREATE OR REPLACE stage MANAGE_DB.external_stages.FromTable_csv
    URL = 's3://awsbucketsnowf/CSV_Files/'
    STORAGE_INTEGRATION = s3_int

COPY INTO @MANAGE_DB.external_stages.FromTable_csv
FROM OUR_FIRST_DB.PUBLIC.TEMP