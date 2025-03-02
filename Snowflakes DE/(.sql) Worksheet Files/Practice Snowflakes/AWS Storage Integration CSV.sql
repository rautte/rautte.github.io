// Create storage integration object

create or replace storage integration s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::864981723182:role/aws_snowfloake_access'
    STORAGE_ALLOWED_LOCATIONS = ('s3://awsbucketsnowf/CSV_Files/', 's3://awsbucketsnowf/JSON_Files/')
    COMMENT = 'This an optional comment' ;
   
   
// See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int;


// Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.movie_titles (
  show_id STRING,
  type STRING,
  title STRING,
  director STRING,
  cast STRING,
  country STRING,
  date_added STRING,
  release_year STRING,
  rating STRING,
  duration STRING,
  listed_in STRING,
  description STRING )


// Create Schema
CREATE SCHEMA MANAGE_DB.file_formats


// Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE    
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
    
    
 // Create stage object with integration object & file format object
CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_folder
    URL = 's3://awsbucketsnowf/CSV_Files/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat --Can be done here too



// Use Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.movie_titles
    FROM @MANAGE_DB.external_stages.csv_folder
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat
      
    
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.movie_titles