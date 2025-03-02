CREATE OR REPLACE STORAGE INTEGRATION S3_DA1
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::864981723182:role/Snowflakes_Analysts_DataAccess'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakedataengineerproject/Datasets_Analysts_uncleaned/') ;

DESC INTEGRATION S3_DA1;