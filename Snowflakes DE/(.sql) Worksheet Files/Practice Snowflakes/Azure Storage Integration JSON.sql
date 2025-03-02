CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMATS.AzureJSON
    TYPE = JSON


CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.AzureSI_JSON
    URL = 'azure://01datalaketesting.blob.core.windows.net/data-1/ActivityLog-01.json'
    FILE_FORMAT = MANAGE_DB.FILE_FORMATS.AzureJSON
    STORAGE_INTEGRATION = Azure_DL

LIST @MANAGE_DB.EXTERNAL_STAGES.AzureSI_JSON

SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.AzureSI_JSON

// CREATE TABLE AS YOU FEED IN DATA
CREATE OR REPLACE TABLE MANAGE_DB.PUBLIC.ActLog_1_JSON AS
SELECT
    $1:Correlationid::string as Correlation_Id,
    $1:Eventcategory::string as Event_Category,
    IFNULL($1:Eventinitiatedby::string, 'N/A') as Event_Initiated_By,
    $1:Level::string as Level,
    $1:Operationname::string as Operation_Name,
    $1:Resource::string as "Resource",
    $1:Resourcegroup::string as Resource_Group,
    $1:Resourcetype::string as Resource_Type,
    $1:Status::string as Status,
    $1:Subscription::string as Subscription,
    DATE(TO_TIMESTAMP_NTZ($1:Time::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) as Date,
    TIME(TO_TIMESTAMP_NTZ($1:Time::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) as Time
FROM @MANAGE_DB.EXTERNAL_STAGES.AzureSI_JSON

SELECT * FROM MANAGE_DB.PUBLIC.ActLog_1_JSON 


             -- OR --


// CREATE A TABLE IS DESTINATION 
CREATE OR REPLACE TABLE MANAGE_DB.PUBLIC.ActLog_1_JSON(
    Correlation_Id STRING,
    Event_Category STRING,
    Event_Initiated_By STRING,
    Level STRING,
    Operation_Name STRING,
    "Resource" STRING,
    Resource_Group STRING,
    Resource_Type STRING,
    Status STRING,
    Subscription STRING,
    "Date" DATE,
    "Time" TIME
);

//COPY INTO THE DESTINATION TABLE
COPY INTO MANAGE_DB.PUBLIC.ACTLOG_1_JSON
    FROM (
        SELECT
        $1:Correlationid::string as Correlation_Id,
        $1:Eventcategory::string as Event_Category,
        IFNULL($1:Eventinitiatedby::string, 'N/A') as Event_Initiated_By,
        $1:Level::string as Level,
        $1:Operationname::string as Operation_Name,
        $1:Resource::string as "Resource",
        $1:Resourcegroup::string as Resource_Group,
        $1:Resourcetype::string as Resource_Type,
        $1:Status::string as Status,
        $1:Subscription::string as Subscription,
        DATE(TO_TIMESTAMP_NTZ($1:Time::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) as Date,
        TIME(TO_TIMESTAMP_NTZ($1:Time::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) as Time
        FROM @MANAGE_DB.EXTERNAL_STAGES.AzureSI_JSON
        );

SELECT * FROM MANAGE_DB.PUBLIC.ActLog_1_JSON 