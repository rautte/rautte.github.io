ALTER DATABASE FIRST_DB RENAME TO OUR_FIRST_DB;

CREATE TABLE "OUR_FIRST_DB"."PUBLIC"."LOAN_PAYMENT"(
"Loan_ID" STRING,
"loan_status" STRING,
"Principal" STRING,
"terms" STRING,
"effective_date" STRING,
"due_date" STRING,
"paid_off_time" STRING,
"past_due_days" STRING,
"age" STRING,
"education" STRING,
"Gender" STRING
);

USE DATABASE OUR_FIRST_DB
USE SCHEMA PUBLIC

COPY INTO LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    FILE_FORMAT = (TYPE = CSV
                    FIELD_DELIMITER = ','
                    SKIP_HEADER = 1);

-- DELETE FROM LOAN_PAYMENT
-- DROP TABLE LOAN_PAYMENT

SELECT * FROM LOAN_PAYMENT
