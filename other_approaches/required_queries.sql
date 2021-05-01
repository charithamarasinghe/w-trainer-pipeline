''' stage tables create queries and parse_json queries '''

CREATE DATABASE IF NOT EXISTS DB_COURSE_PUBLICATIONS_STAGE;
USE DATABASE  DB_COURSE_PUBLICATIONS_STAGE;


CREATE SCHEMA IF NOT EXISTS SC_COURSE_PUBLICATIONS;
USE SCHEMA SC_COURSE_PUBLICATIONS;


CREATE OR REPLACE TABLE T_COURSE_DATA(
AUTHOR VARIANT,
CONTENT_DURATION VARIANT,
COURSE_ID VARIANT,
COURSE_TITLE VARIANT,
IS_PAID VARIANT,
LEVEL VARIANT,
NUM_LECTURES VARIANT,
NUM_REVIEWS VARIANT,
NUM_SUBCRIBERS VARIANT,
PRICE VARIANT,
PUBLISHED_TIMESTAMP VARIANT,
SUBJECT VARIANT
);


copy into T_COURSE_DATA from (select 
parse_json($1):author,
parse_json($1):content_duration,
parse_json($1):course_id,
parse_json($1):course_title,
parse_json($1):is_paid,
parse_json($1):level,
parse_json($1):num_lectures,
parse_json($1):num_reviews,
parse_json($1):num_subscribers,
parse_json($1):price,
parse_json($1):published_timestamp,
parse_json($1):subject
from @T_COURSE_DATA/course_data.json t) on_error='continue';




'''final table queries '''
CREATE DATABASE IF NOT EXISTS DB_COURSE_PUBLICATIONS;
USE DATABASE  DB_COURSE_PUBLICATIONS;


CREATE SCHEMA IF NOT EXISTS SC_COURSE_PUBLICATIONS;
USE SCHEMA SC_COURSE_PUBLICATIONS;



CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE AS(
SELECT DISTINCT
A.value AS TEMP_COL_1,
B.value as TEMP_COL_2 from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.T_COURSE_DATA T_C_DATA,
lateral flatten(input => T_C_DATA.AUTHOR, OUTER => TRUE) A,
lateral flatten(input => T_C_DATA.COURSE_ID, OUTER => TRUE) B
);



CREATE OR REPLACE TABLE AUTHOR_DIM AS(
  SELECT DISTINCT
TEMP_COL_2 AS COURSE_ID,
TEMP_COL_1 AS AUTHOR_ID,
split_part(TEMP_COL_1,'_',1) as A_AUTHOR_F_NAME,
split_part(TEMP_COL_1,'_',2) as A_AUTHOR_L_NAME,
split_part(TEMP_COL_1,'_',3) as A_AUTHOR_CODE
from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.TEMP_TABLE);




CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE AS(
SELECT DISTINCT
A.value AS TEMP_COL_1,
B.value AS TEMP_COL_2 from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.T_COURSE_DATA T_C_DATA,
lateral flatten(input => T_C_DATA.COURSE_ID, OUTER => TRUE) A,
lateral flatten(input => T_C_DATA.CONTENT_DURATION, OUTER => TRUE) B
);


CREATE OR REPLACE TABLE CONTENT_DURATION_DIM AS(
  SELECT
DISTINCT TEMP_COL_1 AS COURSE_ID,
TEMP_COL_2 AS CONTENT_DURATION
from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.TEMP_TABLE);



CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE AS(
SELECT DISTINCT
A.value AS TEMP_COL_1, 
B.value AS TEMP_COL_2 from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.T_COURSE_DATA T_C_DATA,
lateral flatten(input => T_C_DATA.COURSE_ID, OUTER => TRUE) A,
lateral flatten(input => T_C_DATA.LEVEL, OUTER => TRUE) B) ;

CREATE OR REPLACE TABLE LEVEL_DIM AS(
  SELECT DISTINCT
TEMP_COL_1 AS COURSE_ID,
TEMP_COL_2 AS LEVEL
from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.TEMP_TABLE);




CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE AS(
SELECT DISTINCT
A.value AS TEMP_COL_1, 
B.value AS TEMP_COL_2 from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.T_COURSE_DATA T_C_DATA,
lateral flatten(input => T_C_DATA.COURSE_ID, OUTER => TRUE) A,
lateral flatten(input => T_C_DATA.PUBLISHED_TIMESTAMP, OUTER => TRUE) B) ;

CREATE OR REPLACE TABLE PUBLISHED_TIMESTAMP_DIM AS(
  SELECT DISTINCT
TEMP_COL_1 AS COURSE_ID,
TEMP_COL_2 AS PUBLISHED_TIMESTAMP
from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.TEMP_TABLE);





CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE AS(
SELECT DISTINCT
A.value AS TEMP_COL_1, 
B.value AS TEMP_COL_2 from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.T_COURSE_DATA T_C_DATA,
lateral flatten(input => T_C_DATA.COURSE_ID, OUTER => TRUE) A,
lateral flatten(input => T_C_DATA.SUBJECT, OUTER => TRUE) B) ;


CREATE OR REPLACE TABLE SUBJECT_DIM AS(
  SELECT DISTINCT
TEMP_COL_1 AS COURSE_ID,
TEMP_COL_2 AS SUBJECT
from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.TEMP_TABLE);




CREATE OR REPLACE TEMPORARY TABLE TEMP_TABLE AS(
SELECT DISTINCT
A.value AS TEMP_COL_1, 
B.value AS TEMP_COL_2,
C.value AS TEMP_COL_3,
D.value AS TEMP_COL_4,
E.value AS TEMP_COL_5,
F.value AS TEMP_COL_6,
G.value AS TEMP_COL_7
from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.T_COURSE_DATA T_C_DATA,
lateral flatten(input => T_C_DATA.COURSE_ID, OUTER => TRUE) A,
lateral flatten(input => T_C_DATA.IS_PAID, OUTER => TRUE) B,
lateral flatten(input => T_C_DATA.NUM_SUBCRIBERS, OUTER => TRUE) C,
lateral flatten(input => T_C_DATA.COURSE_TITLE, OUTER => TRUE) D,
lateral flatten(input => T_C_DATA.NUM_REVIEWS, OUTER => TRUE) E,
lateral flatten(input => T_C_DATA.NUM_LECTURES, OUTER => TRUE) F,
lateral flatten(input => T_C_DATA.CONTENT_DURATION, OUTER => TRUE) G
) ;


CREATE OR REPLACE TABLE COURSE_PUBLICATIONS_FACT AS(
SELECT DISTINCT
TEMP_COL_1 AS COURSE_ID,
TEMP_COL_2 AS IS_PAID,
TEMP_COL_3 AS NUM_SUBCRIBERS,
TEMP_COL_4 AS COURSE_TITLE,
TEMP_COL_5 AS NUM_REVIEWS,
TEMP_COL_6 AS NUM_LECTURES,
TEMP_COL_7 AS CONTENT_DURATION
from DB_COURSE_PUBLICATIONS_STAGE.SC_COURSE_PUBLICATIONS.TEMP_TABLE);





