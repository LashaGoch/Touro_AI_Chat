--####################################################
-- Building a Simple AI Chat using Touro University & 
-- USCIS PDF Files to answer questions about OPT
--####################################################

--####################################################
-- STEP 0: Create a database and schema in Snowflake
--####################################################

-- 1. Create a new database
CREATE DATABASE TOURO;

-- 2. Switch to that database
USE DATABASE TOURO;

-- 3. Create a new schema inside it
CREATE SCHEMA DEMO;

-- 4. (Optional) Switch to that schema
USE SCHEMA DEMO;

--####################################################
-- STEP 1: Create a stage and upload documents
--####################################################

CREATE STAGE TOURO.DEMO.RAG_DEMO
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY = ( ENABLE = true );

//DROP STAGE SNOWFLAKE_LEARNING_DB.PUBLIC.RAG_DEMO;


-- List the documents from the stage
USE TOURO.DEMO;

  ls @RAG_DEMO;


--####################################################
-- STEP 2: Read PDF from the Stage: 
-- Use the function SNOWFLAKE.CORTEX.PARSE_DOCUMENT 
-- to read the PDF documents directly from the staging area
--####################################################

CREATE OR REPLACE TEMPORARY TABLE RAG_RAW_TEXT AS
SELECT 
    RELATIVE_PATH,
    SIZE,
    FILE_URL,
    build_scoped_file_url(@RAG_DEMO, relative_path) as scoped_file_url,
    TO_VARCHAR (
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT (
            '@RAG_DEMO',
            RELATIVE_PATH,
            {'mode': 'LAYOUT'} ):content
        ) AS EXTRACTED_LAYOUT 
FROM 
    DIRECTORY('@RAG_DEMO');

-- Check the extracted text from the PDF
SELECT * FROM RAG_RAW_TEXT limit 10;

--####################################################
-- STEP 3: Create the table where we are going to 
-- store the chunks for each PDF
--####################################################

CREATE OR REPLACE TABLE TOURO.DEMO.RAG_CHUNKS ( 
    RELATIVE_PATH VARCHAR(16777216),   -- Relative path to the PDF file
    SIZE NUMBER(38,0),                 -- Size of the PDF
    FILE_URL VARCHAR(16777216),        -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216),           -- Piece of text
    CHUNK_INDEX INTEGER                -- Index for the text
);


--####################################################
-- STEP 4: Create chunks. Split the text into shorter strings. 
-- Use the function SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER
--####################################################

INSERT INTO TOURO.DEMO.RAG_CHUNKS (relative_path, size, file_url,
                            scoped_file_url, chunk, chunk_index)

    SELECT relative_path, 
            size,
            file_url, 
            scoped_file_url,
            c.value::TEXT as chunk,
            c.INDEX::INTEGER as chunk_index
            
    FROM 
        RAG_RAW_TEXT,
        LATERAL FLATTEN( input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
              EXTRACTED_LAYOUT,
              'markdown',
              1512,             -- CHUNK_SIZE
              256,              -- CHUNK_OVERLAP
              ['\n\n', '\n', ' ', '']
           )) c;


-- Check the data
SELECT * FROM TOURO.DEMO.RAG_CHUNKS limit 50;

--####################################################
-- STEP 6: Cleanup. Delete small chunks with unnecessary info
--####################################################
SELECT * FROM TOURO.DEMO.RAG_CHUNKS WHERE len(chunk) < 30; 

DELETE FROM TOURO.DEMO.RAG_CHUNKS WHERE LENGTH(chunk) < 30;


--####################################################
-- STEP 7: Create Cortex Search Service
--####################################################

CREATE OR REPLACE CORTEX SEARCH SERVICE TOURO.DEMO.RAG_DEMO
    ON CHUNK
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
    AS (
        SELECT *
        FROM TOURO.DEMO.RAG_CHUNKS
    );
    
    
--DROP CORTEX SEARCH SERVICE SANDBOX.TEAM2.RAG_DEMO;



