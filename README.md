# PLSQL_development_project
-- Schema Design
-- Table to store exchange data
CREATE TABLE exchange_data (
    user_id       NUMBER,
    stock_id      VARCHAR2(20),
    stock_name    VARCHAR2(100),
    stock_count   NUMBER,
    PRIMARY KEY (user_id, stock_id)
);

-- Table to store depository data
CREATE TABLE depository_data (
    user_id       NUMBER,
    stock_id      VARCHAR2(20),
    stock_name    VARCHAR2(100),
    stock_count   NUMBER,
    PRIMARY KEY (user_id, stock_id)
);

-- Table to log comparison results
CREATE TABLE comparison_results (
    id            NUMBER  PRIMARY KEY,
    user_id       NUMBER,
    stock_id      VARCHAR2(20),
    exchange_count NUMBER,
    depository_count NUMBER,
    status        VARCHAR2(20), -- MATCH or MISMATCH
    comparison_date DATE DEFAULT SYSDATE
);

CREATE SEQUENCE comparison_id_seq
       START WITH 1
   INCREMENT BY 1
   NOCACHE;

CREATE OR REPLACE TRIGGER trg_comparison_id
BEFORE INSERT ON comparison_results
FOR EACH ROW
BEGIN
    IF :NEW.id IS NULL THEN
        :NEW.id := comparison_id_seq.NEXTVAL;
    END IF;
END;
/

-- Package Specification
CREATE OR REPLACE PACKAGE stock_comparison_pkg AS
    PROCEDURE process_files(exchange_filepath IN VARCHAR2, depository_filepath IN VARCHAR2);
    PROCEDURE compare_data;
END stock_comparison_pkg;
/
CREATE OR REPLACE PACKAGE BODY stock_comparison_pkg AS

    -- Procedure to process CSV files and load data into tables
    PROCEDURE process_files(exchange_filepath IN VARCHAR2, depository_filepath IN VARCHAR2) IS
    BEGIN
        -- Load exchange data
        EXECUTE IMMEDIATE 'DELETE FROM exchange_data';
        EXECUTE IMMEDIATE 'DELETE FROM depository_data';

        -- Create or replace directory for exchange and depository data
        EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY exchange_dir AS ''' || exchange_filepath || '''';
        EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY depository_dir AS ''' || depository_filepath || '''';

        -- Create External Table for exchange data
        EXECUTE IMMEDIATE '
        CREATE TABLE exchange_data_external (
            user_id VARCHAR2(100),
            stock_id VARCHAR2(100),
            stock_name VARCHAR2(100),
            stock_count NUMBER
        )
        ORGANIZATION EXTERNAL (
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY exchange_dir
            ACCESS PARAMETERS (
                RECORDS DELIMITED BY NEWLINE
                FIELDS TERMINATED BY '',''
                MISSING FIELD VALUES ARE NULL
            )
            LOCATION (''exchange_data.csv'')
        )';

        -- Create External Table for depository data
        EXECUTE IMMEDIATE '
        CREATE TABLE depository_data_external (
            user_id VARCHAR2(100),
            stock_id VARCHAR2(100),
            stock_name VARCHAR2(100),
            stock_count NUMBER
        )
        ORGANIZATION EXTERNAL (
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY depository_dir
            ACCESS PARAMETERS (
                RECORDS DELIMITED BY NEWLINE
                FIELDS TERMINATED BY '',''
                MISSING FIELD VALUES ARE NULL
            )
            LOCATION (''depository_data.csv'')
        )';

        -- Insert data into exchange_data and depository_data tables
        EXECUTE IMMEDIATE 'INSERT INTO exchange_data SELECT * FROM exchange_data_external';
        EXECUTE IMMEDIATE 'INSERT INTO depository_data SELECT * FROM depository_data_external';

    END;

    -- Procedure to compare data between tables
    PROCEDURE compare_data IS
    BEGIN
        -- Clear previous comparison results
        DELETE FROM comparison_results;

        -- Insert matches and mismatches
        INSERT INTO comparison_results (user_id, stock_id, exchange_count, depository_count, status)
        SELECT e.user_id, e.stock_id, e.stock_count, d.stock_count,
               CASE WHEN e.stock_count = d.stock_count THEN 'MATCH' ELSE 'MISMATCH' END
        FROM exchange_data e
        FULL OUTER JOIN depository_data d
        ON e.user_id = d.user_id AND e.stock_id = d.stock_id
        WHERE e.user_id IS NOT NULL OR d.user_id IS NOT NULL;

        COMMIT;
    END;

END stock_comparison_pkg;
/

---before running the below script need to create the folders as D:\PLSQL\exchange location for exchange_data.csv file and 'D:\PLSQL\depository for directory_data.csv file
--you need to save the given data in .csv format.
-- Test Cases
BEGIN
    stock_comparison_pkg.process_files('D:\PLSQL\exchange', 'D:\PLSQL\depository'); 
    stock_comparison_pkg.compare_data;
END;
/
-- Query results
SELECT * FROM comparison_results;



