-- Create audit log table to track procedure executions
CREATE TABLE IF NOT EXISTS AIRLINE_DWH.CORE.AUDIT_LOG (
    LOG_ID INT AUTOINCREMENT START 1 INCREMENT 1, 
    PROCEDURE_NAME VARCHAR,                   
    ROWS_INSERTED INT,                        
    ROWS_UPDATED INT,                         
    EXECUTION_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP() 
);