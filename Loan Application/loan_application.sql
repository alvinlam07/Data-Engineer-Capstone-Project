CREATE TABLE IF NOT EXISTS CDW_SAPP_LOAN_APPLICATION(
    Application_ID varchar(255) PRIMARY KEY,
    Application_Status varchar(255),
    Credit_History bigint,
    Dependents varchar(255),
    Education varchar(255),
    Gender varchar(255),
    Income varchar(255),
    Married varchar(255),
    Property_Area varchar(255),
    Self_Employed varchar(255)
);