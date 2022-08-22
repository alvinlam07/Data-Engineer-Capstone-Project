CREATE DATABASE creditcard_capstone;

CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH(
    BRANCH_CODE int,
    BRANCH_NAME varchar(255),
    BRANCH_STREET varchar(255),
    BRANCH_CITY varchar(255),
    BRANCH_STATE varchar(255),
    BRANCH_ZIP int,
    BRANCH_PHONE varchar(255),
    LAST_UPDATED timestamp
);

CREATE TABLE IF NOT EXISTS CDW_SAPP_CREDIT_CARD(
    CUST_CC_NO varchar(255),
    TIMEID varchar(255),
    CUST_SSN int,
    BRANCH_CODE int,
    TRANSACTION_TYPE varchar(255),
    TRANSACTION_VALUE float(53),
    TRANSACTION_ID int
);

CREATE TABLE IF NOT EXISTS CDW_SAPP_CUSTOMER(
    SSN int,
    FIRST_NAME varchar(255),
    MIDDLE_NAME varchar(255),
    LAST_NAME varchar(255),
    CREDIT_CARD_NO varchar(255),
    FULL_STREET_ADDRESS varchar(255),
    CUST_CITY varchar(255),
    CUST_STATE varchar(255),
    CUST_COUNTRY varchar(255),
    CUST_ZIP int,
    CUST_PHONE varchar(255),
    CUST_EMAIL varchar(255),
    LAST_UPDATED timestamp
);