-- Create database (optional)
DROP DATABASE IF EXISTS transactions_cc;
CREATE DATABASE IF NOT EXISTS transactions_cc;
USE transactions_cc;

-- Create credit_card_types table
CREATE TABLE credit_card_types (
    card_type_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(10),
    credit_score_min INT,
    credit_score_max INT,
    credit_limit_min INT,
    credit_limit_max INT,
    annual_fee INT,
    rewards_rate FLOAT
);

-- Create customers table
CREATE TABLE customers (
    customer_id INT NOT NULL PRIMARY KEY,
    name LONGTEXT,
    phone_number LONGTEXT,
    address LONGTEXT,
    email LONGTEXT,
    credit_score BIGINT,
    annual_income DOUBLE
);

-- Create cards table
CREATE TABLE cards (
    card_id INT NOT NULL PRIMARY KEY,
    customer_id INT,
    card_type_id INT,
    card_number LONGTEXT,
    expiration_date LONGTEXT,
    credit_limit DOUBLE,
    current_balance DOUBLE,
    issue_date DATE
);

-- Create transactions table
CREATE TABLE transactions (
    transaction_id INT NOT NULL PRIMARY KEY,
    card_id INT,
    merchant_name VARCHAR(50),
    timestamp DATETIME,
    amount FLOAT,
    location VARCHAR(100),
    transaction_type VARCHAR(15),
    related_transaction_id INT
);
