DROP DATABASE IF EXISTS retail_analitycs;

CREATE DATABASE retail_analitycs;

CREATE SCHEMA IF NOT EXISTS public;


DROP TABLE IF EXISTS personal_data;
CREATE TABLE IF NOT EXISTS personal_data
(
    customer_id bigserial PRIMARY KEY,
    customer_name varchar NOT NULL CHECK ( customer_name ~ '^[A-ZА-Я][a-zа-я|-| ]+$'),
    customer_surname varchar NOT NULL CHECK ( customer_surname ~ '^[A-ZА-Я][a-zа-я|-| ]+$' ),
    customer_primary_email varchar NOT NULL UNIQUE CHECK ( customer_primary_email ~ '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'),
    customer_primary_phone varchar NOT NULL UNIQUE CHECK ( customer_primary_phone ~ '^[+]7[0-9]{10}$')
);


DROP TABLE IF EXISTS groups_sku;
CREATE TABLE IF NOT EXISTS groups_sku
(
    group_id BIGSERIAL PRIMARY KEY,
    group_name VARCHAR NOT NULL UNIQUE CHECK ( group_name ~ '^[[:print:]]*$' )
);


DROP TABLE IF EXISTS sku;
CREATE TABLE IF NOT EXISTS sku
(
    sku_id BIGSERIAL PRIMARY KEY,
    sku_name VARCHAR NOT NULL CHECK ( sku.sku_name ~ '^[[:print:]]*$' ),
    group_id BIGINT REFERENCES groups_sku(group_id) ON UPDATE CASCADE ON DELETE CASCADE
);


DROP TABLE IF EXISTS cards;
CREATE TABLE IF NOT EXISTS cards
(
    customer_card_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES personal_data(customer_id) ON UPDATE CASCADE ON DELETE CASCADE
);

DROP TABLE IF EXISTS transactions;
CREATE TABLE IF NOT EXISTS transactions
(
    transaction_id BIGSERIAL PRIMARY KEY,
    customer_card_id BIGINT REFERENCES cards ON UPDATE CASCADE ON DELETE CASCADE,
    transaction_summ NUMERIC NOT NULL CHECK ( transaction_summ > 0 ),
    transaction_datetime TIMESTAMP NOT NULL,
    transaction_store_id BIGINT NOT NULL
);


DROP TABLE IF EXISTS checks;
CREATE TABLE IF NOT EXISTS checks
(
    transaction_id BIGINT REFERENCES transactions ON UPDATE CASCADE ON DELETE CASCADE,
    sku_id bigint REFERENCES sku(sku_id) ON UPDATE CASCADE ON DELETE CASCADE,
    sku_amount numeric NOT NULL CHECK ( sku_amount > 0 ),
    sku_summ numeric NOT NULL CHECK ( sku_summ > 0 ),
    sku_summ_paid numeric NOT NULL CHECK ( sku_summ_paid >= 0 ),
    sku_discount numeric NOT NULL CHECK ( sku_discount >= 0 )
);


DROP TABLE IF EXISTS stores;
CREATE TABLE IF NOT EXISTS stores
(
    transaction_store_id BIGINT,
    sku_id BIGINT REFERENCES sku(sku_id) ON UPDATE CASCADE ON DELETE CASCADE,
    sku_purchase_price NUMERIC NOT NULL CHECK ( sku_purchase_price > 0 ),
    sku_retail_price NUMERIC NOT NULL CHECK ( sku_retail_price > 0 ),
    PRIMARY KEY (transaction_store_id, sku_id)
);


DROP TABLE IF EXISTS date_of_analysis_formation CASCADE;
CREATE TABLE IF NOT EXISTS data_of_analysis_formation
(
    analysis_formation TIMESTAMP
);


CREATE OR REPLACE FUNCTION fnc_trg_transactions_check_transaction_store_id()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$ BEGIN
    IF NEW.transaction_store_id <> (SELECT transaction_store_id FROM stores
                                    WHERE transaction_store_id = NEW.transaction_store_id
                                    LIMIT 1)
        THEN
            RAISE EXCEPTION 'Bad argument transaction_store_id: % is not exists in table stores', NEW.transaction_store_id;
    END IF;
    RETURN NEW;
END; $$;


CREATE OR REPLACE TRIGGER trg_before_insert_transactions
    BEFORE INSERT ON transactions FOR EACH ROW
    EXECUTE PROCEDURE fnc_trg_transactions_check_transaction_store_id();


CREATE OR REPLACE PROCEDURE import(table_name varchar, path varchar, delimiter varchar DEFAULT 'E''\t''')
    LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('COPY %I FROM %L WITH DELIMITER %s', table_name, path, delimiter);
END;
$$;


CREATE OR REPLACE PROCEDURE export(table_name varchar, path varchar, delimiter varchar DEFAULT 'E''\t''')
    LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('COPY %I TO %L WITH DELIMITER %s', table_name, path, delimiter);
END;
$$;

-- /home/student/projects/SQL3_RetailAnalitycs_v1.0-0/datasets/Personal_Data.tsv
-- /Users/johnnake/projects/SQL3_RetailAnalitycs_v1.0-0/datasets/Personal_Data.tsv

SET DATESTYLE TO "ISO, DMY";
SHOW DATESTYLE;

DO
$$
DECLARE
    common_path varchar := '/Users/johnnake/projects/SQL3_RetailAnalitycs_v1.0-0/datasets/';
BEGIN
    CALL import('personal_data', common_path || 'Personal_Data.tsv');
    CALL import('groups_sku', common_path || 'Groups_SKU.tsv');
    CALL import('sku', common_path || 'SKU.tsv');
    CALL import('cards', common_path || 'Cards.tsv');
    CALL import('transactions', common_path || 'Transactions.tsv');
    CALL import('checks', common_path || 'Checks.tsv');
    CALL import('stores', common_path || 'Stores.tsv');
    CALL import('data_of_analysis_formation', common_path || 'Date_Of_Analysis_Formation.tsv');
END
$$;


DO
$$
DECLARE
    common_path varchar := '/Users/johnnake/projects/SQL3_RetailAnalitycs_v1.0-0/datasets/';
BEGIN
    CALL import('personal_data', common_path || 'Personal_Data_Mini.tsv');
    CALL import('groups_sku', common_path || 'Groups_SKU_Mini.tsv');
    CALL import('sku', common_path || 'SKU_Mini.tsv');
    CALL import('cards', common_path || 'Cards_Mini.tsv');
    CALL import('transactions', common_path || 'Transactions_Mini.tsv');
    CALL import('checks', common_path || 'Checks_Mini.tsv');
    CALL import('stores', common_path || 'Stores_Mini.tsv');
    CALL import('data_of_analysis_formation', common_path || 'Date_Of_Analysis_Formation.tsv');
END
$$;


DO
$$
DECLARE
    common_path varchar := '/Users/johnnake/projects/SQL3_RetailAnalitycs_v1.0-0/datasets/';
BEGIN
    CALL export('personal_data', common_path || 'Personal_Data_02.tsv');
    CALL export('groups_sku', common_path || 'Groups_SKU_02.tsv');
    CALL export('sku', common_path || 'SKU_02.tsv');
    CALL export('cards', common_path || 'Cards_02.tsv');
    CALL export('transactions', common_path || 'Transactions_02.tsv');
    CALL export('checks', common_path || 'Checks_02.tsv');
    CALL export('stores', common_path || 'Stores_02.tsv');
    CALL export('data_of_analysis_formation', common_path || 'Date_Of_Analysis_Formation_02.tsv');
END
$$;