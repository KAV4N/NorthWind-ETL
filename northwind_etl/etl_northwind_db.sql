-- Vytvorenie databázy
CREATE DATABASE IF NOT EXISTS EAGLE_NorthWind_DB;
-- Vytvorenie schémy pre staging tabuľky
CREATE SCHEMA IF NOT EXISTS EAGLE_NorthWind_DB.staging;

USE SCHEMA EAGLE_NorthWind_DB.staging;

DROP TABLE IF EXISTS order_details_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS suppliers_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS shippers_staging;


-- Vytvorenie staging tabuliek
CREATE TABLE categories_staging (      
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR(25),
    description VARCHAR(255)
);

CREATE TABLE customers_staging (      
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(50),
    contact_name VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(20),
    postal_code VARCHAR(10),
    country VARCHAR(15)
);

CREATE TABLE employees_staging (
    employee_id INTEGER PRIMARY KEY,
    last_name VARCHAR(15),
    first_name VARCHAR(15),
    birth_date DATETIME,
    photo VARCHAR(25),
    notes VARCHAR(1024)
);

CREATE TABLE shippers_staging (
    shipper_id INTEGER PRIMARY KEY,
    shipper_name VARCHAR(25),
    phone VARCHAR(15)
);

CREATE TABLE suppliers_staging (
    supplier_id INTEGER PRIMARY KEY,
    supplier_name VARCHAR(50),
    contact_name VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(20),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(15)
);

CREATE TABLE products_staging (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(50),
    supplier_id INTEGER,
    category_id INTEGER,
    unit VARCHAR(25),
    price NUMERIC,
    FOREIGN KEY (category_id) REFERENCES categories_staging (category_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers_staging (supplier_id)
);

CREATE TABLE orders_staging (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    employee_id INTEGER,
    order_date DATETIME,
    shipper_id INTEGER,
    FOREIGN KEY (employee_id) REFERENCES employees_staging (employee_id),
    FOREIGN KEY (customer_id) REFERENCES customers_staging (customer_id),
    FOREIGN KEY (shipper_id) REFERENCES shippers_staging (shipper_id)
);

CREATE TABLE order_details_staging (
    order_detail_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    FOREIGN KEY (order_id) REFERENCES orders_staging (order_id),
    FOREIGN KEY (product_id) REFERENCES products_staging (product_id)
);

TRUNCATE TABLE suppliers_staging;
TRUNCATE TABLE shippers_staging;
TRUNCATE TABLE products_staging;
TRUNCATE TABLE orders_staging;
TRUNCATE TABLE order_details_staging;
TRUNCATE TABLE employees_staging;
TRUNCATE TABLE customers_staging;
TRUNCATE TABLE categories_staging;


-- Vytvorenie stage pre .csv súbory
CREATE OR REPLACE STAGE northwind_stage;

COPY INTO suppliers_staging
FROM @northwind_stage/suppliers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO shippers_staging
FROM @northwind_stage/shippers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO products_staging
FROM @northwind_stage/products.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orders_staging
FROM @northwind_stage/orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO order_details_staging
FROM @northwind_stage/orderdetails.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employees_staging
FROM @northwind_stage/employees.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO customers_staging
FROM @northwind_stage/customers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO categories_staging
FROM @northwind_stage/categories.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

--- Transform
DROP TABLE IF EXISTS fact_order_details;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_shippers;
DROP TABLE IF EXISTS dim_employees;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_suppliers;
DROP TABLE IF EXISTS dim_order_date;
DROP TABLE IF EXISTS dim_order_time;

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(50),
    contact_name VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(20),
    postal_code VARCHAR(10),
    country VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS dim_shippers (
    shipper_id INT PRIMARY KEY,
    shipper_name VARCHAR(25),
    phone VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS dim_employees (
    employee_id INT PRIMARY KEY,
    last_name VARCHAR(15),
    first_name VARCHAR(15),
    birth_date DATETIME
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(50),
    unit VARCHAR(25),
    price DECIMAL(10,0),
    category_name VARCHAR(25)
);

CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(50),
    contact_name VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(20),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS dim_order_date (
    order_date_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    day_of_week INT,
    day_of_week_string VARCHAR(45),
    week INT,
    month INT,
    month_string VARCHAR(45),
    year INT,
    quarter INT
);

CREATE TABLE IF NOT EXISTS dim_order_time (
    order_time_id INT PRIMARY KEY,
    full_time TIME,
    hour INT,
    minute INT,
    second INT,
    ampm VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS fact_order_details (
    order_detail_id INT PRIMARY KEY,
    order_id INT,
    order_date DATETIME,
    quantity INT,
    total_price DECIMAL(10,0),
    customer_id INT,
    shipper_id INT,
    employee_id INT,
    product_id INT,
    supplier_id INT,
    order_date_id INT,
    order_time_id INT,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (shipper_id) REFERENCES dim_shippers(shipper_id),
    FOREIGN KEY (employee_id) REFERENCES dim_employees(employee_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (supplier_id) REFERENCES dim_suppliers(supplier_id),
    FOREIGN KEY (order_date_id) REFERENCES dim_order_date(order_date_id),
    FOREIGN KEY (order_time_id) REFERENCES dim_order_time(order_time_id)
);

TRUNCATE TABLE fact_order_details;
TRUNCATE TABLE dim_customers;
TRUNCATE TABLE dim_shippers;
TRUNCATE TABLE dim_employees;
TRUNCATE TABLE dim_products;
TRUNCATE TABLE dim_suppliers;
TRUNCATE TABLE dim_order_date;
TRUNCATE TABLE dim_order_time;

INSERT INTO dim_customers
SELECT customer_id, customer_name, contact_name, address, city, postal_code, country
FROM customers_staging;

INSERT INTO dim_shippers
SELECT shipper_id, shipper_name, phone
FROM shippers_staging;

INSERT INTO dim_employees
SELECT employee_id, last_name, first_name, birth_date
FROM employees_staging;

INSERT INTO dim_products
SELECT p.product_id, p.product_name, p.unit, p.price, c.category_name
FROM products_staging p
JOIN categories_staging c ON p.category_id = c.category_id;

INSERT INTO dim_suppliers
SELECT supplier_id, supplier_name, contact_name, address, city, postal_code, country, phone
FROM suppliers_staging;

INSERT INTO dim_order_date
SELECT 
    ROW_NUMBER() OVER (ORDER BY DATE(order_date)) as order_date_id,
    DATE(order_date) as full_date,
    DAY(order_date) as day,
    DAYOFWEEK(order_date) as day_of_week,
    DAYNAME(order_date) as day_of_week_string,
    WEEK(order_date) as week,
    MONTH(order_date) as month,
    MONTHNAME(order_date) as month_string,
    YEAR(order_date) as year,
    QUARTER(order_date) as quarter
FROM orders_staging
GROUP BY DATE(order_date), 
         DAY(order_date),
         DAYOFWEEK(order_date),
         DAYNAME(order_date),
         WEEK(order_date),
         MONTH(order_date),
         MONTHNAME(order_date),
         YEAR(order_date),
         QUARTER(order_date);

INSERT INTO dim_order_time
SELECT 
    ROW_NUMBER() OVER (ORDER BY full_time) as order_time_id,
    full_time,
    hour,
    minute,
    second,
    ampm
FROM (
    SELECT DISTINCT
        TIME(order_date) as full_time,
        HOUR(order_date) as hour,
        MINUTE(order_date) as minute,
        SECOND(order_date) as second,
        CASE WHEN HOUR(order_date) < 12 THEN 'AM' ELSE 'PM' END as ampm
    FROM orders_staging
    WHERE TIME(order_date) IS NOT NULL
);

INSERT INTO fact_order_details
SELECT 
    od.order_detail_id,
    o.order_id,
    o.order_date,
    od.quantity,
    od.quantity * p.price as total_price,
    o.customer_id,
    o.shipper_id,
    o.employee_id,
    od.product_id,
    p.supplier_id,
    od_date.order_date_id,
    od_time.order_time_id
FROM order_details_staging od
JOIN orders_staging o ON od.order_id = o.order_id
JOIN products_staging p ON od.product_id = p.product_id
JOIN dim_order_date od_date ON DATE(o.order_date) = od_date.full_date
JOIN dim_order_time od_time ON TIME(o.order_date) = od_time.full_time;

-- Testovanie
SELECT * FROM dim_customers;
SELECT * FROM dim_shippers;
SELECT * FROM dim_employees;
SELECT * FROM dim_products;
SELECT * FROM dim_suppliers;
SELECT * FROM dim_order_date;
SELECT * FROM dim_order_time;
SELECT * FROM fact_order_details;


-- zmazanie staging tabuliek
DROP TABLE IF EXISTS order_details_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS suppliers_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS shippers_staging;