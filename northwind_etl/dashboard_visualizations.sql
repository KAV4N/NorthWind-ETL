CREATE DATABASE IF NOT EXISTS eagle_northwind_db;
CREATE SCHEMA IF NOT EXISTS eagle_northwind_db.staging;
USE SCHEMA eagle_northwind_db.staging;


--(chart1) All time daily revenue
SELECT 
    d.full_date,
    SUM(f.total_price) as daily_revenue,
FROM fact_order_details f
JOIN dim_order_date d ON f.order_date_id = d.order_date_id
GROUP BY d.full_date
ORDER BY d.full_date;

--(chart2) All time revenue by countries
SELECT 
    c.country,
    SUM(f.total_price) as total_revenue,
FROM fact_order_details f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC;

--(chart3) Order value distribution per country
SELECT 
    c.country,
    CASE 
        WHEN f.total_price < 100 THEN 'Small (<$100)'
        WHEN f.total_price < 500 THEN 'Medium ($100-$500)'
        WHEN f.total_price < 1000 THEN 'Large ($500-$1000)'
        ELSE 'Extra Large (>$1000)'
    END as order_size,
    COUNT(*) as order_count
FROM fact_order_details f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.country, order_size
ORDER BY c.country, order_size;

--(chart4) Product sales comparison by year
SELECT 
    p.product_name, 
    d.year, 
    SUM(f.quantity) AS total_sold
FROM fact_order_details f
JOIN dim_products p ON f.product_id = p.product_id
JOIN dim_order_date d ON f.order_date_id = d.order_date_id
GROUP BY p.product_name, d.year
ORDER BY p.product_name, d.year;


--(chart5) Best selling product categories
SELECT 
    p.category_name,
    SUM(f.total_price) as revenue
FROM fact_order_details f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.category_name
ORDER BY revenue DESC;

--(chart6) Sales performance by employee (TOP 10)
SELECT 
    CONCAT(e.first_name, ' ', e.last_name) as employee_name,
    COUNT(DISTINCT f.order_id) as total_orders,
FROM dim_employees e
JOIN fact_order_details f ON e.employee_id = f.employee_id
GROUP BY employee_name
ORDER BY total_orders DESC
LIMIT 10;

