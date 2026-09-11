-- creat database
CREATE DATABASE IF NOT EXISTS streaming2;
USE streaming2;

-- creat external table
CREATE EXTERNAL TABLE IF NOT EXISTS streaming_orders (
    order_id INT,
    customer_id INT,
    customer_gender STRING,
    product_id INT,
    quantity INT,
    price FLOAT,
    discount FLOAT,
    order_time TIMESTAMP,
    Expected_amount FLOAT,
    is_haker STRING,
    leh_wala_3aleh STRING
)
STORED AS PARQUET
LOCATION '/user/student/streaming2/final/data/';

-- some queries

-- q1
SELECT COUNT(order_id) AS total_orders 
FROM streaming_orders;

-- q2
SELECT 
	sum(case when is_haker = 'Haker' then 1 else 0 end) as total_hakars,
	sum(case when is_haker = 'Dafa3 zeada 7elo' then 1 else 0 end) as total_Dafa3_zeada,
	sum(case when is_haker = 'Not haker' then 1 else 0 end) as total_not_hakars
FROM streaming_orders;

-- q3
SELECT SUM(Expected_amount) AS total_expected_revenue 
FROM streaming_orders;

-- q4
SELECT product_id, SUM(Expected_amount) AS product_sales 
FROM streaming_orders 
GROUP BY product_id 
ORDER BY product_sales DESC
LIMIT 5;

-- q5
SELECT customer_id, SUM(Expected_amount) AS customer_total_spending 
FROM streaming_orders 
GROUP BY customer_id 
ORDER BY customer_total_spending DESC
LIMIT 5;

-- q5
SELECT payment_method, SUM(case when is_haker = 'Haker' then 1 else 0) AS total_hakers
FROM streaming_orders
GROUP BY payment_method
ORDER BY customer_total_spending DESC;

