CREATE TABLE customers (
		customer_id INT PRIMARY KEY,
        customer_namee VARCHAR(100),
        email VARCHAR(150),
        city VARCHAR(100),
        signup_date DATE
);

CREATE TABLE orders (
		order_id INT PRIMARY KEY,
        customer_id INT,
        product_id INT,
        order_date DATE,
        quantity INT,
        unit_price DECIMAL(10,2),
        payment_status VARCHAR(50)
        );

CREATE TABLE products (
		product_id INT PRIMARY KEY,
        product_namee VARCHAR(100),
        category VARCHAR(100),
        supplier_id INT,
        cost_price DECIMAL(10,2),
        selling_price DECIMAL(10,2)
);

ALTER TABLE products
MODIFY COLUMN supplier_id INT;


CREATE TABLE clickstream (
    event_id INT PRIMARY KEY,
    customer_id INT,
    event_type VARCHAR(50),
    page_url VARCHAR(255),
    event_timestamp DATETIME
);

ALTER TABLE clickstream
ADD COLUMN device_type TEXT;

SELECT *
FROM clickstream


ALTER TABLE products
RENAME COLUMN product_namee TO product_name

SHOW TABLES;

DESCRIBE customers; # describe table 

USE mini_project ##switch to schema 

SELECT DATABASE(); ##show schema 

##change column name 
ALTER TABLE customers
RENAME COLUMN customer_namee TO customer_name;


##LOADING FILE IN MYSQL
LOAD DATA INFILE '/Users/andreams/Desktop/files/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT *
FROM customers


SELECT *
FROM orders


##1. Find top 10 customers by revenue.  
SELECT 
	customer_id,
    SUM(quantity*unit_price) AS revenue 
FROM orders
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10;

###2. Find month-over-month sales growth.  

WITH monthly_sales AS (
	SELECT
		DATE_FORMAT(order_date, '%Y-%m') as month,
        SUM(quantity*unit_price) AS revenue 
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
)

SELECT
	month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS previous_month_revenue,
    ROUND(
		(revenue - LAG(revenue) OVER (ORDER BY month))
        / LAG(revenue) OVER (ORDER BY month) * 100,2
        ) AS mom_growth_percentage
	from monthly_sales;


##3. Find customers who ordered in consecutive months.  

SELECT DISTINCT 
	a.customer_id,
    a.order_date AS first_order,
    b.order_date AS next_order
FROM orders a
JOIN orders b
	ON a.customer_id = b.customer_id
    AND TIMESTAMPDIFF(MONTH, a.order_date, b.order_date) = 1; 


##4. Find products never ordered.  

SELECT 
	p.product_id,
    p.product_name
FROM products p
LEFT JOIN orders o
	ON p.product_id = o.product_id
WHERE o.order_id IS NULL;
 
SELECT COUNT(*) FROM products;

SELECT COUNT(DISTINCT product_id) FROM orders;

##5. Find revenue contribution percentage by category.  

SELECT 
	p.category,
    SUM(o.quantity*o.unit_price) AS revenue,
    ROUND(
		SUM(o.quantity*o.unit_price) / 
        (SELECT SUM(quantity*unit_price) FROM orders)
		* 100,2
) AS revenue_percentage
FROM orders o
JOIN products p
	ON o.product_id = p.product_id
GROUP BY category;


	
##Q2. Advanced SQL
#1. Rank customers based on total revenue.  

SELECT 
	customer_id,
    SUM(quantity*unit_price) AS total_revenue,
    RANK() OVER (
		ORDER BY SUM(quantity*unit_price) desc
        ) AS revenue_rank
FROM orders
GROUP BY customer_id;

##2. Find running total sales by month.  


SELECT
	DATE_FORMAT(order_date, '%Y-%m') as month,
	SUM(quantity*unit_price) AS monthly_sales
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m');


SELECT
	DATE_FORMAT(order_date, '%Y-%m') as month,
	SUM(quantity*unit_price) AS monthly_sales,
    SUM(SUM(quantity*unit_price)) OVER (
		ORDER BY DATE_FORMAT(order_date, '%Y-%m')
	) AS running_total_sales
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m');
	
    

###3. Find the highest selling product per category.  

SELECT
	p.category,
    p.product_name,
    SUM(o.quantity*o.unit_price) AS revenue
FROM products p
JOIN orders o
	ON p.product_id = o.product_id
GROUP BY 
	p.category,
    p.product_name;
    
##4. Find 7-day rolling average sales.  

SELECT 
	order_date,
    SUM(quantity*unit_price) as daily_sales,
    AVG(SUM(quantity*unit_price)) OVER (
    ORDER BY order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
	) AS rolling_7_day_average
FROM orders
GROUP BY order_date
ORDER BY order_date;

##Q3. SQL Optimization Scenario 
EXPLAIN ANALYZE
SELECT c.customer_name, 
 SUM(o.quantity * o.unit_price) 
FROM orders o 
JOIN customers c 
ON o.customer_id = c.customer_id
WHERE order_date >= '2025-01-01' 
GROUP BY c.customer_name; 

CREATE INDEX idx_orders_date
ON orders(order_date);

    




