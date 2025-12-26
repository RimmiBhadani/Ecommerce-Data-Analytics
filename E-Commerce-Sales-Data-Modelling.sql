--Checking if the dataset was uploaded correctly
SELECT * FROM workspace.default.e_commerce_dataset LIMIT 20;

-- This creates a clean version of the table for analysis
CREATE OR REPLACE TABLE sales_table AS 
SELECT 
CAST(Order_Date AS DATE) AS order_date,
 `Time`,
Aging,
Customer_Id,
Gender,
Device_Type,
Customer_Login_type,
Product_Category,
Product,
CAST(Sales AS DOUBLE) AS sales,
CAST(Quantity AS INT) AS quantity,
CAST(Discount AS DOUBLE) AS discount,
CAST(Profit AS DOUBLE) AS profit,
CAST(Shipping_Cost AS DOUBLE) AS shipping_cost,
Order_Priority,
Payment_method
FROM workspace.default.e_commerce_dataset;

--Checking The sales_table was created correctly and exploring the data
SELECT * FROM sales_table;
SELECT COUNT(*) FROM sales_table; -- There are total 51290 rows
DESCRIBE sales_table;

-- Checking for duplicate values
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY order_date, `Time`, Aging, Customer_Id, Gender, Device_Type, Customer_Login_type, Product_Category, Product
ORDER BY order_date) AS row_num
FROM sales_table
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

-- This table have no duplicate values so we can move to next steps

-- Handle missing values or NULL values in important columns
SELECT
  SUM(CASE WHEN order_date IS NULL THEN 1 END) AS null_order_date,
  SUM(CASE WHEN Customer_Id IS NULL THEN 1 END) AS null_customer,
  SUM(CASE WHEN Product IS NULL THEN 1 END) AS null_product,
  SUM(CASE WHEN sales IS NULL THEN 1 END) AS null_sales,
  SUM(CASE WHEN quantity IS NULL THEN 1 END) AS null_quantity,
  SUM(CASE WHEN profit IS NULL THEN 1 END) AS null_profit,
  SUM(CASE WHEN Device_Type IS NULL THEN 1 END) AS null_device,
  SUM(CASE WHEN Payment_method IS NULL THEN 1 END) AS null_payment
FROM sales_table;

-- There are 2 missing values in the column quantity and 1 missing value in the column sales.
-- So we will drop that rows. Every other column have no missing values.

DELETE FROM sales_table WHERE quantity IS NULL OR sales IS NULL OR Order_Priority IS NULL;

-- Now checking for missing values again to make sure
SELECT
  SUM(CASE WHEN sales IS NULL THEN 1 END) AS null_sales,
  SUM(CASE WHEN quantity IS NULL THEN 1 END) AS null_quantity
FROM sales_table; 

-- Now we have handled all the missing values and we can proceed to next step
-- Remove any unneccessary columns
ALTER TABLE sales_table SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
);

ALTER TABLE sales_table DROP COLUMN `Time`;
ALTER TABLE sales_table DROP COLUMN Aging;

SELECT * FROM sales_table;

-- Now we create fact table and dimension tables

CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT order_date, YEAR(order_date) AS year, MONTH(order_date) AS month
FROM sales_table;

CREATE OR REPLACE TABLE dim_customer AS
SELECT
  Customer_Id AS customer_id,
  FIRST(Gender) AS Gender,
  FIRST(Customer_Login_type) AS Customer_Login_type
FROM sales_table
GROUP BY Customer_Id;


CREATE OR REPLACE TABLE dim_product AS
SELECT DISTINCT
Product AS product_name,
Product_Category AS product_category
FROM sales_table;

CREATE OR REPLACE TABLE dim_payment AS
SELECT DISTINCT
Payment_method AS payment_method
FROM sales_table;

CREATE OR REPLACE TABLE dim_order_priority AS
SELECT DISTINCT
Order_Priority AS order_priority
FROM sales_table;

SELECT * FROM dim_date;
SELECT * FROM dim_customer;
SELECT * FROM dim_payment;
SELECT * FROM dim_order_priority;
SELECT * FROM dim_product;

CREATE OR REPLACE TABLE fact_sales AS
SELECT
order_date,
Customer_Id AS customer_id,
Product AS product_name,
Payment_method,
Device_Type,
Order_Priority,
quantity,
sales,
profit,
discount,
shipping_cost
FROM sales_table;

