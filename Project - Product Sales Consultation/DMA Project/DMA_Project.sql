# DATABASE CREATION
CREATE DATABASE IF NOT EXISTS BEAUTY_PRODUCT_SALES;
SHOW DATABASES;
USE BEAUTY_PRODUCT_SALES;
SHOW TABLES;

# TABLE CREATION
USE BEAUTY_PRODUCT_SALES;
CREATE TABLE Product
(Prod_id INT NOT NULL, 
Prod_Name VARCHAR(100) NOT NULL,
Prod_Type VARCHAR(100),
C_Name VARCHAR(200),
PRIMARY KEY(Prod_id));
CREATE TABLE Delivery_Partner
(Dp_id INT NOT NULL, 
Name VARCHAR(100) NOT NULL,
Phone VARCHAR(100),
C_Name VARCHAR(200),
PRIMARY KEY(Dp_id));
CREATE TABLE Online_Order
(O_Ord_no INT NOT NULL, 
Amount INT NOT NULL,
To_Address VARCHAR(500) NOT NULL,
Cust_id INT NOT NULL,
Dp_id INT NOT NULL,
C_Name VARCHAR(200) NOT NULL,
PRIMARY KEY(O_Ord_no));
CREATE TABLE Online_Bill
(O_Bill_no INT NOT NULL, 
Total_Amount INT NOT NULL,
Payment_Type VARCHAR(100) NOT NULL,
Cust_id INT NOT NULL,
O_Ord_no INT NOT NULL,
PRIMARY KEY(O_Bill_no));
CREATE TABLE Store
(Store_id INT NOT NULL, 
Location VARCHAR(500) NOT NULL,
C_Name VARCHAR(200) NOT NULL,
PRIMARY KEY(Store_id));
CREATE TABLE Retail_Order
(R_Ord_no INT NOT NULL, 
Amount INT NOT NULL,
Qnty INT NOT NULL,
Cust_id INT NOT NULL,
Store_id INT NOT NULL,
PRIMARY KEY(R_Ord_no));
CREATE TABLE Retail_Bill
(R_Bill_no INT NOT NULL, 
Total_Amount INT NOT NULL,
Cust_id INT NOT NULL,
R_Ord_no INT NOT NULL,
PRIMARY KEY(R_Bill_no));
CREATE TABLE Online_Order_Facilitates
(OOF_no INT NOT NULL,
O_Ord_no INT NOT NULL, 
Prod_id INT NOT NULL,
Qnty INT NOT NULL,
C_Name VARCHAR(200) NOT NULL,
PRIMARY KEY(OOF_no));
CREATE TABLE Retail_Store_Facilitates
(RSF_no INT NOT NULL,
Store_id INT NOT NULL, 
Prod_id INT NOT NULL,
C_Name VARCHAR(200) NOT NULL,
PRIMARY KEY(RSF_no));
CREATE TABLE Comapny
(C_Name VARCHAR(200) NOT NULL,
Location VARCHAR(100) NOT NULL, 
Contact INT NOT NULL,
Web_Site VARCHAR(500),
PRIMARY KEY(C_Name));
CREATE TABLE Customer
(Cust_id INT NOT NULL,
Age INT, 
Name VARCHAR(150) NOT NULL,
Phone INT,
Cust_Type VARCHAR(100),
PRIMARY KEY(Cust_id));

# DESCRIBE AND SELECT STATEMENT
DESCRIBE Product;
SELECT * FROM Store;
SELECT * FROM Product;
SELECT * FROM Online_Order;
SELECT * FROM Online_Bill;
SELECT * FROM Online_Order_Facilitates;
SELECT * FROM Delivery_Partner;
SELECT * FROM Company;
SELECT * FROM Retail_Bill;
SELECT * FROM Retail_Order;
SELECT * FROM Retail_Store_Facilitates;
SELECT * FROM Customer;

# ALTERING TABLES
ALTER TABLE Retail_Store_Facilitates
DROP FOREIGN KEY Store_id;
DESCRIBE Retail_Store_Facilitates;
ALTER TABLE Retail_Store_Facilitates
ADD Qnty INT;
DESCRIBE Retail_Bill;
ALTER TABLE Comapny
RENAME TO Company;

# QUERY_1 - FETCH DATA FOR TOP 10 HIGHEST PAYING ONLINE CUSTOMER WITH TOTAL AMOUNT SPENT
SELECT DISTINCT(Customer.Cust_Id), Customer.Name AS Highest_Paying_Customer, 
SUM(Online_Order.Amount) As Total_Amount_Paid, Customer.Cust_Type
FROM Online_Order
INNER JOIN Customer ON Online_Order.Cust_id = Customer.Cust_id
GROUP BY Customer.Cust_id, Customer.Name
ORDER BY SUM(Online_Order.Amount) DESC
limit 10;

# QUERY_2 - FETCH DATA FOR TOP 10 HIGHEST PAYING RETAIL CUSTOMER WITH TOTAL AMOUNT SPENT
SELECT DISTINCT(Customer.Cust_Id), Customer.Name AS Highest_Paying_Customer, 
SUM(Retail_Order.Amount) As Total_Amount_Paid, Customer.Cust_Type
FROM Retail_Order
INNER JOIN Customer ON Retail_Order.Cust_id = Customer.Cust_id
GROUP BY Customer.Cust_id, Customer.Name
ORDER BY SUM(Retail_Order.Amount) DESC
limit 10;

# QUERY_3 - FETCH DATA FOR TOP 5 SOLD PRODUCTS ONLINE
SELECT DISTINCT(Online_Order_Facilitates.Prod_id), Product.Prod_Name, Product.Prod_Type,
Sum(Online_Order_Facilitates.Qnty) As Total_Qnty_Sold, Online_Order_Facilitates.C_Name AS Company_Name
FROM Online_Order_Facilitates
INNER JOIN Product ON Online_Order_Facilitates.Prod_id = Product.Prod_id
GROUP BY Prod_id, Company_Name
ORDER BY Sum(Online_Order_Facilitates.Qnty) DESC 
limit 5;

# QUERY_4 - FETCH DATA FOR TOP 5 SOLD PRODUCTS IN RETAIL
SELECT DISTINCT(Retail_Store_Facilitates.Prod_id), Product.Prod_Name, Product.Prod_Type,
Sum(Retail_Store_Facilitates.Qnty) As Total_Qnty_Sold, Retail_Store_Facilitates.C_Name AS Company_Name
FROM Retail_Store_Facilitates
INNER JOIN Product ON Retail_Store_Facilitates.Prod_id = Product.Prod_id
GROUP BY Prod_id, Company_Name
ORDER BY Sum(Retail_Store_Facilitates.Qnty) DESC 
limit 5;

# QUERY_5 - FETCH DATA FOR MOST USED METHOD FOR ONLINE MONEY TRANSACTIONS
SELECT DISTINCT(Payment_Type), COUNT(Payment_Type) AS No_of_Transactions
FROM Online_Bill
GROUP BY Payment_Type
ORDER BY COUNT(Payment_Type) DESC;

# QUERY_6 - FETCH DATA FOR TOP 5 POTENTIAL BUSINESS LOCATIONS FOR ONLINE CUSTOMERS
SELECT DISTINCT(Online_Order.C_Name) AS Company_Name, 
SUM(Online_Order.Amount) AS Total_Amount_of_Prod_Sold, Company.Location AS Top_5_Locations
FROM Online_Order 
JOIN Company ON Online_Order.C_Name = Company.C_Name
GROUP BY Location, Company_Name
ORDER BY SUM(Online_Order.Amount) DESC 
limit 5;

# QUERY_7 - FETCH DATA FOR TOP 5 POTENTIAL BUSINESS LOCATIONS FOR RETAIL CUSTOMERS
SELECT DISTINCT(Retail_Order.Store_id) AS Store_id, Store.C_Name AS Company_Name,
SUM(Retail_Order.Amount) AS Total_Amount_of_Prod_Sold, Store.Location AS Top_5_Locations
FROM Retail_Order 
JOIN Store ON Retail_Order.Store_id = Store.Store_id
GROUP BY Location, Company_Name, Store_id
ORDER BY SUM(Retail_Order.Amount) DESC
limit 5;

# QUERY_8 - FETCH DATA FOR TOP 5 TARGET AGE GROUPS FOR ONLINE CUSTOMERS ACCORDING TO TOTAL AMOUNT SPENT BY THEM
SELECT DISTINCT(Customer.Age), COUNT(Customer.Age) AS No_of_Customers, 
SUM(Online_Order.Amount) AS Total_Amount_Spent
FROM Online_Order
INNER JOIN Customer ON Online_Order.Cust_id = Customer.Cust_id
GROUP BY Customer.Age
ORDER BY SUM(Online_Order.Amount) DESC
limit 5;

# QUERY_9 - FETCH DATA FOR TOP 5 TARGET AGE GROUPS FOR RETAIL CUSTOMERS ACCORDING TO TOTAL AMOUNT SPENT BY THEM
SELECT DISTINCT(Customer.Age), COUNT(Customer.Age) AS No_of_Customers, 
SUM(Retail_Order.Amount) As Total_Amount_Spent
FROM Retail_Order
INNER JOIN Customer ON Retail_Order.Cust_id = Customer.Cust_id
GROUP BY Customer.Age
ORDER BY SUM(Retail_Order.Amount) DESC
limit 5;

# QUERY_10 - FETCH DATA FOR TOP 10 CUSTOMERS WITH AGE GROUP AS A NULL VALUE FOR ONLINE CUSTOMERS ACCORDING TO DESC TOTAL AMOUNT SPENT 
SELECT Online_Order.Cust_id, Customer.Name, Customer.Age, 
Online_Order.Amount
FROM Online_Order
INNER JOIN Customer ON Online_Order.Cust_id = Customer.Cust_id
WHERE Customer.Age = ''
ORDER BY Online_Order.Amount DESC
LIMIT 5;

# QUERY_11 - FETCH DATA FOR TOP 10 CUSTOMERS WITH AGE GROUP AS A NULL VALUE FOR RETAIL CUSTOMERS ACCORDING TO DESC TOTAL AMOUNT SPENT
SELECT Retail_Order.Cust_id, Customer.Name, Customer.Age, 
Retail_Order.Amount
FROM Retail_Order
INNER JOIN Customer ON Retail_Order.Cust_id = Customer.Cust_id
WHERE Customer.Age = ''
ORDER BY Retail_Order.Amount DESC
LIMIT 5;