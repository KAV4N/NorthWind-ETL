-- Dimension tables
INSERT INTO dim_customers
SELECT CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country
FROM Customers;

INSERT INTO dim_shippers
SELECT ShipperID, ShipperName, Phone
FROM Shippers;

INSERT INTO dim_employees
SELECT EmployeeID, LastName, FirstName, BirthDate
FROM Employees;

INSERT INTO dim_products
SELECT p.ProductID, p.ProductName, p.Unit, p.Price, c.CategoryName
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID;

INSERT INTO dim_suppliers
SELECT SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone
FROM Suppliers;

-- Time dimension tables
INSERT INTO dim_order_date
SELECT 
    ROW_NUMBER() OVER (ORDER BY OrderDate) as OrderDateID,
    DATE(OrderDate) as timestamp,
    DAY(OrderDate) as day,
    WEEKDAY(OrderDate) as dayOfWeek,
    DAYNAME(OrderDate) as dayOfWeekAsString,
    WEEK(OrderDate) as week,
    MONTH(OrderDate) as month,
    MONTHNAME(OrderDate) as monthAsString,
    YEAR(OrderDate) as year,
    QUARTER(OrderDate) as quarter
FROM Orders
GROUP BY DATE(OrderDate);

INSERT INTO dim_order_time
SELECT 
    ROW_NUMBER() OVER (ORDER BY OrderDate) as OrderTimeID,
    TIME(OrderDate) as timestamp,
    HOUR(OrderDate) as hour,
    CASE WHEN HOUR(OrderDate) < 12 THEN 'AM' ELSE 'PM' END as ampm
FROM Orders
GROUP BY TIME(OrderDate);

-- Fact table
INSERT INTO fact_orderdetails
SELECT 
    od.OrderDetailID,
    o.OrderDate,
    od.Quantity,
    od.Quantity * p.Price as TotalPrice,
    o.CustomerID,
    o.ShipperID,
    o.EmployeeID,
    od.ProductID,
    p.SupplierID,
    od_date.OrderDateID,
    od_time.OrderTimeID
FROM OrderDetails od
JOIN Orders o ON od.OrderID = o.OrderID
JOIN Products p ON od.ProductID = p.ProductID
JOIN dim_order_date od_date ON DATE(o.OrderDate) = od_date.timestamp
JOIN dim_order_time od_time ON TIME(o.OrderDate) = od_time.timestamp;