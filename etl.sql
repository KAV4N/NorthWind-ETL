USE ROLE training_role;

CREATE WAREHOUSE IF NOT EXISTS EAGLE_WH;
ALTER WAREHOUSE EAGLE_WH SET WAREHOUSE_SIZE=XSmall;
USE WAREHOUSE EAGLE_WH;

CREATE DATABASE IF NOT EXISTS NorthWind_DB;
USE NorthWind_DB;

CREATE SCHEMA IF NOT EXISTS staging;
USE staging;


DROP TABLE IF EXISTS OrderDetails_staging;
DROP TABLE IF EXISTS Orders_staging;
DROP TABLE IF EXISTS Products_staging;
DROP TABLE IF EXISTS Categories_staging;
DROP TABLE IF EXISTS Suppliers_staging;
DROP TABLE IF EXISTS Employees_staging;
DROP TABLE IF EXISTS Customers_staging;
DROP TABLE IF EXISTS Shippers_staging;



CREATE TABLE Categories_staging
(      
    CategoryID INTEGER PRIMARY KEY,
    CategoryName VARCHAR(25),
    Description VARCHAR(255)
);

CREATE TABLE Customers_staging
(      
    CustomerID INTEGER PRIMARY KEY,
    CustomerName VARCHAR(50),
    ContactName VARCHAR(50),
    Address VARCHAR(50),
    City VARCHAR(20),
    PostalCode VARCHAR(10),
    Country VARCHAR(15)
);

CREATE TABLE Employees_staging
(
    EmployeeID INTEGER PRIMARY KEY,
    LastName VARCHAR(15),
    FirstName VARCHAR(15),
    BirthDate DATETIME,
    Photo VARCHAR(25),
    Notes VARCHAR(1024)
);

CREATE TABLE Shippers_staging(
    ShipperID INTEGER PRIMARY KEY,
    ShipperName VARCHAR(25),
    Phone VARCHAR(15)
);

CREATE TABLE Suppliers_staging(
    SupplierID INTEGER PRIMARY KEY,
    SupplierName VARCHAR(50),
    ContactName VARCHAR(50),
    Address VARCHAR(50),
    City VARCHAR(20),
    PostalCode VARCHAR(10),
    Country VARCHAR(15),
    Phone VARCHAR(15)
);

CREATE TABLE Products_staging(
    ProductID INTEGER PRIMARY KEY,
    ProductName VARCHAR(50),
    SupplierID INTEGER,
    CategoryID INTEGER,
    Unit VARCHAR(25),
    Price NUMERIC,
	FOREIGN KEY (CategoryID) REFERENCES Categories_staging (CategoryID),
	FOREIGN KEY (SupplierID) REFERENCES Suppliers_staging (SupplierID)
);

CREATE TABLE Orders_staging(
    OrderID INTEGER PRIMARY KEY,
    CustomerID INTEGER,
    EmployeeID INTEGER,
    OrderDate DATETIME,
    ShipperID INTEGER,
    FOREIGN KEY (EmployeeID) REFERENCES Employees_staging (EmployeeID),
    FOREIGN KEY (CustomerID) REFERENCES Customers_staging (CustomerID),
    FOREIGN KEY (ShipperID) REFERENCES Shippers_staging (ShipperID)
);

CREATE TABLE OrderDetails_staging(
    OrderDetailID INTEGER PRIMARY KEY,
    OrderID INTEGER,
    ProductID INTEGER,
    Quantity INTEGER,
	FOREIGN KEY (OrderID) REFERENCES Orders_staging (OrderID),
	FOREIGN KEY (ProductID) REFERENCES Products_staging (ProductID)
);





CREATE OR REPLACE STAGE my_stage;

COPY INTO Suppliers_staging
FROM @my_stage/suppliers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO Shippers_staging
FROM @my_stage/shippers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO Products_staging
FROM @my_stage/products.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO Orders_staging
FROM @my_stage/orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO OrderDetails_staging
FROM @my_stage/orderdetails.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO Employees_staging
FROM @my_stage/employees.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO Customers_staging
FROM @my_stage/customers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO Categories_staging
FROM @my_stage/categories.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);




--- ELT - (T)ransform
-- -----------------------------------------------------
-- Table `dim_customers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_customers` (
  `CustomerID` INT(11) NOT NULL,
  `CustomerName` VARCHAR(50) NOT NULL,
  `ContactName` VARCHAR(50) NOT NULL,
  `Address` VARCHAR(50) NOT NULL,
  `City` VARCHAR(20) NOT NULL,
  `PostalCode` VARCHAR(10) NOT NULL,
  `Country` VARCHAR(15) NOT NULL,
  PRIMARY KEY (`CustomerID`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dim_shippers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_shippers` (
  `ShipperID` INT NOT NULL,
  `ShipperName` VARCHAR(25) NOT NULL,
  `Phone` VARCHAR(15) NOT NULL,
  PRIMARY KEY (`ShipperID`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dim_employees`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_employees` (
  `EmployeeID` INT(11) NOT NULL,
  `LastName` VARCHAR(15) NOT NULL,
  `FirstName` VARCHAR(15) NOT NULL,
  `BirthDate` DATETIME NOT NULL,
  PRIMARY KEY (`EmployeeID`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dim_products`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_products` (
  `ProductID` INT NOT NULL,
  `ProductName` VARCHAR(50) NOT NULL,
  `Unit` VARCHAR(25) NOT NULL,
  `Price` DECIMAL(10,0) NOT NULL,
  `CategoryName` VARCHAR(25) NOT NULL,
  PRIMARY KEY (`ProductID`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dim_suppliers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_suppliers` (
  `SupplierID` INT NOT NULL,
  `SupplierName` VARCHAR(50) NOT NULL,
  `ContactName` VARCHAR(50) NOT NULL,
  `Address` VARCHAR(50) NOT NULL,
  `City` VARCHAR(20) NOT NULL,
  `PostalCode` VARCHAR(10) NOT NULL,
  `Country` VARCHAR(15) NOT NULL,
  `Phone` VARCHAR(15) NOT NULL,
  PRIMARY KEY (`SupplierID`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dim_order_date`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_order_date` (
  `OrderDateID` INT NOT NULL,
  `timestamp` DATE NOT NULL,
  `day` INT NOT NULL,
  `dayOfWeek` INT NOT NULL,
  `dayOfWeekAsString` VARCHAR(45) NOT NULL,
  `week` INT NOT NULL,
  `month` INT NOT NULL,
  `monthAsString` VARCHAR(45) NOT NULL,
  `year` INT NOT NULL,
  `quarter` INT NOT NULL,
  PRIMARY KEY (`OrderDateID`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dim_order_time`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_order_time` (
  `OrderTimeID` INT NOT NULL,
  `timestamp` TIME NOT NULL,
  `hour` INT NOT NULL,
  `ampm` VARCHAR(2) NOT NULL,
  PRIMARY KEY (`OrderTimeID`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `fact_orderdetails`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fact_orderdetails` (
  `OrderDetailID` INT(11) NOT NULL,
  `OrderDate` DATETIME NOT NULL,
  `Quantity` INT(11) NOT NULL,
  `TotalPrice` DECIMAL(10,0) NOT NULL,
  `CustomerID` INT(11) NOT NULL,
  `ShipperID` INT NOT NULL,
  `EmployeeID` INT(11) NOT NULL,
  `ProductID` INT NOT NULL,
  `SupplierID` INT NOT NULL,
  `OrderDateID` INT NOT NULL,
  `OrderTimeID` INT NOT NULL,
  PRIMARY KEY (`OrderDetailID`),
  INDEX `fk_fact_orders_dim_customers1_idx` (`CustomerID` ASC),
  INDEX `fk_fact_orders_dim_shippers1_idx` (`ShipperID` ASC),
  INDEX `fk_fact_orders_dim_employees1_idx` (`EmployeeID` ASC),
  INDEX `fk_fact_orders_dim_products1_idx` (`ProductID` ASC),
  INDEX `fk_fact_orders_dim_suppliers1_idx` (`SupplierID` ASC),
  INDEX `fk_fact_orders_dim_order_date1_idx` (`OrderDateID` ASC),
  INDEX `fk_fact_orders_dim_review_time1_idx` (`OrderTimeID` ASC),
  CONSTRAINT `fk_fact_orders_dim_customers1`
    FOREIGN KEY (`CustomerID`)
    REFERENCES `dim_customers` (`CustomerID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_orders_dim_shippers1`
    FOREIGN KEY (`ShipperID`)
    REFERENCES `dim_shippers` (`ShipperID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_orders_dim_employees1`
    FOREIGN KEY (`EmployeeID`)
    REFERENCES `dim_employees` (`EmployeeID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_orders_dim_products1`
    FOREIGN KEY (`ProductID`)
    REFERENCES `dim_products` (`ProductID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_orders_dim_suppliers1`
    FOREIGN KEY (`SupplierID`)
    REFERENCES `dim_suppliers` (`SupplierID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_orders_dim_order_date1`
    FOREIGN KEY (`OrderDateID`)
    REFERENCES `dim_order_date` (`OrderDateID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_orders_dim_review_time1`
    FOREIGN KEY (`OrderTimeID`)
    REFERENCES `dim_order_time` (`OrderTimeID`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;






INSERT INTO dim_customers
SELECT CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country
FROM Customers_staging;


INSERT INTO dim_shippers
SELECT ShipperID, ShipperName, Phone
FROM Shippers_staging;


INSERT INTO dim_employees
SELECT EmployeeID, LastName, FirstName, BirthDate
FROM Employees_staging;


INSERT INTO dim_products
SELECT p.ProductID, p.ProductName, p.Unit, p.Price, c.CategoryName
FROM Products_staging p
JOIN Categories_staging c ON p.CategoryID = c.CategoryID;


INSERT INTO dim_suppliers
SELECT SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone
FROM Suppliers_staging;


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
FROM Orders_staging
GROUP BY DATE(OrderDate);


INSERT INTO dim_order_time
SELECT 
    ROW_NUMBER() OVER (ORDER BY OrderDate) as OrderTimeID,
    TIME(OrderDate) as timestamp,
    HOUR(OrderDate) as hour,
    CASE WHEN HOUR(OrderDate) < 12 THEN 'AM' ELSE 'PM' END as ampm
FROM Orders_staging
GROUP BY TIME(OrderDate);


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
FROM OrderDetails_staging od
JOIN Orders_staging o ON od.OrderID = o.OrderID
JOIN Products_staging p ON od.ProductID = p.ProductID
JOIN dim_order_date od_date ON DATE(o.OrderDate) = od_date.timestamp
JOIN dim_order_time od_time ON TIME(o.OrderDate) = od_time.timestamp;


DROP TABLE IF EXISTS OrderDetails_staging;
DROP TABLE IF EXISTS Orders_staging;
DROP TABLE IF EXISTS Products_staging;
DROP TABLE IF EXISTS Categories_staging;
DROP TABLE IF EXISTS Suppliers_staging;
DROP TABLE IF EXISTS Employees_staging;
DROP TABLE IF EXISTS Customers_staging;
DROP TABLE IF EXISTS Shippers_staging;









