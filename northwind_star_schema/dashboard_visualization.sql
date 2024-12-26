
CREATE DATABASE IF NOT EXISTS EAGLE_NorthWind_DB;

CREATE SCHEMA IF NOT EXISTS EAGLE_NorthWind_DB.staging;

USE SCHEMA EAGLE_NorthWind_DB.staging;


-- Vizualizácia: Časový trend predajov
-- Otázka: Ako sa vyvíjajú predaje v rokoch/mesiacoch? DONE

SELECT 
    d.year,
    d.monthAsString,
    COUNT(DISTINCT f.OrderDetailID) as NumberOfOrders,
    SUM(f.TotalPrice) as MonthlyRevenue
FROM fact_orderdetails f
JOIN dim_order_date d ON f.OrderDateID = d.OrderDateID
GROUP BY d.year, d.month, d.monthAsString
ORDER BY d.year, d.month;



-- Vizualizácia: Výkonnosti najdrahšších dodávateľov
-- Otázka: Ako sa vyvíjajú predaje v rokoch/mesiacoch? DONE

SELECT 
    s.SupplierName,
    s.Country,
    COUNT(DISTINCT f.OrderDetailID) as NumberOfOrders,
    SUM(f.Quantity) as TotalQuantity,
    SUM(f.TotalPrice) as TotalRevenue
FROM fact_orderdetails f
JOIN dim_suppliers s ON f.SupplierID = s.SupplierID
GROUP BY s.SupplierID, s.SupplierName, s.Country
ORDER BY TotalRevenue DESC
LIMIT 10;

-- Vizualizácia: Porovnanie predaja produktov v jednolivých rokoch
-- Otázka: Porovnanie predajov pre produkty v jednotlivých rokoch DONE


SELECT 
    p.ProductName, 
    d.year, 
    d.month, 
    SUM(f.Quantity) AS TotalSold
FROM fact_orderdetails f
JOIN dim_products p ON f.ProductID = p.ProductID
JOIN dim_order_date d ON f.OrderDateID = d.OrderDateID
GROUP BY p.ProductName, d.year, d.month
ORDER BY p.ProductName, d.year, d.month;


-- Vizualizácia: Predaje podľa kategórií produktov
-- Otázka: Aký je podiel jednotlivých kategórií na celkových tržbách? DONE

SELECT 
    p.CategoryName,
    SUM(f.TotalPrice) as Revenue
FROM fact_orderdetails f
JOIN dim_products p ON f.ProductID = p.ProductID
GROUP BY p.CategoryName
ORDER BY Revenue DESC;

-- Vizualizácia: Kľúčové metriky výkonnosti zamestnancov (Bar chart)
-- Otázka: Ktorí zamestnanci sú najproduktívnejší z hľadiska predajov? DONE

SELECT 
    CONCAT(de.FirstName, ' ', de.LastName) as EmployeeName,
    COUNT(DISTINCT f.OrderDetailID) as OrderCount,
    SUM(f.TotalPrice) as TotalSales,
    AVG(f.TotalPrice) as AvgOrderValue
FROM fact_orderdetails f
JOIN dim_employees de ON f.EmployeeID = de.EmployeeID
GROUP BY de.EmployeeID, EmployeeName
ORDER BY TotalSales DESC;







