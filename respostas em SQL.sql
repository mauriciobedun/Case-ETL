/*1.Quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, com pelo menos três linhas de detalhes:*/

SELECT "SalesOrderID", COUNT(*) AS QuantidadeLinhas
FROM "SalesOrderDetail"
GROUP BY "SalesOrderID"
HAVING COUNT(*) >= 3;

	
--2
WITH RankedProducts AS (
    SELECT 
        p."DaysToManufacture",
        p."Name",
        SUM(sod."OrderQty") AS TotalSold,
        ROW_NUMBER() OVER (
            PARTITION BY p."DaysToManufacture" 
            ORDER BY SUM(sod."OrderQty") DESC
        ) AS Rank
    FROM 
        "SalesOrderDetail" sod
    JOIN 
        "SpecialOfferProduct" sop ON sod."ProductID" = sop."ProductID"
    JOIN 
        "Product" p ON sop."ProductID" = p."ProductID"
    GROUP BY 
        p."DaysToManufacture", p."Name"
)
SELECT 
    "DaysToManufacture",
    "Name",
    TotalSold
FROM 
    RankedProducts
WHERE 
    Rank <= 3
ORDER BY 
    "DaysToManufacture", TotalSold DESC;

--3

SELECT 
    p.FirstName || ' ' || p.LastName AS CustomerName,
    COUNT(soh.SalesOrderID) AS OrderCount
FROM 
    "Customer" c
JOIN 
    "Person" p ON c.PersonID = p.BusinessEntityID
LEFT JOIN 
    Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
GROUP BY 
    p.FirstName, p.LastName
ORDER BY 
    OrderCount DESC, CustomerName;
	
SELECT
    "CustomerID",
    COUNT(*) AS NumberOfOrders
FROM
    "SalesOrderHeader"
GROUP BY
    "CustomerID"
ORDER BY
    NumberOfOrders DESC;
	
--4
SELECT
    p."Name",
    sod."ProductID",
    soh."OrderDate",
    SUM(sod."OrderQty") AS TotalQuantity
FROM
    "SalesOrderDetail" sod
JOIN
    "SalesOrderHeader" soh ON sod."SalesOrderID" = soh."SalesOrderID"
JOIN
    "Product" p ON sod."ProductID" = p."ProductID"
GROUP BY
    p."Name",
    sod."ProductID",
    soh."OrderDate"
ORDER BY
    soh."OrderDate", p."Name";
	
	
--5
SELECT
    "SalesOrderID",
    "OrderDate",
    "TotalDue"
FROM
    "SalesOrderHeader"
WHERE
    "OrderDate" >= '2011-09-01' AND
    "OrderDate" < '2011-10-01' AND
    "TotalDue" > 1000
ORDER BY
    "TotalDue" DESC;





