## Projeto de Análise de Dados para Empresa de Bicicletas
- Introdução
O objetivo deste projeto é demonstrar habilidades em modelagem conceitual de dados, infraestruturação de banco de dados e análise de dados utilizando Python e SQL. O cenário fictício escolhido para este desafio é uma empresa especializada na produção de bicicletas. Iniciamos com a modelagem dos dados, seguida pela criação da infraestrutura necessária para armazenar esses dados em um banco de dados no Google Cloud Platform (GCP). Posteriormente, foram desenvolvidos scripts em Python para inserção de dados a partir de arquivos CSV e realizadas análises de dados utilizando consultas SQL.

- Estrutura e Inserção de Dados
O projeto foi iniciado com a criação de um banco de dados no GCP e a modelagem conceitual dos dados da empresa. Utilizamos Python para desenvolver um script que lê arquivos CSV contendo os dados da empresa e os insere nas tabelas correspondentes do banco de dados. Esse script proporciona uma forma eficiente de carregar grandes volumes de dados, mantendo a integridade e a consistência das informações.

- Análises Realizadas
Após a inserção dos dados, foram elaboradas consultas SQL para responder a diversas questões analíticas sobre o negócio. As análises incluíram:

Identificação da quantidade de linhas na tabela Sales.SalesOrderDetail por SalesOrderID, com pelo menos três linhas de detalhes.
Determinação dos 3 produtos mais vendidos (pela soma de OrderQty) por DaysToManufacture, ligando as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product.
Listagem de nomes de clientes e contagem de pedidos efetuados, ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader.
Cálculo da soma total de produtos (OrderQty) por ProductID e OrderDate, utilizando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product.
Filtragem das ordens feitas durante setembro de 2011 com TotalDue acima de 1.000, exibindo os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader.

- Consultas SQL
As consultas SQL utilizadas para realizar estas análises são apresentadas abaixo:

```
-- 1
SELECT "SalesOrderID", COUNT(*) AS QuantidadeLinhas
FROM "SalesOrderDetail"
GROUP BY "SalesOrderID"
HAVING COUNT(*) >= 3;

-- 2
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

-- 3
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

-- 4
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

```
