# Projeto de Análise de Dados para Empresa de Bicicletas

## 1. Introdução
O projeto consiste em um sistema de análise de dados para uma empresa fictícia de produção de bicicletas. O objetivo é extrair insights valiosos dos dados da empresa por meio de análises SQL e Python. A arquitetura é composta por três componentes principais: modelagem de dados, infraestruturação do banco de dados e processamento e análise dos dados.

## 2. Modelagem de Dados
Para representar os dados da empresa, foram definidas classes de modelo em Python usando a biblioteca SQLAlchemy. Cada classe representa uma tabela do banco de dados, incluindo detalhes de pedidos, cabeçalhos de pedidos, produtos, ofertas especiais, clientes e pessoas. Essas classes servem como uma camada de abstração entre os dados brutos e o banco de dados, facilitando a inserção e recuperação de dados.

## 3. Infraestruturação do Banco de Dados
O banco de dados foi hospedado no Google Cloud Platform (GCP) e configurado para armazenar os dados da empresa. A conexão com o banco de dados é estabelecida usando uma URL fornecida por variáveis de ambiente. A criação das tabelas é automatizada através da função `create_tables`, que utiliza a base declarativa definida anteriormente para criar as tabelas correspondentes no banco de dados.

## 4. Processamento e Análise dos Dados
Os dados são fornecidos em arquivos CSV, que são lidos e processados usando a biblioteca pandas. Antes de inserir os dados no banco de dados, é realizada uma limpeza e transformação dos dados, incluindo a conversão de vírgulas para pontos em campos numéricos e a conversão de strings para tipos booleanos onde necessário. Após o processamento, os dados são inseridos no banco de dados usando a função `insert_data_to_db`.

## 5. Análises Realizadas
Após a inserção dos dados, foram realizadas análises SQL para responder a diversas questões sobre o negócio, como a quantidade de linhas na tabela `Sales.SalesOrderDetail` por `SalesOrderID`, os produtos mais vendidos por `DaysToManufacture`, a contagem de pedidos por cliente e a soma total de produtos por `ProductID` e `OrderDate`.

## 6. Conclusão
Este projeto demonstra uma abordagem completa para analisar dados de uma empresa de produção de bicicletas. A combinação de modelagem de dados eficiente, infraestruturação do banco de dados e análises robustas permite extrair insights valiosos que podem ser utilizados para melhorar as operações e tomadas de decisão da empresa.


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
