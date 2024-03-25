CREATE TABLE "SalesOrderDetail" (
  "SalesOrderID" INT PRIMARY KEY,
  "SalesOrderDetailID" INT,
  "CarrierTrackingNumber" VARCHAR(255),
  "OrderQty" INT,
  "ProductID" INT,
  "SpecialOfferID" INT,
  "UnitPrice" FLOAT,
  "UnitPriceDiscount" FLOAT,
  "LineTotal" FLOAT,
  "rowguid" UUID,
  "ModifiedDate" TIMESTAMP
);

CREATE TABLE "SalesOrderHeader" (
  "SalesOrderID" INT PRIMARY KEY,
  "RevisionNumber" INT,
  "OrderDate" DATE,
  "DueDate" DATE,
  "ShipDate" DATE,
  "Status" VARCHAR(50),
  "OnlineOrderFlag" BOOLEAN,
  "SalesOrderNumber" VARCHAR(50),
  "PurchaseOrderNumber" VARCHAR(50),
  "AccountNumber" VARCHAR(50),
  "CustomerID" INT,
  "SalesPersonID" INT,
  "TerritoryID" INT,
  "BillToAddressID" INT,
  "ShipToAddressID" INT,
  "ShipMethodID" INT,
  "CreditCardID" INT,
  "CreditCardApprovalCode" VARCHAR(50),
  "CurrencyRateID" INT,
  "SubTotal" NUMERIC(18,2),
  "TaxAmt" NUMERIC(18,2),
  "Freight" NUMERIC(18,2),
  "TotalDue" NUMERIC(18,2),
  "Comment" TEXT,
  "rowguid" UUID,
  "ModifiedDate" TIMESTAMP
);

CREATE TABLE "Product" (
  "ProductID" INT PRIMARY KEY,
  "Name" VARCHAR(255),
  "ProductNumber" VARCHAR(50),
  "MakeFlag" BOOLEAN,
  "FinishedGoodsFlag" BOOLEAN,
  "Color" VARCHAR(50),
  "SafetyStockLevel" INT,
  "ReorderPoint" INT,
  "StandardCost" NUMERIC(18,2),
  "ListPrice" NUMERIC(18,2),
  "Size" VARCHAR(50),
  "SizeUnitMeasureCode" VARCHAR(10),
  "WeightUnitMeasureCode" VARCHAR(10),
  "Weight" NUMERIC(18,2),
  "DaysToManufacture" INT,
  "ProductLine" CHAR(2),
  "Class" CHAR(2),
  "Style" VARCHAR(50),
  "ProductSubcategoryID" INT,
  "ProductModelID" INT,
  "SellStartDate" DATE,
  "SellEndDate" DATE,
  "DiscontinuedDate" DATE,
  "rowguid" UUID,
  "ModifiedDate" TIMESTAMP
);

CREATE TABLE "SpecialOfferProduct" (
  "SpecialOfferID" INT PRIMARY KEY,
  "ProductID" INT,
  "rowguid" UUID,
  "ModifiedDate" TIMESTAMP
);

CREATE TABLE "Customer" (
  "CustomerID" INT PRIMARY KEY,
  "PersonID" INT,
  "StoreID" INT,
  "TerritoryID" INT,
  "AccountNumber" VARCHAR(50),
  "rowguid" UUID,
  "ModifiedDate" TIMESTAMP
);

CREATE TABLE "Person" (
  "BusinessEntityID" INT PRIMARY KEY,
  "PersonType" CHAR(2),
  "NameStyle" BOOLEAN,
  "Title" VARCHAR(50),
  "FirstName" VARCHAR(50),
  "MiddleName" VARCHAR(50),
  "LastName" VARCHAR(50),
  "Suffix" VARCHAR(10),
  "EmailPromotion" INT,
  "AdditionalContactInfo" TEXT,
  "Demographics" TEXT,
  "rowguid" UUID,
  "ModifiedDate" TIMESTAMP
);

ALTER TABLE "SalesOrderDetail" ADD FOREIGN KEY ("SalesOrderID") REFERENCES "SalesOrderHeader" ("SalesOrderID");

ALTER TABLE "SalesOrderDetail" ADD FOREIGN KEY ("SpecialOfferID") REFERENCES "SpecialOfferProduct" ("SpecialOfferID");

ALTER TABLE "SpecialOfferProduct" ADD FOREIGN KEY ("ProductID") REFERENCES "Product" ("ProductID");

ALTER TABLE "SalesOrderHeader" ADD FOREIGN KEY ("CustomerID") REFERENCES "Customer" ("CustomerID");

ALTER TABLE "Customer" ADD FOREIGN KEY ("PersonID") REFERENCES "Person" ("BusinessEntityID");
