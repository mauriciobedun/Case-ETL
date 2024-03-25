import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, Numeric, Date, Text, DateTime
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Definição da base declarativa
Base = declarative_base()

# Definindo a classe de modelo para SalesOrderDetail
class SalesOrderDetail(Base):
    __tablename__ = 'SalesOrderDetail'
    SalesOrderID = Column(Integer, primary_key=True)
    SalesOrderDetailID = Column(Integer, primary_key=True)
    CarrierTrackingNumber = Column(String(255))
    OrderQty = Column(Integer)
    ProductID = Column(Integer)
    SpecialOfferID = Column(Integer)
    UnitPrice = Column(Float)
    UnitPriceDiscount = Column(Float)
    LineTotal = Column(Float)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

# Definindo a classe de modelo para SalesOrderHeader
class SalesOrderHeader(Base):
    __tablename__ = 'SalesOrderHeader'
    SalesOrderID = Column(Integer, primary_key=True)
    RevisionNumber = Column(Integer)
    OrderDate = Column(Date)
    DueDate = Column(Date)
    ShipDate = Column(Date)
    Status = Column(String(50))
    OnlineOrderFlag = Column(Boolean)
    SalesOrderNumber = Column(String(50))
    PurchaseOrderNumber = Column(String(50))
    AccountNumber = Column(String(50))
    CustomerID = Column(Integer)
    SalesPersonID = Column(Integer)
    TerritoryID = Column(Integer)
    BillToAddressID = Column(Integer)
    ShipToAddressID = Column(Integer)
    ShipMethodID = Column(Integer)
    CreditCardID = Column(Integer)
    CreditCardApprovalCode = Column(String(50))
    CurrencyRateID = Column(Integer)
    SubTotal = Column(Numeric(18, 2))
    TaxAmt = Column(Numeric(18, 2))
    Freight = Column(Numeric(18, 2))
    TotalDue = Column(Numeric(18, 2))
    Comment = Column(Text)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

# Definindo a classe de modelo para Product
class Product(Base):
    __tablename__ = 'Product'
    ProductID = Column(Integer, primary_key=True)
    Name = Column(String(255))
    ProductNumber = Column(String(50))
    MakeFlag = Column(Boolean)
    FinishedGoodsFlag = Column(Boolean)
    Color = Column(String(50))
    SafetyStockLevel = Column(Integer)
    ReorderPoint = Column(Integer)
    StandardCost = Column(Numeric(18, 2))
    ListPrice = Column(Numeric(18, 2))
    Size = Column(String(50))
    SizeUnitMeasureCode = Column(String(10))
    WeightUnitMeasureCode = Column(String(10))
    Weight = Column(Numeric(18, 2))
    DaysToManufacture = Column(Integer)
    ProductLine = Column(String(2))
    Class = Column(String(2))
    Style = Column(String(50))
    ProductSubcategoryID = Column(Integer)
    ProductModelID = Column(Integer)
    SellStartDate = Column(Date)
    SellEndDate = Column(Date)
    DiscontinuedDate = Column(Date)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

# Definindo a classe de modelo para SpecialOfferProduct
class SpecialOfferProduct(Base):
    __tablename__ = 'SpecialOfferProduct'
    SpecialOfferID = Column(Integer, primary_key=True)
    ProductID = Column(Integer, primary_key=True)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

# Definindo a classe de modelo para Customer
class Customer(Base):
    __tablename__ = 'Customer'
    CustomerID = Column(Integer, primary_key=True)
    PersonID = Column(Integer)
    StoreID = Column(Integer)
    TerritoryID = Column(Integer)
    AccountNumber = Column(String(50))
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

# Definindo a classe de modelo para Person
class Person(Base):
    __tablename__ = 'Person'
    BusinessEntityID = Column(Integer, primary_key=True)
    PersonType = Column(String(2))
    NameStyle = Column(Boolean)
    Title = Column(String(50))
    FirstName = Column(String(50))
    MiddleName = Column(String(50))
    LastName = Column(String(50))
    Suffix = Column(String(10))
    EmailPromotion = Column(Integer)
    AdditionalContactInfo = Column(Text)
    Demographics = Column(Text)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

# Mapeamento dos nomes dos arquivos para as classes de modelo
file_to_class_mapping = {
    'Sales.Customer': Customer,
    'Person.Person': Person,
    'Production.Product': Product,
    'Sales.SalesOrderDetail': SalesOrderDetail,
    'Sales.SalesOrderHeader': SalesOrderHeader,
    'Sales.SpecialOfferProduct': SpecialOfferProduct
}

# Carregar a URL do banco de dados e o caminho da pasta CSV das variáveis de ambiente
DATABASE_URL = os.getenv('DATABASE_URL')
CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH')

def create_db_engine():
    return create_engine(DATABASE_URL)

def create_tables(engine):
    Base.metadata.create_all(engine)

def insert_data_to_db(engine, df, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Dados inseridos com sucesso na tabela {table_name}.")
    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")

def replace_comma_with_dot_and_convert(df, columns_to_convert):
    for column in columns_to_convert:
        if column in df.columns and df[column].dtype == object:
            df[column] = df[column].str.replace(',', '.').astype(float)

def process_csv_files(engine, csv_folder):
    columns_to_fix = ['StandardCost', 'ListPrice', 'UnitPrice', 'UnitPriceDiscount', 'LineTotal', 'SubTotal', 'Freight', 'TotalDue', 'TaxAmt']
    for file_name in os.listdir(csv_folder):
        if file_name.endswith('.csv'):
            base_name_with_prefix, _ = os.path.splitext(file_name)
            if base_name_with_prefix in file_to_class_mapping:
                model_class = file_to_class_mapping[base_name_with_prefix]
                nome_tabela = model_class.__tablename__
                df = pd.read_csv(os.path.join(csv_folder, file_name), delimiter=';', encoding='utf-8')
                
                replace_comma_with_dot_and_convert(df, columns_to_fix)

                # Tratamento específico para campos booleanos
                if 'NameStyle' in df.columns:
                    df['NameStyle'] = df['NameStyle'].astype(bool)
                if 'MakeFlag' in df.columns:
                    df['MakeFlag'] = df['MakeFlag'].astype(bool)
                if 'FinishedGoodsFlag' in df.columns:
                    df['FinishedGoodsFlag'] = df['FinishedGoodsFlag'].astype(bool)   
                if 'OnlineOrderFlag' in df.columns:
                    df['OnlineOrderFlag'] = df['OnlineOrderFlag'].astype(bool)


                insert_data_to_db(engine, df, nome_tabela)
            else:
                print(f"Não foi encontrado mapeamento para o arquivo: {file_name}")

if __name__ == "__main__":
    if DATABASE_URL and CSV_FOLDER_PATH:
        engine = create_db_engine()
        create_tables(engine)
        process_csv_files(engine, CSV_FOLDER_PATH)
    else:
        print("As configurações de conexão ao banco de dados ou o caminho da pasta CSV não estão definidas.")


