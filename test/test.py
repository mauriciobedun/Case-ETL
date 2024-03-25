import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, select, Column, Integer, String, Boolean, Float, Numeric, Date, Text, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session
from sqlalchemy.exc import NoResultFound
from dotenv import load_dotenv

import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


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


# Criação do motor do banco de dados
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
Session = scoped_session(sessionmaker(bind=engine))


# Carregar a URL do banco de dados e o caminho da pasta CSV das variáveis de ambiente
CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH')


# Mapeamento dos nomes dos arquivos para as classes de modelo
file_to_class_mapping = {
    'Sales.Customer': Customer,
    'Person.Person': Person,
    'Production.Product': Product,
    'Sales.SalesOrderDetail': SalesOrderDetail,
    'Sales.SalesOrderHeader': SalesOrderHeader,
    'Sales.SpecialOfferProduct': SpecialOfferProduct
}


def insert_or_update_data(session, model_class, data_row):
    primary_keys = [key.name for key in model_class.__table__.primary_key]
    filter_args = {key: data_row[key] for key in primary_keys if key in data_row}

    try:
        existing_record = session.query(model_class).filter_by(**filter_args).one()
        for key, value in data_row.items():
            setattr(existing_record, key, value)
    except NoResultFound:
        new_record = model_class(**data_row)
        session.add(new_record)


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

            
def clean_nan_values(df):
    # Para colunas numéricas, converte `nan` para `None`
    numerical_columns = ['PersonID', 'StoreID', 'TerritoryID']  # Adicione outras colunas conforme necessário
    for column in numerical_columns:
        df[column] = df[column].apply(lambda x: None if pd.isna(x) else x)
    
    # Para colunas que já são do tipo objeto (incluindo strings), também trata strings vazias como `None`
    object_columns = [col for col in df.columns if df[col].dtype == object]
    for column in object_columns:
        df[column] = df[column].replace({np.nan: None, '': None})

        
def process_csv_files(engine, csv_folder):
    Session = scoped_session(sessionmaker(bind=engine, autoflush=False))
    session = Session()
    columns_to_fix = ['StandardCost', 'ListPrice', 'UnitPrice', 'UnitPriceDiscount', 'LineTotal', 'SubTotal', 'Freight', 'TotalDue', 'TaxAmt']
    
    try:
        for file_name, model_class in file_to_class_mapping.items():
            df = pd.read_csv(os.path.join(csv_folder, f"{file_name}.csv"), delimiter=';', encoding='utf-8')
            
            replace_comma_with_dot_and_convert(df, columns_to_fix)
            clean_nan_values(df)

            # Usando no_autoflush aqui
            with session.no_autoflush:
                for index, row in df.iterrows():
                    try:
                        insert_or_update_data(session, model_class, row.to_dict())
                    except Exception as e:
                        print(f"Erro ao inserir/atualizar a linha {index + 1} do arquivo {file_name}.csv")
                        print("Detalhes da linha com erro:", row.to_dict())
                        # Re-lança a exceção para interromper a execução e manter a mensagem de erro original
                        raise e
        
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Erro ao processar arquivos CSV: {e}")
    finally:
        session.close()





if __name__ == "__main__":
    if DATABASE_URL and CSV_FOLDER_PATH:
        engine = create_db_engine()
        create_tables(engine)
        process_csv_files(engine, CSV_FOLDER_PATH)
    else:
        print("As configurações de conexão ao banco de dados ou o caminho da pasta CSV não estão definidas.")

