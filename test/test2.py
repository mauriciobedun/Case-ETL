import os
import pandas as pd
import numpy as np
# from sqlalchemy import create_engine
from sqlalchemy import ForeignKey, create_engine, select, Column, Integer, String, Boolean, Float, Numeric, Date, Text, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session, relationship
from sqlalchemy.exc import NoResultFound
from dotenv import load_dotenv
import logging

# Configuração do Logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Carregamento das variáveis de ambiente
load_dotenv()

# Definição da Base Declarativa
Base = declarative_base()

class SalesOrderDetail(Base):
    __tablename__ = 'SalesOrderDetail'
    SalesOrderID = Column(Integer, primary_key=True)
    SalesOrderDetailID = Column(Integer, ForeignKey('SalesOrder.DetailID'))  # Alterado para indicar uma chave estrangeira
    CarrierTrackingNumber = Column(String(255))
    OrderQty = Column(Integer)
    ProductID = Column(Integer)
    SpecialOfferID = Column(Integer)
    UnitPrice = Column(Numeric(10, 2))  # Mantido como Numeric
    UnitPriceDiscount = Column(Numeric(10, 2))
    LineTotal = Column(Numeric(10, 2))
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

    # Relacionamento (opcional, dependendo se você quer acessar SalesOrder diretamente de SalesOrderDetail)
    # sales_order = relationship("SalesOrder", back_populates="sales_order_details")


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
    CustomerID = Column(Integer, ForeignKey('Customer.ID'))  
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

    # Relacionamento opcional para acessar detalhes do cliente diretamente
    # customer = relationship("Customer", back_populates="sales_order_headers")


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
    ProductSubcategoryID = Column(Integer, ForeignKey('ProductSubcategory.ID'))
    ProductModelID = Column(Integer, ForeignKey('ProductModel.ID'))
    SellStartDate = Column(Date)
    SellEndDate = Column(Date)
    DiscontinuedDate = Column(Date)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

    # Relacionamentos (opcional)
    # subcategory = relationship("ProductSubcategory", back_populates="products")
    # model = relationship("ProductModel", back_populates="products")


class SpecialOfferProduct(Base):
    __tablename__ = 'SpecialOfferProduct'
    SpecialOfferID = Column(Integer, ForeignKey('SpecialOffer.SpecialOfferID'), primary_key=True)
    ProductID = Column(Integer, ForeignKey('Product.ProductID'), primary_key=True)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

    # Relacionamentos (opcional)
    # special_offer = relationship("SpecialOffer", back_populates="products")
    # product = relationship("Product", back_populates="special_offers")


from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

class Customer(Base):
    __tablename__ = 'Customer'
    CustomerID = Column(Integer, primary_key=True)
    PersonID = Column(Integer, ForeignKey('Person.BusinessEntityID'), nullable=True)
    StoreID = Column(Integer, ForeignKey('Store.StoreID'), nullable=True)
    TerritoryID = Column(Integer, ForeignKey('SalesTerritory.TerritoryID'), nullable=True)
    AccountNumber = Column(String(50), unique=True)  # Supondo que AccountNumber deva ser único
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

    # Relacionamentos (opcional, caso existam essas entidades)
    # person = relationship("Person", back_populates="customers")
    # store = relationship("Store", back_populates="customers")
    # territory = relationship("SalesTerritory", back_populates="customers")



class Person(Base):
    __tablename__ = 'Person'
    BusinessEntityID = Column(Integer, primary_key=True)
    PersonType = Column(String(2))
    NameStyle = Column(Boolean)
    Title = Column(String(50), nullable=True)  # Assumindo que Title pode ser nulo
    FirstName = Column(String(50))
    MiddleName = Column(String(50), nullable=True)  # Assumindo que MiddleName pode ser nulo
    LastName = Column(String(50))
    Suffix = Column(String(10), nullable=True)  # Assumindo que Suffix pode ser nulo
    EmailPromotion = Column(Integer, nullable=True)  # Se EmailPromotion pode ser nulo
    AdditionalContactInfo = Column(Text, nullable=True)  # Se pode ser nulo
    Demographics = Column(Text, nullable=True)  # Se pode ser nulo
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

    # Relacionamento com Customer
    # customers = relationship("Customer", back_populates="person")


# Conexão com o Banco de Dados
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
Session = scoped_session(sessionmaker(bind=engine, autoflush=False))

# Caminho da pasta CSV
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

def replace_comma_with_dot_and_convert(df, columns_to_convert):
    for column in columns_to_convert:
        if column in df.columns and df[column].dtype == object:
            df[column] = df[column].str.replace(',', '.').astype(float)

def clean_nan_values(df):
    for column in df.columns:
        if df[column].dtype == float or df[column].dtype == object:
            df[column] = df[column].replace({np.nan: None, '': None})
            
def process_csv_files(engine, csv_folder):
    session = Session()
    columns_to_fix = ['StandardCost', 'ListPrice', 'UnitPrice', 'UnitPriceDiscount', 'LineTotal', 'SubTotal', 'Freight', 'TotalDue', 'TaxAmt']
    
    try:
        for file_name, model_class in file_to_class_mapping.items():
            df = pd.read_csv(os.path.join(csv_folder, f"{file_name}.csv"), delimiter=';', encoding='utf-8')
            replace_comma_with_dot_and_convert(df, columns_to_fix)
            clean_nan_values(df)

            for index, row in df.iterrows():
                try:
                    insert_or_update_data(session, model_class, row.to_dict())
                except Exception as e:
                    print(f"Erro ao inserir/atualizar a linha {index + 1} do arquivo {file_name}.csv")
                    print("Detalhes da linha com erro:", row.to_dict())
                    raise e
        
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Erro ao processar arquivos CSV: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    if DATABASE_URL and CSV_FOLDER_PATH:
        create_tables(engine)  # Certifique-se de que a função create_tables está definida
        process_csv_files(engine, CSV_FOLDER_PATH)
    else:
        print("As configurações de conexão ao banco de dados ou o caminho da pasta CSV não estão definidas.")
