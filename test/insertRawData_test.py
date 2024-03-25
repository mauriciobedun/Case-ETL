import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session, relationship
from sqlalchemy import Column, Integer, String, Float, Numeric, Boolean, Date, DateTime, Text, ForeignKey
from dotenv import load_dotenv

from sqlalchemy.orm import relationship

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH')

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
    sales_order = relationship("SalesOrder", back_populates="sales_order_details")


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
    customer = relationship("Customer", back_populates="sales_order_headers")


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
    subcategory = relationship("ProductSubcategory", back_populates="products")
    model = relationship("ProductModel", back_populates="products")


class SpecialOfferProduct(Base):
    __tablename__ = 'SpecialOfferProduct'
    SpecialOfferID = Column(Integer, ForeignKey('SpecialOffer.SpecialOfferID'), primary_key=True)
    ProductID = Column(Integer, ForeignKey('Product.ProductID'), primary_key=True)
    rowguid = Column(String)
    ModifiedDate = Column(DateTime)

    # Relacionamentos (opcional)
    special_offer = relationship("SpecialOffer", back_populates="products")
    product = relationship("Product", back_populates="special_offers")




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

engine = create_engine(DATABASE_URL)

# Função para criar tabelas
def create_tables():
    Base.metadata.create_all(engine)

# Função para processar e inserir dados dos CSVs
def process_and_insert_data():
    for filename in os.listdir(CSV_FOLDER_PATH):
        if filename.endswith(".csv"):
            filepath = os.path.join(CSV_FOLDER_PATH, filename)
            # Inferir o nome da tabela a partir do nome do arquivo
            table_name = filename[:-4]  # Remove a extensão '.csv'
            # Ler o CSV
            df = pd.read_csv(filepath)
            # Inserir dados no banco de dados
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Dados inseridos na tabela {table_name}")

if __name__ == "__main__":
    create_tables()
    process_and_insert_data()
