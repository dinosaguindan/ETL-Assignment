import pandas as pd
import sqlite3
from datetime import datetime
from Config.configuration import *

class ProcessData:

    conn = sqlite3.connect(Database)

    def __init__(self):
        """Initialize the ProcessData."""

    #retrieve data table based on parameter value
    def getData(self, tableName):
        result = pd.read_sql("SELECT * FROM "+tableName, self.conn)
        return result

    #joining tables as requirement, based on ERD
    def joiningTables(self, customers, sales, orders,items):       
        merged_df1 = pd.merge(customers, sales, on='customer_id', how='inner')
        merged_df2 = pd.merge(merged_df1, orders, on='sales_id', how='inner')
        merged_df3 = pd.merge(merged_df2, items, on='item_id', how='inner')
        return merged_df3

    #assingin column name and datatypes
    def assignNamingAndDatatypes(self, aggJoinedTablesResult):
        result = aggJoinedTablesResult.astype({
            'customer_id': 'Int32',  
            'age': 'Int8',           
            'item_name': 'string',    
            'Quantity': 'Int32'       
        }).rename(columns={'customer_id': 'Customer',
            'age': 'Age',
            'item_name': 'Item'
        })
        return result

    #return filepath and filename for the location of csv, configurable in Config/configuration
    def getFilename(self):
        
        if(Overwrite) : 
            date='' 
        else: 
            date = ' - '+datetime.now().strftime("%Y%m%d%H%M%S")
        filename = TargetFilepath+TargetFilename+date+'.'+TargetFileExtension
        return filename
       



    

