'''
Data sources and File formats:
A. ORANGE BOOK: The first three files are from the Orange Book. They need to be transformed into the format in the corresponding MS Excel Spread Sheets:
1. OBProd_STAN_17_Sep (This is products)
2. OBPat_STAN_14_Sep (This is patents)
3. OBExcl_STAN_14_Sep (This is exclusivity)
Website: https://www.fda.gov/drugs/drug-approvals-and-databases/orange-book-data-files
They are downloaded in a zip file with three CSV files, one each for products, patents, and exclusivity.
'''

import requests
import zipfile
import io
from transformer import Transformer
from datetime import datetime

class OrangeBook():
    def __init__(self, orange_book_url = 'https://www.fda.gov/media/76860/download', 
                                            raw_data_path = '/raw_data/'):
        self.orange_book_url = orange_book_url
        self.raw_data_path = raw_data_path
        
    def get_unzipped_data(self):
        response = requests.get(self.orange_book_url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(self.raw_data_path)

class Product(Transformer):
    def __init__(self, raw_file, products_map, raw_data_path = '/raw_data/'):
        self.name = 'OBProd'
        self.raw_data = raw_data_path + raw_file
        self.products = products_map
        super().__init__(self.raw_data_path + raw_file, end_data='/finished data/',
                            final_columns = ['Entity_NonProp Name', 'Ingredient', 'DF',
                                'Route', 'Entity_Trade Name', 'Trade_Name', 'Applicant',
                                'Strength', 'Appl_Type', 'Entity_Trade Name_AP#PR#',
                                'Entity_Trade Name_AP#PR#_App_Date', 'Entity_AP#PR#',
                                'Appl_No', 'Product_No', 'TE_Code', 'Entity_Approval_Date',
                                'RLD', 'RS', 'Type', 'Applicant_Full_Name', 'Source',
                                'Text Approval Date'])
        
    def _transform_Entity_NonProp_Name(self):
        def Entity_NonProp_Name(row):
            ingredient = row['Ingredient']
            return f'{ingredient} (NonProp Name)'
        self.data['Entity_NonProp Name'] = self.data.apply(Entity_NonProp_Name, axis = 1)
    
    def _transform_DF_Route_split(self):
        self.data['DF'] = self.data['DF;Route'].apply(lambda row: row.split(';')[0])
        self.data['Route'] = self.data['DF;Route'].apply(lambda row: row.split(';')[1]) 
    
    def _transform_Entity_Trade_Name(self):
        def Entity_Trade_Name(row):
            trade_name = row['Trade_Name']
            return f'{trade_name} (Trade Name)'
        self.data['Entity_Trade Name'] = self.data.apply(Entity_Trade_Name, axis=1)
    
    def _transofrm_Entity_Trade_Name_AP_PR_(self):
        def Entity_Trade_Name_AP_PR_(row):
            trade_name = row['Trade_Name']
            ap = row['Appl_No']
            pr = row['Product_No']
            return f'{trade_name} AP#{ap} PR#{pr}'
        self.data['Entity_Trade Name_AP#PR#'] = self.data.apply(Entity_Trade_Name_AP_PR_, axis=1)

    def _transform_Entity_Trade_Name_AP_PR_App_Date(self):
        def Entity_Trade_Name_AP_PR_App_Date(row):
            trade_name = row['Trade_Name']
            ap = row['Appl_No']
            pr = row['Product_No']
            appr_date = datetime.strptime(row['Approval_Date'],'%b %d, %Y').strftime('%#m/%#d/%y')
            return f'Appr Date {trade_name} AP#{ap}PR#{pr}-{appr_date}'
        self.data['Entity_Trade Name_AP#PR#_App Date'] = self.data.apply(Entity_Trade_Name_AP_PR_App_Date, axis=1)
    
    def _transform_Entity_App_PR_(self):
        def Entity_App_PR_(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            return f'AP#{ap}PR#{pr}'
        self.data['Entity_App#PR#'] = self.data.apply(Entity_App_PR_, axis = 1)
        
    def _transform_Entity_Approval_Date(self):
        def convert_text_date_to_excel_ordinal(datetime) :
            offset = 693594
            n = datetime.toordinal()
            return (n - offset)
        self.data['Entity_Approval_Date']=self.data['Exclusivity_Date'].apply(lambda date: convert_text_date_to_excel_ordinal(datetime.strptime(date,'%b %d, %Y')))
        self.data['Text Approval Date']=self.data['Exclusivity_Date'].apply(lambda date: datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%y'))
        pass

    def _transform_Source(self):
        self.data['Source'] = 'FDA Orange Book'

class Exclusivity(Transformer):
    def __init__(self, raw_file, products_map, raw_data_path = '/raw_data/'):
        self.name = 'OBExcl'
        self.raw_data = raw_data_path + raw_file
        self.products = products_map
        super().__init__(self.raw_data, end_data = '/finished data/', 
                            final_columns = ['Appl_Type', 
                                'Entity_Excl Date_Combined', 'Entity_App#PR#', 
                                'Entity_Trade_AP#PR#', 'Appl_No', 'Product_No', 
                                'Exclusivity_Code', 'Entity_Exclusivity_Date',
                                'Text_Exclusivity Date','Source'])

    def _transform_Entity_Excl_Date_Combined(self):
        def Entity_Excl_Date_Combined(row):
            excl_code = row['Exclusivity_Code']
            ap = row['Appl_No']
            pr = row['Product_No']
            excl_date = datetime.strptime(row['Exclusivity_Date'],'%b %d, %Y').strftime('%#m/%#d/%y')
            return f'Excl Date ({excl_code}) AP#{ap}PR#{pr}-{excl_date}'
        self.data['Entity_Excl Date_Combined'] = self.data.apply(Entity_Excl_Date_Combined, axis = 1)

    def _transform_Entity_App_PR_(self):
        def Entity_App_PR_(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            return f'AP#{ap}PR#{pr}'
        self.data['Entity_App#PR#'] = self.data.apply(Entity_App_PR_, axis = 1)
    
    def _transform_Entity_Trade_AP_PR_(self):
        def Entity_Trade_AP_PR_(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            molecule = self.products[ap][pr]
            return f'{molecule} AP#{ap} PR#{pr}'
        self.data['Entity_Trade_AP#PR#'] = self.data.apply(Entity_Trade_AP_PR_, axis = 1)

    def _transform_add_dates_and_source(self):
        self.data['Entity_Exclusivity_Date']=self.data['Exclusivity_Date'].apply(lambda date: datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%y'))
        self.data['Text_Exclusivity Date']=self.data['Exclusivity_Date'].apply(lambda date: datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%y'))

    def _transform_Source(self):
        self.data['Source'] = 'FDA Orange Book'