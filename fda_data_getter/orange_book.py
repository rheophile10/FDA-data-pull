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
from .transformer import Transformer
from datetime import datetime
import numpy as np

class OrangeBook():
    def __init__(self, orange_book_url = 'https://www.fda.gov/media/76860/download', 
                                            raw_data_path = 'raw_data/'):
        self.orange_book_url = orange_book_url
        self.raw_data_path = raw_data_path
        
    def _get_unzipped_data(self):
        response = requests.get(self.orange_book_url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(self.raw_data_path)

    def get_book(self):
        print('processing orange book data')
        print(f'   getting orange book data from {self.orange_book_url}')
        self._get_unzipped_data()
        print('processing product data')
        products = Product('products.txt', self.raw_data_path)
        products.etl(products.name)
        trade_name_map = products.yield_trade_name_map()
        molecule_map = products.yield_molecule_map()
        print('processing exclusivity data')
        exclusivity = Exclusivity('exclusivity.txt', molecule_map, self.raw_data_path)
        exclusivity.etl(exclusivity.name)
        print('processing patent data')
        patents = Patent('patent.txt', trade_name_map, self.raw_data_path)
        patents.etl(patents.name)

class Product(Transformer):
    def __init__(self, raw_file, raw_data_path = 'raw_data/'):
        self.name = 'OBProd'
        self.raw_data = raw_data_path + raw_file
        super().__init__(self.raw_data, end_data='\\finished data\\',
                            final_columns = ['Entity_NonProp Name', 'Ingredient', 'DF',
                                'Route', 'Entity_Trade Name', 'Trade_Name', 'Applicant',
                                'Strength', 'Appl_Type', 'Entity_Trade Name_AP#PR#',
                                'Entity_Trade Name_AP#PR#_App Date', 'Entity_AP#PR#',
                                'Appl_No', 'Product_No', 'TE_Code', 'Entity_Approval_Date',
                                'RLD', 'RS', 'Type', 'Applicant_Full_Name', 'Source',
                                'Text Approval Date'])
        
    def _transform_Entity_NonProp_Name(self):
        def Entity_NonProp_Name(row):
            ingredient = row['Ingredient'].upper()
            return f'{ingredient} (NonProp Name)'
        self.data['Entity_NonProp Name'] = self.data.apply(Entity_NonProp_Name, axis = 1)
    
    def _transform_DF_Route_split(self):
        self.data['DF'] = self.data['DF;Route'].apply(lambda row: row.split(';')[0])
        self.data['Route'] = self.data['DF;Route'].apply(lambda row: row.split(';')[1]) 
    
    def _transform_Entity_Trade_Name(self):
        def Entity_Trade_Name(row):
            trade_name = row['Trade_Name'].upper()
            return f'{trade_name} (Trade Name)'
        self.data['Entity_Trade Name'] = self.data.apply(Entity_Trade_Name, axis=1)
    
    def _transform_Entity_Trade_Name_AP_PR_(self):
        def Entity_Trade_Name_AP_PR_(row):
            trade_name = row['Trade_Name'].upper()
            ap = row['Appl_No']
            pr = row['Product_No']
            return f'{trade_name} AP#{ap} PR#{pr}'
        self.data['Entity_Trade Name_AP#PR#'] = self.data.apply(Entity_Trade_Name_AP_PR_, axis=1)

    def _transform_Entity_Trade_Name_AP_PR_App_Date(self):
        def Entity_Trade_Name_AP_PR_App_Date(row):
            trade_name = row['Trade_Name'].upper()
            ap = row['Appl_No']
            pr = row['Product_No']
            appr_date = 'Jan 1, 1982' if row['Approval_Date'] == 'Approved Prior to Jan 1, 1982' else row['Approval_Date'] 
            appr_date = datetime.strptime(appr_date,'%b %d, %Y').strftime('%#m/%#d/%Y')
            return f'Appr Date {trade_name} AP#{ap}PR#{pr}-{appr_date}'
        self.data['Entity_Trade Name_AP#PR#_App Date'] = self.data.apply(Entity_Trade_Name_AP_PR_App_Date, axis=1)
    
    def _transform_Entity_App_PR_(self):
        def Entity_App_PR_(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            return f'AP#{ap}PR#{pr}'
        self.data['Entity_AP#PR#'] = self.data.apply(Entity_App_PR_, axis = 1)
        
    def _transform_Entity_Approval_Date(self):
        def convert_text_date_to_excel_ordinal(datetime) :
            offset = 693594
            n = datetime.toordinal()
            return (n - offset)

        def get_approval_date(row):
            date = 'Jan 1, 1982' if row['Approval_Date'] == 'Approved Prior to Jan 1, 1982' else row['Approval_Date'] 
            return datetime.strptime(date,'%b %d, %Y')
                
        self.data['Entity_Approval_Date']=self.data.apply(lambda row: convert_text_date_to_excel_ordinal(get_approval_date(row)), axis = 1)
        self.data['Text Approval Date']=self.data.apply(lambda row: get_approval_date(row).strftime('%#m/%#d/%Y'), axis = 1)

    def _transform_Source(self):
        self.data['Source'] = 'FDA Orange Book'

    def yield_molecule_map(self):
        records = self.data[['Ingredient', 'Appl_No', 'Product_No']].to_dict(orient='records')
        molecule_map = dict()
        for record in records:
            ap = record['Appl_No']
            pr = record['Product_No']
            ingredient = record['Ingredient'].upper()
            if ap in molecule_map.keys():
                molecule_map[ap][pr] = ingredient
            else:
                molecule_map[ap] = {pr:ingredient}
        return molecule_map

    def yield_trade_name_map(self):
        records = self.data[['Trade_Name', 'Appl_No', 'Product_No']].to_dict(orient='records')
        trade_name_map = dict()
        for record in records:
            ap = record['Appl_No']
            pr = record['Product_No']
            trade_name = record['Trade_Name'].upper()
            if ap in trade_name_map.keys():
                trade_name_map[ap][pr] = trade_name
            else:
                trade_name_map[ap] = {pr:trade_name}
        return trade_name_map

class Exclusivity(Transformer):
    def __init__(self, raw_file, molecule_map, raw_data_path = 'raw_data/'):
        self.name = 'OBExcl'
        self.raw_data = raw_data_path + raw_file
        self.molecule_map = molecule_map
        super().__init__(self.raw_data, end_data = '\\finished data\\', 
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
            excl_date = datetime.strptime(row['Exclusivity_Date'],'%b %d, %Y').strftime('%#m/%#d/%Y')
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
            molecule = self.molecule_map[ap][pr]
            return f'{molecule} AP#{ap} PR#{pr}'
        self.data['Entity_Trade_AP#PR#'] = self.data.apply(Entity_Trade_AP_PR_, axis = 1)

    def _transform_add_dates_and_source(self):
        self.data['Entity_Exclusivity_Date']=self.data['Exclusivity_Date'].apply(lambda date: datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%Y'))
        self.data['Text_Exclusivity Date']=self.data['Exclusivity_Date'].apply(lambda date: datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%Y'))

    def _transform_Source(self):
        self.data['Source'] = 'FDA Orange Book'

class Patent(Transformer):
    def __init__(self, raw_file, trade_name_map, raw_data_path = 'raw_data/'):
        self.name = 'OBPat'
        self.raw_data = raw_data_path + raw_file
        self.trade_name_map = trade_name_map
        super().__init__(self.raw_data, end_data = '\\finished data\\', 
                            final_columns = ['Appl_Type', 'Entity_AP#PR#',
                                'Appl_No', 'Product_No', 'Entity_Pat Sub_Combined',
                                'Entity_Pat Exp_Combined', 'Entity_Pat#_Trade_AP#PR#',
                                'Entity_Pat#', 'Patent_No', 'Entity_Patent_Expire_Date',
                                'Drug_Substance_Flag', 'Drug_Product_Flag', 'Patent_Use_Code',
                                'Delist_Flag', 'Entity_Submission_Date', 'Source', 
                                'Text_Patent_Expire_Date_Text', 'Text_Submission_Date',
                                'Entity_Trade_AP#PR#2', 'Trade Name', 'Column1'])
    
    def _transform_Column1(self):
        def column1(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            return f'#{ap}PR#{pr}'
        self.data['Column1'] = self.data.apply(column1, axis = 1)
    
    def _transform_Trade_Name(self):
        def Trade_Name(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            trade_name = self.trade_name_map[ap][pr]
            return trade_name            
        self.data['Trade Name'] = self.data.apply(Trade_Name, axis = 1)

    def _transform_Text_Submission_Date(self):
        def Text_Submission_Date(date):
            return datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%Y') if not date != date else np.nan
        self.data['Text_Submission_Date']=self.data['Submission_Date'].apply(lambda date: Text_Submission_Date(date))

    def _transform_Text_Patent_Expire_Date_Text(self):
        def Text_Submission_Date(date):
            return datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%Y') if not date != date else np.nan
        self.data['Text_Patent_Expire_Date_Text']=self.data['Patent_Expire_Date_Text'].apply(lambda date: Text_Submission_Date(date))
    
    def _transform_Entity_Submission_Date(self):
        def Text_Submission_Date(date):
            return datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%Y') if not date != date else np.nan
        self.data['Entity_Submission_Date']=self.data['Submission_Date'].apply(lambda date: Text_Submission_Date(date))

    def _transform_Entity_Patent_Expire_Date(self):
        def Text_Submission_Date(date):
            return datetime.strptime(date,'%b %d, %Y').strftime('%#m/%#d/%Y') if not date != date else np.nan
        self.data['Entity_Patent_Expire_Date']=self.data['Patent_Expire_Date_Text'].apply(lambda date: Text_Submission_Date(date))
    
    def _transform_Entity_Pat_(self):
        def Entity_Pat(patent_no):
            return f'Pat#{patent_no}'
        self.data['Entity_Pat#'] = self.data['Patent_No'].apply(lambda patent_no: Entity_Pat(patent_no))
    
    def _transform_Entity_Pat_Exp_Combined(self):
        def entity_pat_exp_combined(row):
            use_code = row['Patent_Use_Code']
            patent = row['Patent_No']
            ap = row['Appl_No']
            pr = row['Product_No']
            exp_date = row['Patent_Expire_Date_Text']
            exp_date = datetime.strptime(exp_date,'%b %d, %Y').strftime('%#m/%#d/%Y')
            return f'Pat Exp ({use_code}) Pat#{patent} AP#{ap}PR#{pr}-{exp_date}'
        self.data['Entity_Pat Exp_Combined'] = self.data.apply(entity_pat_exp_combined, axis = 1)
    
    def _transform_Entity_Pat_Sub_Combined(self):
        def entity_pat_sub_combined(row):
            use_code = row['Patent_Use_Code']
            patent = row['Patent_No']
            ap = row['Appl_No']
            pr = row['Product_No']
            sub_date = row['Submission_Date']
            if sub_date != sub_date:
                sub_date = ''
            else:
                sub_date = datetime.strptime(sub_date,'%b %d, %Y').strftime('%#m/%#d/%Y') 
            return f'Pat Sub  ({use_code}) Pat#{patent} AP#{ap}PR#{pr}-{sub_date}'
        self.data['Entity_Pat Sub_Combined'] = self.data.apply(entity_pat_sub_combined, axis = 1)

    def _transform_entity_Pat_Trade_AP_PR(self):
        def entity_Pat_Trade_AP_PR(row):
            patent = row['Patent_No']
            ap = row['Appl_No']
            pr = row['Product_No']
            trade_name = self.trade_name_map[ap][pr]
            return f'Pat#{patent} {trade_name} AP#{ap}PR#{pr}'
        self.data['Entity_Pat#_Trade_AP#PR#'] = self.data.apply(entity_Pat_Trade_AP_PR, axis=1)

    def _transform_Entity_App_PR_(self):
        def Entity_App_PR_(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            return f'AP#{ap}PR#{pr}'
        self.data['Entity_AP#PR#'] = self.data.apply(Entity_App_PR_, axis = 1)

    def _transform_Entity_Trade_AP_PR_2(self):
        def Entity_Trade_AP_PR_(row):
            ap = row['Appl_No']
            pr = row['Product_No']
            trade_name = self.trade_name_map[ap][pr]
            return f'{trade_name} AP#{ap} PR#{pr}'
        self.data['Entity_Trade_AP#PR#2'] = self.data.apply(Entity_Trade_AP_PR_, axis = 1)
    
    def _transform_Source(self):
        self.data['Source'] = 'FDA Orange Book'
    