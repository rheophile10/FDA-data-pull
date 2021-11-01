'''
6. NDC_STAN_14_Sep (This is a listing of Drug Codes)
Website: https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory
From this site download the MS Excel Zip file labeled: NDC database file - Excel version (zip format)
There is a column “K” in this file labelled “Marketing Category”. Any row with this column labeled “Unapproved Homeopathic” can be deleted.
'''

import requests
import zipfile
import io
from .transformer import Transformer
import pandas as pd
import os
from datetime import datetime

class NDCBook():
    def __init__(self, ndc_url = 'https://www.accessdata.fda.gov/cder/ndcxls.zip', 
                        raw_data_path = 'raw_data/', format = 'xlsx'):
        self.ndc_url = ndc_url
        self.raw_data_path = raw_data_path
        self.format = format

    def _get_data(self):
        response = requests.get(self.ndc_url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall(self.raw_data_path)
        files = [self.raw_data_path+'package.xls', self.raw_data_path+'product.xls']
        for file in files:
            base = file.split('.')[0]
            new_file = base+'.txt'
            if os.path.exists(new_file):
                os.remove(new_file)
            os.rename(file, new_file)

    def get_book(self):
        print('getting NDC data')
        self._get_data()
        print('processing NDC data')
        ndc = NDC('product.txt', self.raw_data_path, self.format)
        ndc.etl(ndc.name)

class NDC(Transformer):
    def __init__(self, raw_file, raw_data_path = 'raw_data/', format='xlsx'):
        self.name = 'NDC'
        self.raw_data = raw_data_path + raw_file
        self.format = format
        super().__init__(self.raw_data, end_data='\\finished data\\',format=self.format,
                            final_columns = ['PRODUCTID',
                                            'PRODUCTNDC',
                                            'PRODUCTTYPENAME',
                                            'Entity-Trade Name',
                                            'Entity_Start Mkt Date_Combined',
                                            'Entity_End Mkt Date_Combined',
                                            'Entity_Trade Name_NDC',
                                            'PROPRIETARYNAME',
                                            'PROPRIETARYNAMESUFFIX',
                                            'Entity_NonProp Name',
                                            'NONPROPRIETARYNAME',
                                            'DOSAGEFORMNAME',
                                            'ROUTENAME',
                                            'Entity_Start Mkt Date',
                                            'Entity_End Mkt Date',
                                            'MARKETINGCATEGORYNAME',
                                            'APPLICATIONNUMBER',
                                            'Entity_Labeler',
                                            'LABELERNAME',
                                            'SUBSTANCENAME',
                                            'ACTIVE_NUMERATOR_STRENGTH',
                                            'ACTIVE_INGRED_UNIT',
                                            'Entity_PHARM_CLASSES',
                                            'DEASCHEDULE',
                                            'NDC_EXCLUDE_FLAG',
                                            'LISTING_RECORD_CERTIFIED_THROUGH',
                                            'Source',
                                            'Text_Start Mkt Date',
                                            'Text_End Mkt Date'])

    def _extract(self):
        print(f'extracting {self.source_data}')
        return pd.read_csv(self.source_data, sep='\t', encoding='cp1252')
    
    def upper_row(self, row, col):
        return '' if row[col] != row[col] else row[col].upper()

    def _transform_filter_homeo_out(self):
        self.data = self.data[self.data['MARKETINGCATEGORYNAME']!='UNAPPROVED HOMEOPATHIC']
    
    def _transform_entity_labeler(self):
        def entity_labeler(row):
            return self.upper_row(row, 'LABELERNAME')
        self.data['Entity_Labeler'] = self.data.apply(entity_labeler, axis=1)
        
    def _transform_entity_end_mkt_date_combined_(self):
        def entity_end_mkt_date_combined_(row):
            name=self.upper_row(row,'PROPRIETARYNAME')
            ndc = row['PRODUCTNDC']
            date = '' if row['ENDMARKETINGDATE']!=row['ENDMARKETINGDATE'] else datetime.strptime(str(int(row['ENDMARKETINGDATE'])),'%Y%m%d').strftime('%m/%d/%Y')
            return f'End Mkt Date {name} NDC{ndc}-{date}'
        self.data['Entity_End Mkt Date_Combined'] = self.data.apply(entity_end_mkt_date_combined_, axis=1)
        
    def _transform_entity_end_mkt_date(self):
        def entity_end_mkt_date(row):
            return '' if row['ENDMARKETINGDATE']!=row['ENDMARKETINGDATE'] else datetime.strptime(str(int(row['ENDMARKETINGDATE'])),'%Y%m%d').strftime('%m/%d/%Y')
        self.data['Entity_End Mkt Date'] = self.data.apply(entity_end_mkt_date, axis=1)
        
    def _transform_text_start_mkt_date(self):
        def text_start_mkt_date(row):
            return self.date_formula('' if row['STARTMARKETINGDATE']!=row['STARTMARKETINGDATE'] else datetime.strptime(str(int(row['STARTMARKETINGDATE'])),'%Y%m%d').strftime('%m/%d/%Y'))
        self.data['Text_Start Mkt Date'] = self.data.apply(text_start_mkt_date, axis=1)
        
    def _transform_entity_start_mkt_date_combined(self):
        def entity_start_mkt_date_combined(row):
            date = '' if row['STARTMARKETINGDATE']!=row['STARTMARKETINGDATE'] else datetime.strptime(str(int(row['STARTMARKETINGDATE'])),'%Y%m%d').strftime('%m/%d/%Y')
            name=self.upper_row(row,'PROPRIETARYNAME')
            ndc = row['PRODUCTNDC']
            return f'Start Mkt Date {name} NDC{ndc}-{date}'
        self.data['Entity_Start Mkt Date_Combined'] = self.data.apply(entity_start_mkt_date_combined, axis=1)
        
    def _transform_entity_trade_name_ndc(self):
        def entity_trade_name_ndc(row):
            name= self.upper_row(row,'PROPRIETARYNAME')
            ndc = row['PRODUCTNDC']
            return f'{name} NDC{ndc}'
        self.data['Entity_Trade Name_NDC'] = self.data.apply(entity_trade_name_ndc, axis=1)
        
    def _transform_text_end_mkt_date(self):
        def text_end_mkt_date(row):
            return self.date_formula('' if row['ENDMARKETINGDATE']!=row['ENDMARKETINGDATE'] else datetime.strptime(str(int(row['ENDMARKETINGDATE'])),'%Y%m%d').strftime('%m/%d/%Y'))
        self.data['Text_End Mkt Date'] = self.data.apply(text_end_mkt_date, axis=1)
        
    def _transform_entity_pharm_classes(self):
        self.data['Entity_PHARM_CLASSES'] = self.data['PHARM_CLASSES']
        
    def _transform_entity_nonprop_name(self):
        def entity_nonprop_name(row):
            name = self.upper_row(row, 'NONPROPRIETARYNAME')
            return f'{name} (NonProp Name)'
        self.data['Entity_NonProp Name'] = self.data.apply(entity_nonprop_name, axis=1)
        
    def _transform_source(self):
        self.data['Source'] = 'FDA Nationla Drug Code Directory'
        
    def _transform_entity_start_mkt_date(self):
        def entity_start_mkt_date(row):
            return '' if row['STARTMARKETINGDATE']!=row['STARTMARKETINGDATE'] else datetime.strptime(str(int(row['STARTMARKETINGDATE'])),'%Y%m%d').strftime('%m/%d/%Y')
        self.data['Entity_Start Mkt Date'] = self.data.apply(entity_start_mkt_date, axis=1)
        
    def _transform_entity_trade_name(self):
        def entity_trade_name(row):
            name = self.upper_row(row, 'PROPRIETARYNAME')
            return f'{name} (Trade Name)'
        self.data['Entity-Trade Name'] = self.data.apply(entity_trade_name, axis=1)