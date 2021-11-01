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

class NDCBook():
    def __init__(self, ndc_url = 'https://www.accessdata.fda.gov/cder/ndcxls.zip', 
                                            raw_data_path = 'raw_data/'):
        self.ndc_url = ndc_url
        self.raw_data_path = raw_data_path

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
        ndc = NDC('product.txt', self.raw_data_path)
        ndc.etl(ndc.name)

class NDC(Transformer):
    def __init__(self, raw_file, raw_data_path = 'raw_data/'):
        self.name = 'NDC'
        self.raw_data = raw_data_path + raw_file
        super().__init__(self.raw_data, end_data='\\finished data\\',
                            final_columns = ['PRODUCTID',
                                        'PRODUCTNDC',
                                        'PRODUCTTYPENAME',
                                        'PROPRIETARYNAME',
                                        'PROPRIETARYNAMESUFFIX',
                                        'NONPROPRIETARYNAME',
                                        'DOSAGEFORMNAME',
                                        'ROUTENAME',
                                        'STARTMARKETINGDATE',
                                        'ENDMARKETINGDATE',
                                        'MARKETINGCATEGORYNAME',
                                        'APPLICATIONNUMBER',
                                        'LABELERNAME',
                                        'SUBSTANCENAME',
                                        'ACTIVE_NUMERATOR_STRENGTH',
                                        'ACTIVE_INGRED_UNIT',
                                        'PHARM_CLASSES',
                                        'DEASCHEDULE',
                                        'NDC_EXCLUDE_FLAG',
                                        'LISTING_RECORD_CERTIFIED_THROUGH'])

    def _extract(self):
        print(f'extracting {self.source_data}')
        return pd.read_csv(self.source_data, sep='\t', encoding='cp1252')
    
    def _transform_filter_homeo_out(self):
        self.data = self.data[self.data['MARKETINGCATEGORYNAME']!='UNAPPROVED HOMEOPATHIC']