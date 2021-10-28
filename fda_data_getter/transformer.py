import pandas as pd
from datetime import datetime
import os

class Transformer():
    def __init__(self, source_data, end_data, final_columns):
        self.source_data = source_data
        self.final_columns = final_columns
        self.data = []
        self.end_data = end_data
    
    def _extract(self):
        print(f'extracting {self.source_data}')
        return pd.read_csv(self.source_data, sep='~')
    
    def _transform(self):
        transformations = [getattr(self, method) for method in dir(self) if method.startswith('_transform_')]
        print('transforming data')
        for transformation in transformations:
            print(f'    applying {transformation.__name__} to data')
            transformation()
        self.data = self.data[self.final_columns]
    
    def _load(self, filename_prefix):
        print('loading data')
        filename = os.getcwd()+self.end_data + filename_prefix + datetime.strftime(datetime.utcnow(),'_STAN_%d_%b.xlsx')
        self.data.to_excel(filename, index=False)

    def etl(self, filename_prefix):
        self.data = self._extract()
        self._transform()
        self._load(filename_prefix)

