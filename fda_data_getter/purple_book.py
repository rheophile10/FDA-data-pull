'''
B. PURPLE BOOK: The Second two files are from the Purple Book. They need to be transformed into the Formats in the corresponding MS Excel Spread Sheets:
4. PB_STAN_14_Sep (The Purple book is a listing of Biologic Drugs)
Website: https://purplebooksearch.fda.gov/downloads
The files for “4. PB_STAN_14_Sep” only requires downloading the most recent month. It is downloaded as as a CSV file, but unlike the Orange Book files it looks like an MS Excel Spreadsheet
There is a break in the file, only the data after the break needs to be included. The data above the break is repeated below the break and just pulled to the top to highlight that it has changed.
5. PB_Pat_STAN_13_Sep (These are biologic patents)
Website: https://purplebooksearch.fda.gov/patent-list
This data can be downloaded in a variety of formats, excel, CSV, PDF
'''


import requests
from datetime import datetime, timedelta
import pandas as pd
from .transformer import Transformer

class PurpleBook():
    def __init__(self, raw_data_path = 'raw_data/', format='xlsx'):
        self.raw_data_path = raw_data_path
        self.format = format
        
    def _get_biologics(self):
        def find_date(date):
            date = date.replace(day=1)
            year = date.strftime('%Y')
            month = date.strftime('%B').lower()
            url = f'https://purplebooksearch.fda.gov/files/{year}/purplebook-search-{month}-data-download.csv'
            response = requests.get(url)
            data = response.content
            if data[:15] == b'<!DOCTYPE html>':
                date = date - timedelta(days=1)
                return find_date(date)
            else:
                return data
        data = find_date(datetime.utcnow())
        with open(self.raw_data_path+'purple_book_database_extract.csv', 'wb') as csv_file:
            csv_file.write(data)

    def _get_purple_patents(self):
        response = requests.get('https://purplebooksearch.fda.gov/api/v1/patent-list').json()
        data = pd.DataFrame(response)
        data.columns = ['id', 'Reference Product BLA Number', 'Applicant', 'Proprietary Name', 
            'Proper Name', 'Patent Number', 'Text Patent Expiration Date','created_at', 
            'updated_at']
        data = data[['Reference Product BLA Number', 'Applicant', 'Proprietary Name', 
            'Proper Name', 'Patent Number', 'Text Patent Expiration Date']]
        data.to_csv(self.raw_data_path+'purple_patent.csv', index=False)

    def _get_data(self):
        print('   getting biologics data')
        self._get_biologics()
        print('   getting patents data')
        self._get_purple_patents()

    def get_book(self):
        print('processing purple book data')
        self._get_data()
        print('processing biologics data')
        biologics = BiologicalDrugs('purple_book_database_extract.csv', self.raw_data_path, self.format)
        biologics.etl(biologics.name)
        print('processing biologics patent data')
        purple_patents = PurplePatents('purple_patent.csv', self.raw_data_path, self.format)
        purple_patents.etl(purple_patents.name)

class BiologicalDrugs(Transformer):
    def __init__(self, raw_file, raw_data_path = 'raw_data/', format='xlsx'):
        self.name = 'PB'
        self.raw_data = raw_data_path + raw_file
        self.format = format
        super().__init__(self.raw_data, end_data='\\finished data\\', format = self.format,
                            final_columns = ['N/R/U', 'Entity_Applicant',
                                            'Entity_Appr Date_Combined',
                                            'Entity_Orph Excl_Combined',
                                            'Entity_Trade Name_BLA#PR#.1',
                                            'Entity_BLA#PR#',
                                            'BLA Number',
                                            'Entity_Trade Name',
                                            'Proprietary Name',
                                            'Entity_NonProp Name',
                                            'Proper Name',
                                            'BLA Type',
                                            'Strength',
                                            'Dosage Form',
                                            'Route of Administration',
                                            'Product Presentation',
                                            'Status',
                                            'Licensure',
                                            'Entity_Approval Date',
                                            'Ref Product Proper Name',
                                            'Ref Product Proprietary Name',
                                            'Supplement Number',
                                            'Submission Type',
                                            'License Number',
                                            'Product Number',
                                            'Center',
                                            'Entity_Date of First Licensure',
                                            'Entity_Exclusivity Expiration Date',
                                            'First Interchangeable Exclusivity Exp Date',
                                            'Entity_Ref Product Exclusivity Exp Date',
                                            'Entity_Orphan Exclusivity Exp Date',
                                            'Source',
                                            'Text_Approval Date',
                                            'Text_Entity_Date of First Licensure',
                                            'Text_Exclusivity Expiration Date',
                                            'Text_First Interchangeable Exclusivity Exp Date',
                                            'Text_Ref Product Exclusivity Exp Date',
                                            'Text_Orphan Exclusivity Exp Date'])

    def _extract(self):
        data = pd.read_csv(self.source_data)
        start = data.index[data[data.columns[0]] == 'Purple Book Database Extract'].to_list()[0]+1
        columns = data.iloc[start]
        data = data.iloc[start+1:len(data)]
        data.columns = columns
        return data

    def _transform_dates(self):
        self.data['Text_Entity_Date of First Licensure'] = self.date_formula(self.data['Date of First Licensure'])
        self.data['Entity_Date of First Licensure'] = self.data['Date of First Licensure']
        #
        self.data['Text_Orphan Exclusivity Exp Date'] = self.date_formula(self.data['Orphan Exclusivity Exp. Date'])
        self.data['Entity_Orphan Exclusivity Exp Date'] = self.data['Orphan Exclusivity Exp. Date']
        #
        self.data['Text_First Interchangeable Exclusivity Exp Date'] = self.date_formula(self.data['First Interchangeable Exclusivity Exp. Date'])
        self.data['First Interchangeable Exclusivity Exp Date'] = self.data['First Interchangeable Exclusivity Exp. Date']
        #
        self.data['Text_Exclusivity Expiration Date'] = self.date_formula(self.data['Exclusivity Expiration Date'])
        self.data['Entity_Exclusivity Expiration Date'] = self.data['Exclusivity Expiration Date']
        #
        self.data['Entity_Approval Date'] = self.data['Approval Date']
        self.data['Text_Approval Date'] = self.date_formula(self.data['Approval Date'])
        #
        self.data['Entity_Ref Product Exclusivity Exp Date'] = self.data['Ref. Product Exclusivity Exp. Date']
        self.data['Text_Ref Product Exclusivity Exp Date'] = self.date_formula(self.data['Ref. Product Exclusivity Exp. Date'])

    def _transform_product_number(self):
        self.data['Product Number'] = self.data['Product Number'].apply(lambda row: int(row))

    def _transform_entity_nonprop_name(self):
        def entity_nonprop_name(row):
            name = '' if row['Proper Name']!=row['Proper Name'] else row['Proper Name'].upper()
            return f'{name} (NonProp Name)'
        self.data['Entity_NonProp Name'] = self.data.apply(entity_nonprop_name, axis=1)

    def _transform_proper_name(self):
        def proper_name(row):
            name = '' if row['Proper Name']!=row['Proper Name'] else row['Proper Name'].upper()
            return name
        self.data['Proper Name'] = self.data.apply(proper_name, axis=1)

    def _transform_source(self):
        self.data['Source'] = 'FDA Purple Book'

    def _transform_ref_product_proprietary_name(self):
        def ref_product_proprietary_name(row):
            return 'N/A' if row['Ref. Product Proprietary Name']!=row['Ref. Product Proprietary Name'] else row['Ref. Product Proprietary Name'].upper()
        self.data['Ref Product Proprietary Name'] = self.data.apply(ref_product_proprietary_name, axis=1)

    def _transform_entity_trade_name(self):
        def entity_trade_name(row):
            name = '' if row['Proprietary Name'] != row['Proprietary Name'] else row['Proprietary Name'].upper()
            return f'{name} (Trade Name)'
        self.data['Entity_Trade Name'] = self.data.apply(entity_trade_name, axis=1)

    def _transform_propreitary_name(self):
        def propreitary_name(row):
            return '' if row['Proprietary Name'] != row['Proprietary Name'] else row['Proprietary Name'].upper()
        self.data['Propreitary Name'] = self.data.apply(propreitary_name, axis=1)

    def _transform_entity_orph_excl_combined(self):
        def entity_orph_excl_combined(row):
            name = '' if row['Proprietary Name'] != row['Proprietary Name'] else row['Proprietary Name'].upper()
            bla = row['BLA Number']
            pr = int(row['Product Number'])
            date = '' if row['Orphan Exclusivity Exp. Date'] != row['Orphan Exclusivity Exp. Date'] else row['Orphan Exclusivity Exp. Date']
            return f'Orph Excl Date {name} BLA#{bla}PR#{pr}-{date}'
        self.data['Entity_Orph Excl_Combined'] = self.data.apply(entity_orph_excl_combined, axis=1)

    def _transform_entity_applicant(self):
        def entity_applicant(row):
            return '' if row['Applicant']!=row['Applicant'] else row['Applicant'].upper().replace('.','')
        self.data['Entity_Applicant'] = self.data.apply(entity_applicant, axis=1)

    def _transform_entity_bla_pr_(self):
        def entity_bla_pr_(row):
            bla = row['BLA Number']
            pr = int(row['Product Number'])
            return f'BLA#{bla}PR#{pr}'
        self.data['Entity_BLA#PR#'] = self.data.apply(entity_bla_pr_, axis=1)

    def _transform_entity_trade_name_bla_pr__1(self):
        def entity_trade_name_bla_pr__1(row):
            name = '' if row['Proprietary Name'] != row['Proprietary Name'] else row['Proprietary Name'].upper()
            bla = row['BLA Number']
            pr = int(row['Product Number'])
            return f'{name} BLA#{bla}PR#{pr}'
        self.data['Entity_Trade Name_BLA#PR#.1'] = self.data.apply(entity_trade_name_bla_pr__1, axis=1)

    def _transform_ref_product_proper_name(self):
        def ref_product_proper_name(row):
            return 'N/A' if row['Ref. Product Proper Name']!=row['Ref. Product Proper Name'] else row['Ref. Product Proper Name'].upper()
        self.data['Ref Product Proper Name'] = self.data.apply(ref_product_proper_name, axis=1)

    def _transform_entity_appr_date_combined(self):
        def entity_appr_date_combined(row):
            name = '' if row['Proprietary Name'] != row['Proprietary Name'] else row['Proprietary Name'].upper()
            bla = row['BLA Number']
            pr = int(row['Product Number'])
            date = '' if row['Approval Date']!=row['Approval Date'] else ''
            return f'Appr Date {name} BLA#{bla}PR#{pr}-{date}'
        self.data['Entity_Appr Date_Combined'] = self.data.apply(entity_appr_date_combined, axis=1)

class PurplePatents(Transformer):
    def __init__(self, raw_file, raw_data_path = 'raw_data/', format='xlsx'):
            self.name = 'PBPat'
            self.raw_data = raw_data_path + raw_file
            self.format = format
            super().__init__(self.raw_data, end_data='\\finished data\\', format=self.format, 
                                final_columns = ['Entity_BLA#', 'Reference Product BLA Number', 
                                'Entity_Applicant', 'Applicant', 'Entity_Trade Name', 'Proprietary Name', 
                                'Entity_Non Prop Name', 'Column9', 'Proper Name', 'Column11', 
                                'Patent Number', 'Entity_Trade Name_Pat#_ Exp Date', 
                                'Text Patent Expiration Date'])
        
    def _extract(self):
        print(f'extracting {self.source_data}')
        return pd.read_csv(self.source_data)

    def _transform_zdate(self):
        self.data['Text Patent Expiration Date']=self.date_formula(self.data['Text Patent Expiration Date'])

    def _transform_entity_trade_name(self):
        def entity_trade_name(row):
            trade_name = '' if row['Proprietary Name']!=row['Proprietary Name'] else row['Proprietary Name'].upper()
            return f'{trade_name} (Trade Name)'
        self.data['Entity_Trade Name'] = self.data.apply(entity_trade_name, axis=1)
    
    def _transform_entity_trade_name_pat___exp_date(self):
        def entity_trade_name_pat___exp_date(row):
            trade_name = '' if row['Proprietary Name']!=row['Proprietary Name'] else row['Proprietary Name'].upper()
            patent = row['Patent Number'].replace(',','')
            date = '' if row['Text Patent Expiration Date']!= row['Text Patent Expiration Date'] else datetime.strptime(row['Text Patent Expiration Date'],'%Y-%m-%d').strftime('%m/%d/%Y')
            return f'{trade_name} Pat#{patent} Patent Exp Date {date}'
        self.data['Entity_Trade Name_Pat#_ Exp Date'] = self.data.apply(entity_trade_name_pat___exp_date, axis=1)
        
    def _transform_entity_bla_(self):
        def entity_bla_(row):
            bla = row['Reference Product BLA Number']
            return f'BLA#{bla}'
        self.data['Entity_BLA#'] = self.data.apply(entity_bla_, axis=1)
        
    def _transform_entity_non_prop_name_(self):
        def entity_non_prop_name_(row):
            name = '' if row['Proper Name']!= row['Proper Name'] else row['Proper Name'].upper()
            return f'{name} (NonProp Name)'
        self.data['Entity_Non Prop Name'] = self.data.apply(entity_non_prop_name_, axis=1)
        
    def _transform_column9(self):
        def proper_name(row):
            return '' if row['Proper Name']!=row['Proper Name'] else row['Proper Name'].upper()
        self.data['Column9'] = self.data.apply(proper_name, axis=1)
        
    def _transform_column11(self):
        def column11(row):
            name = '' if row['Proprietary Name']!=row['Proprietary Name'] else row['Proprietary Name'].upper()
            pat = row['Patent Number'].replace(',','')
            return f'{name} Pat#{pat}'
        self.data['Column11'] = self.data.apply(column11, axis=1)
        self.data['Patent Number'] = self.data['Patent Number'].apply(lambda row: row.replace(',',''))
        
    def _transform_entity_applicant(self):
        def applicant(row):
            return '' if row['Applicant']!=row['Applicant'] else row['Applicant'].upper().replace('.','')
        self.data['Entity_Applicant'] = self.data.apply(applicant, axis=1)
        self.data['Applicant']=self.data['Applicant'].apply(lambda row: row.replace('.',''))