from fda_data_getter.orange_book import OrangeBook
from fda_data_getter.purple_book import PurpleBook
from fda_data_getter.ndc import NDCBook
from time import time
import sys
import os

def main(format='xlsx'):
    print('\033[94mGetting fda and NDC data\033[0m')
    start = time()
    orange_book = OrangeBook(format=format)
    purple_book = PurpleBook(format=format)
    ndc_book = NDCBook(format=format)
    ops = [purple_book, ndc_book, orange_book]
    for op in ops:
        op.get_book()
    end = time()
    elapsed = round((end-start)/60,1)
    print(f'\033[92mDone! Operation took {elapsed} minutes\033[0m')

if __name__ == '__main__': 
    for dirs in ['raw_data', 'finished data']:
        if not os.path.exists(dirs):
            os.mkdirs(dirs)
    if len(sys.argv) > 1: 
        run_format = sys.argv[1]
        if run_format not in ('xlsx', 'csv'):
            print(f'arguments must be in \'xlsx\', \'csv\', \'\'\n')
            print('try \'python main.py csv\'\n')
            print('or \'python main.py xlsx \'\n')
            print('or just \'python main.py\'\n')
        else:
            print(f'output files will be in {run_format} format')
            main(run_format)
    else:
        print(f'output files will be in xlsx format')
        main()
