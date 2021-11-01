from fda_data_getter.orange_book import OrangeBook
from fda_data_getter.purple_book import PurpleBook
from fda_data_getter.ndc import NDCBook
from time import time

def main():
    orange_book = OrangeBook()
    purple_book = PurpleBook()
    ndc_book = NDCBook()
    ops = [purple_book, ndc_book, orange_book]
    for op in ops:
        op.get_book()

if __name__ == '__main__': 
    print('\033[94mGetting fda and NDC data\033[0m')
    start = time()
    main()
    end = time()
    elapsed = round((end-start)/60,1)
    print(f'\033[92mDone! Operation took {elapsed} minutes\033[0m')