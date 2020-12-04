import os
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import re

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(PROJECT_DIR)

from models import BranchProduct, Product

ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
PRODUCTS_PATH = os.path.join(ASSETS_DIR, "PRODUCTS.csv")
PRICES_STOCK_PATH = os.path.join(ASSETS_DIR, "PRICES-STOCK.csv")
DB = os.path.join(PROJECT_DIR, "db.sqlite")
engine = create_engine('sqlite:///' + DB)

def process_csv_files():
    products_df = pd.read_csv(filepath_or_buffer=PRODUCTS_PATH, sep="|",)
    prices_stock_df = pd.read_csv(filepath_or_buffer=PRICES_STOCK_PATH, sep="|",)
    filter(products_df,prices_stock_df)

def filter(products_df,prices_stock_df):
    prices_stock_df = prices_stock_df[(prices_stock_df['BRANCH'] == 'RHSM') 
                        |(prices_stock_df['BRANCH'] == 'MM')]
    prices_stock_df = prices_stock_df[prices_stock_df['STOCK'] > 0]

    products_df['DESCRIPTION'] = products_df['DESCRIPTION'].map(lambda x: str(x)
                                    .lstrip('<p>').rstrip('</p>').lstrip())
    products_df["CATEGORY"] = (products_df["CATEGORY"] + "|" + products_df["SUB_CATEGORY"]+ "|" + products_df["SUB_SUB_CATEGORY"]).str.lower()
    
    condition = products_df['SKU'].isin(prices_stock_df['SKU'].values)
    products_df = products_df[condition]

    df = products_df['DESCRIPTION'].str.replace('.', '')
    products_df['PACKAGE'] = df.apply(lambda x: re.findall(r'((?!0)\d+\s*\D+$)', x))
    products_df['PACKAGE'] = products_df['PACKAGE'].apply(''.join).apply(lambda x: x.replace('.', ''))
    
    add_product_db(products_df,prices_stock_df)
    

def add_product_db(products_df,prices_stock_df):
    products_df = products_df.drop(
        columns=['KIRLAND_ITEM', 'BUY_UNIT', 'FINELINE_NUMBER',
                    'SUB_CATEGORY', 'SUB_SUB_CATEGORY', 'ORGANIC_ITEM', 
                    'DESCRIPTION_STATUS'])
    prices_stock_df = prices_stock_df.rename(columns={'SKU':'PRODUCT_ID'})

    products_df['STORE'] = 'Richarts'
    products_df['URL'] = 'PRODUCTS.csv'
    products_df.fillna(value='', inplace=True)

    products_df.to_sql('products', con = engine, 
                        if_exists='append',index = False)

    prices_stock_df.to_sql('branchproducts', con = engine, 
                            if_exists='append',index = False)


if __name__ == "__main__":
    process_csv_files()



