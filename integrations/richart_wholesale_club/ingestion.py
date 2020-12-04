import os
import sys
import numpy as np
import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(PROJECT_DIR)

from models import BranchProduct, Product


ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
PRODUCTS_PATH = os.path.join(ASSETS_DIR, "PRODUCTS.csv")
PRICES_STOCK_PATH = os.path.join(ASSETS_DIR, "PRICES-STOCK.csv")


def process_csv_files():

    products_df = pd.read_csv(filepath_or_buffer=PRODUCTS_PATH, sep="|",)
    prices_stock_df = pd.read_csv(filepath_or_buffer=PRICES_STOCK_PATH, sep="|",)

    filter(products_df,prices_stock_df)


def filter(products_df,prices_stock_df):

    prices_stock_df = prices_stock_df[(prices_stock_df['BRANCH'] == 'RHSM') 
                        | (prices_stock_df['BRANCH'] == 'MM')]
    prices_stock_df = prices_stock_df[prices_stock_df['STOCK'] > 0]

    products_df['DESCRIPTION'] = products_df['DESCRIPTION'].map(lambda x: str(x).lstrip('<p>')
                        .rstrip('</p>').lstrip())

    products_df["CATEGORY"] = products_df["CATEGORY"] + "|" + products_df["SUB_CATEGORY"]+ "|" + products_df["SUB_SUB_CATEGORY"]




    print(products_df["CATEGORY"])


if __name__ == "__main__":
    process_csv_files()



