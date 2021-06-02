import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from typing import List, Dict
from sqlalchemy.types import Integer, Text, String, DateTime, Float, Boolean
from urllib.parse import quote_plus as urlquote

password = urlquote("Victoria1008@")
db_uri = 'mysql+pymysql://root:{}@localhost:3306/CATest'.format(password)
engine = create_engine(db_uri, echo=True)


def h_read_and_sort(hfilename):

    """Read and sort Holdings data by securityID and EffectiveDate"""

    df = pd.read_csv(hfilename, delimiter=',')
    
    df_h_sort = df.sort_values(by=['SecurityID','EffectiveDate'], ascending=True)
    return df_h_sort


def t_read_and_sort(tfilename):

    """Read and sort Transaction data by dealdate"""
    
    df = pd.read_csv(tfilename, header=0, delimiter=',')
    df.columns = ['Description','SecurityID','ISIN','Cusip','Asset Sub-Class', 'Asset Sub-type', 'Cost Currency', 'DealDate','Settlement/Start Date','Nominal','Price','NetConsideration','Accrued','Buy Currency']
    df_t_sort = df.sort_values(by=['DealDate'], ascending=True)
    return df_t_sort


def remove_comma(df_h_sort, headername):

    """Remove Comma in any numeric file"""

    df_h_sort[headername] = df_h_sort[headername].apply(lambda x: x.replace(',', '')).astype('float64').apply(lambda x: "%.4f" % x).astype('float64')
    return df_h_sort


def group_by_assetclass(df_h_sort):

    df_a_pivot = pd.pivot_table(df_h_sort, index = ['AssetClass'], columns=['EffectiveDate'],values=['TotalMarketValue','Face'],aggfunc=[np.sum],fill_value=0)
    df_a_pivot.columns = ['_'.join((j,k,i)) for i,j,k in df_a_pivot.columns]
    df_a_pivot['Face Movement'] = df_a_pivot['Face_28-Jan-21_sum'] - df_a_pivot['Face_27-Jan-21_sum']
    df_a_pivot['Total MarketValue Movement'] = df_a_pivot['TotalMarketValue_28-Jan-21_sum'] - df_a_pivot['TotalMarketValue_27-Jan-21_sum'] 
    return df_a_pivot

def calculate_return(df_h_sort):

    df_pivot = pd.pivot_table(df_h_sort,index=['SecurityID'],columns=['EffectiveDate'],values=['TotalMarketValue','Face'] ,aggfunc=[np.sum],fill_value=0)
    
    df_pivot.style.format({'return': "{:.2%}"})
    df_pivot.columns = ['_'.join((j,k,i)) for i,j,k in df_pivot.columns]
    df_pivot['ID'] = df_pivot.index
    df_pivot['return'] = ((df_pivot['TotalMarketValue_28-Jan-21_sum'] / df_pivot['Face_28-Jan-21_sum'])   
    - (df_pivot['TotalMarketValue_27-Jan-21_sum']/df_pivot['Face_27-Jan-21_sum'])) /(
        df_pivot['TotalMarketValue_27-Jan-21_sum']/df_pivot['Face_27-Jan-21_sum']).apply(lambda x: "%.4f" % x).astype('float64')
    
    
    df_pivot['movement'] = (df_pivot['Face_28-Jan-21_sum'] - df_pivot['Face_27-Jan-21_sum']).astype('float64').apply(lambda x: "%.4f" % x).astype('float64')
    
    return df_pivot

def calculate_movement(df_pivot,df_t_sort):

    transactions_fromholdings = df_pivot[df_pivot['movement'] != 0]

    compare_result = pd.merge(transactions_fromholdings, df_t_sort, how='outer',on='SecurityID')
    notintransaction = compare_result[(compare_result['Nominal'] - compare_result['movement']) !=0 ] 

    return notintransaction

def import_to_database(table_name, schema, jobs_df):
    jobs_df.to_sql(
        table_name,
        engine,
        if_exists ='replace',
        index=False,
        chunksize=500,
        dtype=schema
    )

def calculate_sales_price(df_t_sort,df_h_sort):
   
    df_t = pd.merge(df_t_sort,df_h_sort,left_on=['SecurityID','DealDate'],right_on=['SecurityID','EffectiveDate'],how = 'left')
    df_t['Dif'] =df_t['Price_x'] - df_t['Price_y']

    return df_t


HOLDINGS_SCHEMA = {
    'EffectiveDate':        String(50),
    'SecurityID':           String(50),
    'Description':          String(200),
    'AssetClass':           String(50),
    'SubClass':             String(50),
    'Currency':             String(50),
    'Face':                 Float,
    'Price':                Float,
    'TotalMarketValue':     Float,
    'Accrued':              Float,
    'mvpreacc':             Float,
    'tmvrc':                Float,
    'dq1':                  Boolean
}

TRANSACTION_SCHEMA = {
    'Description':               String(200),
    'SecurityID':                String(50),
    'ISIN':                      String(50),
    'Cusip':                     String(50),
    'Asset Sub-Class':           String(50),
    'Asset Sub-type':            String(50),
    'Cost Currency':             String(50),
    'DealDate':                  String(50),
    'Settlement/Start Date':     String(50),
    'Nominal':                   Float,
    'Price':                     Float,
    'NetConsideration':          Float,
    'Accrued':                   Float,
    'Buy Currency':              String(50)
}

RETURN_CALCULATION_SCHEMA = {
    'Face_27-Jan-21_sum':                  Float,
    'Face_28-Jan-21_sum':                  Float,
    'TotalMarketValue_27-Jan-21_sum':      Float,
    'TotalMarketValue_28-Jan-21_sum':      Float,
    'ID':                                  String(50),
    'return':                              Float,
    'movement':                            Float
    }

EXCEPTIONS_SCHEMA = {
    'SecurityID':                           String(50),
    'Face_27-Jan-21_sum':                   Float,
    'Face_28-Jan-21_sum':                   Float,
    'TotalMarketValue_27-Jan-21_sum':       Float,
    'TotalMarketValue_28-Jan-21_sum':       Float,
    'ID':                                   String(50),
    'return':                               Float,
    'movement':                             Float,
    'Description':                          String(200),
    'ISIN':                                 String(50),
    'Cusip':                                String(50),
    'Asset Sub-Class':                      String(50),
    'Asset Sub-type':                       String(50),
    'Cost Currency':                        String(50),
    'DealDate':                             String(50),
    'Settlement/Start Date':                String(50),
    'Nominal':                              Float,
    'Price':                                Float,
    'NetConsideration':                     Float,
    'Accrued':                              Float,
    'Buy Currency':                         String(50)
    }

DIFFERENCES_RECALC_SCHEMA = {
    'Description_x':             String(200),
    'SecurityID':                String(50),
    'ISIN':                      String(50),
    'Cusip':                     String(50),
    'Asset Sub-Class':           String(50),
    'Asset Sub-type':            String(50),
    'Cost Currency':             String(50),
    'DealDate':                  String(50),
    'Settlement/Start Date':     String(50),
    'Nominal':                   Float,
    'Price_x':                   Float,
    'NetConsideration':          Float,
    'Accrued_x':                 Float,
    'Buy Currency':              String(50),
    'EffectiveDate':             String(50),
    'Description_y':             String(200),
    'AssetClass':                String(50),
    'SubClass':                  String(50),
    'Currency':                  String(50),
    'Face':                      Float,
    'Price_y':                   Float,
    'TotalMarketValue':          Float,
    'Accrued_y':                 Float,
    'Dif':                       Float,
    }












if __name__ == '__main__':    

    """Display any float64 into 4 decimal places"""

    pd.options.display.float_format = '{:.4f}'.format

    """Read Holdings and Transactions data from  csv"""

    df_h_sort = h_read_and_sort('Holdings.csv')
    df_t_sort = t_read_and_sort('Transactions.csv')
 
    
    """Remove comma for Face, TotalMarketValue and Accrued fields in Holdings"""
    h_headers = ['Face','TotalMarketValue','Accrued']
    for header in h_headers:
        df_h_sort = remove_comma(df_h_sort, header)

    """Remove comma for Nominal, NetConsideration, Accrued in Transaction"""

    t_headers = ['Nominal','NetConsideration','Accrued']
    for header in t_headers:
        df_t_sort = remove_comma(df_t_sort, header)
    
    
    """Create a pivot table to calculate returns and transaction movement"""

    df_pivot = calculate_return(df_h_sort)
   
    df_a_pivot = group_by_assetclass(df_h_sort)


    df_t = calculate_sales_price(df_t_sort,df_h_sort)
    """Create transactions_fromholdings table to extract all Face value movement during 2 days"""
    
    notintransaction = calculate_movement(df_pivot, df_t_sort)
    
    """Calculate Market value of the instruments of the day"""
    df_h_sort['mvpreacc'] = (df_h_sort['Face'] * df_h_sort['Price'] / 100)
    
    """Re-Calculate Total Market Value for Data Quality check"""
    df_h_sort['tmvrc'] = df_h_sort['mvpreacc'] + df_h_sort['Accrued']

    """Data Quatlity check 1, check Total market value is equal to calculated market value"""
    df_h_sort['dq1'] = (df_h_sort['tmvrc'] - df_h_sort['TotalMarketValue'] < 1 ) 
    
    import_to_database("holdings", HOLDINGS_SCHEMA, df_h_sort)
    import_to_database("transactions", TRANSACTION_SCHEMA, df_t_sort)
    import_to_database('return', RETURN_CALCULATION_SCHEMA, df_pivot)
    import_to_database('exceptions',EXCEPTIONS_SCHEMA, notintransaction)
    import_to_database('salesrecalc',DIFFERENCES_RECALC_SCHEMA,df_t)
    
    transactions_fromholdings = df_pivot[df_pivot['movement'] != 0]

    print('==========================================================================================================================')
    print('Print Holdings information with data quality check and recalculation')
    print(df_h_sort)
   
    print('==========================================================================================================================')
    print('Print Transaction information with sorting by DealDate')
    print(df_t_sort)
    
    print('==========================================================================================================================')
    print('Print Holdings information with return calculated')
    print(df_pivot)
    
    print('==========================================================================================================================')
    print('Print transactions identified from Holdings')
    print(transactions_fromholdings)
    
    print('==========================================================================================================================')
    print('Compare Transactions identified from Holdings with Transactions table provided to identify differences')
    print(notintransaction)
    
    print('==========================================================================================================================')
    print('Compare Face/MarketValue Movement by Asset Class by Date')
    print(df_a_pivot)
    
    print('==========================================================================================================================')
    print('Calculate the actual transaction price with market price of that date')
    print(df_t)
 
    
   
    
