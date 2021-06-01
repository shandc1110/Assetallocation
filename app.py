import pandas as pd
import numpy as np
from typing import List, Dict

def h_read_and_sort(hfilename):

    """Read and sort Holdings data by securityID and EffectiveDate"""

    df = pd.read_csv(hfilename, delimiter=',')
    df_h_sort = df.sort_values(by=['SecurityID','EffectiveDate'], ascending=True)
    return df_h_sort


def t_read_and_sort(tfilename):

    """Read and sort Transaction data by dealdate"""
    
    df = pd.read_csv(tfilename,delimiter=',')
    df_t_sort = df.sort_values(by=['DealDate'], ascending=True)
    return df_t_sort


def remove_comma(df_h_sort, headername):

    """Remove Comma in any numeric file"""

    df_h_sort[headername] = df_h_sort[headername].apply(lambda x: x.replace(',', '')).astype('float64').apply(lambda x: "%.4f" % x).astype('float64')
    return df_h_sort






if __name__ == '__main__':    

    """Display any float64 into 4 decimal places"""

    pd.options.display.float_format = '{:.4f}'.format

    """Read Holdings and Transactions data from  csv"""

    df_h_sort = h_read_and_sort('Holdings.csv')
    df_t_sort = t_read_and_sort('Transactions.csv')
 
    
    """Remove comma for Face, TotalMarketValue and Accrued fields"""
    headers = ['Face','TotalMarketValue','Accrued']
    for header in headers:
        df_h_sort = remove_comma(df_h_sort, header)
    
    
    df_pivot = pd.pivot_table(df_h_sort,index=['SecurityID'],columns=['EffectiveDate'],values=['TotalMarketValue','Face'] ,aggfunc=[np.sum],fill_value=0)
    df_pivot.style.format({'return': "{:.2%}"})
    df_pivot.columns = ['_'.join((j,k,i)) for i,j,k in df_pivot.columns]
    
    df_pivot['return'] = ((df_pivot['TotalMarketValue_28-Jan-21_sum'] - df_pivot['TotalMarketValue_27-Jan-21_sum']) / df_pivot['TotalMarketValue_27-Jan-21_sum'] * 100).apply(lambda x: "%.4f" % x).astype('float64')
    df_pivot['movement'] = (df_pivot['Face_28-Jan-21_sum'] - df_pivot['Face_27-Jan-21_sum']).astype('float64').apply(lambda x: "%.4f" % x).astype('float64')
    
    Transactions = df_pivot[df_pivot['movement'] != 0]
    
    print('=========================')
    print(df_t_sort)
    print(df_pivot)
    print(df_pivot.dtypes)
   
    print(Transactions)
    
   
    
    """Calculate Market value of the instruments of the day"""
    df_h_sort['mvpreacc'] = (df_h_sort['Face'] * df_h_sort['Price'] / 100)
    
    """Re-Calculate Total Market Value for Data Quality check"""
    df_h_sort['tmvrc'] = df_h_sort['mvpreacc'] + df_h_sort['Accrued']

    """Data Quatlity check 1, check Total market value is equal to calculated market value"""
    df_h_sort['dq1'] = (df_h_sort['tmvrc'] == df_h_sort['TotalMarketValue'] )
    

    print(df_h_sort)
   
   
