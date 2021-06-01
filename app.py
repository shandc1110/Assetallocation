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

    df_h_sort[headername] = df_h_sort[headername].apply(lambda x: x.replace(',', '')).astype('float32').apply(lambda x: "%.4f" % x)
    return df_h_sort



if __name__ == '__main__':    

    df_h_sort = h_read_and_sort('Holdings.csv')
    df_t_sort = t_read_and_sort('Transactions.csv')
    
    headers = ['Face','TotalMarketValue','Accrued']
    for header in headers:
        df_h_sort = remove_comma(df_h_sort, header)
    
    
    #df_h_sort[mvpreacc] = df_h_sort['Face'] * df_h_sort['Price'] / 100

    print('=========================')
    print(df_t_sort)
    print(df_h_sort.dtypes)
    
    #df_pivot = pd.pivot_table(df,index=['SecurityID'],columns=['EffectiveDate'],values=['TotalMarketValue'],aggfunc=[np.sum],fill_value=0)
    #df_pivot.columns = ['_'.join((j,k,i)) for i,j,k in df_pivot.columns]
  
    df_h_sort['mvpreacc'] = df_h_sort['Face'].apply(lambda x: x.replace(',', '')).astype('float32') * df_h_sort['Price']
    df_h_sort['tmvrc'] = df_h_sort['mvpreacc'] + df_h_sort['Accrued'].apply(lambda x: x.replace(',', '')).astype('float32')
    print(df_h_sort)
    print(df_h_sort.dtypes)
    # print(df_sort['TotalMarketValuepreAccrued'].apply(lambda x: "%.4f" % x))
    #df_sort['TotalMarektvaluerecalc'] = df_sort['TotalMarketValuepreAccrued'] + df_sort['Accrued']



    #df_pivot['return'] = df_pivot['TotalMarketValue_27-Jan-21_sum'] / df_pivot['TotalMarketValue_28-Jan-21_sum']
    #print(df_sort)
    #pricedata = df_pivot['TotalMarketValue_27-Jan-21_sum']

    #print(pricedata)
