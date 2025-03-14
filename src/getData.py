# -*- encoding: utf-8 -*-
'''
@File    : getData.py
@Date    : 2025-01-03 11:21:37
@Author  : DDB
@Version : 1.0
@Desc    : GoldMine 的个股筛选 与 数据获取
'''


from __future__ import print_function, absolute_import, unicode_literals
from gm.api import *
set_token('b96fda27fa89f65dba4bc34487a5333695fcebb2')

from tqdm import tqdm
import numpy as np
import pandas as pd



class GOLDMINE:
    
    def __init__(self):
        self.symbol_list = None

    def get_data(self):
        data = get_symbol_infos(sec_type1=1010, sec_type2=101001, df=True)
        data = data = data[(data['delisted_date'] >= '2038-01-01')]
        # # 选取创业板
        # data = data[(data['board'] == 10100102) & 
        #             (data['delisted_date'] >= '2038-01-01')]
        self.symbol_list = list(set(data.symbol))
        return data
    
    def get_price(self, 
                  symbol_list:list | str = None,
                  start_date:str = '2023-01-01',
                  end_date:str = '2024-12-31',
                  split:int = 10):
        
        if symbol_list: pass
        else: 
            self.get_data()
            symbol_list = self.symbol_list
        price_list = []
        dateRange = np.array_split(pd.date_range(start_date, end_date).strftime('%Y-%m-%d'), split)
        for Range in tqdm(dateRange):
            piece = history(symbol_list, frequency='1d', start_time=Range[0], end_time=Range[-1], adjust=2, df=True)
            price_list.append(piece)
        price = pd.concat(price_list, axis=0)
        price.eob = price.eob.dt.tz_localize(None)
        price = price.set_index(['eob', 'symbol'])
        return price
    

    def get_one_line(self,
                     data:pd.DataFrame = None,
                     T_N : int = 2):
        '''判断当前data是否是一字涨停'''
        if data: pass
        else:
            data = self.get_price()
        ## 获取一字涨停index
        oneLine_index = data[(data.open == data.low) & (data.low == data.close) & (data.close == data.high) & (data.close > data.pre_close)].index
        ## 获取涨停后的 T+1 & T+2
        groups = data.swaplevel().loc[list(set(oneLine_index.get_level_values(1)))].groupby('symbol')
        info = {}
        for symbol, grouped in groups:

            for date in oneLine_index[oneLine_index.swaplevel().get_loc(symbol)].get_level_values(0):
                pct_change = grouped.swaplevel().loc[date:].iloc[:T_N+1].close.pct_change().iloc[-(T_N):].dropna().tolist()
                while len(pct_change) < T_N:
                    pct_change.append(np.nan)
                info[symbol, date] = np.round(pct_change, 4)
        ## 整合info
        oneLine_df = pd.DataFrame(info).T
        oneLine_df.index.names = ['symbol', 'date']
        col_list = []
        for i in range(1, T_N+1):
            col_list.append(f'T+{i}')

        oneLine_df.columns = col_list
        ## to_csv
        # oneLine_df.to_csv(r'D:\Working\DJCT\Task-01\一字涨停表现.csv')
        return oneLine_df
    

    def get_one_line_plus(self,
                          data:pd.DataFrame = None,
                          T_N:int = 2):
        '''T一字板 & T-1 非涨停 & T+1 非一字板'''
        if data: pass
        else: 
            data = self.get_price()

        plus_list = []
        for symbol, grouped in data.groupby('symbol'):
            for i, date in enumerate(grouped.index.get_level_values(0), start=1):
                
                if i + 1 >= len(grouped): break
                row = grouped.iloc[i]
                pre_row = grouped.iloc[i-1]
                next_row = grouped.iloc[i+1]
                if row.close == row.open == row.high == row.low and row.close > row.pre_close:    # 涨停一字板
                    # T-1 非涨停 & T+1 非一字板
                    if pre_row.close != pre_row.high and not (next_row.close == next_row.open == next_row.high == next_row.low):
                        plus_list.append(row.name)
        
        ## 获取涨停后的 T+1 & T+2
        oneLine_index = pd.MultiIndex.from_arrays(np.array(plus_list).T)
        groups = data.swaplevel().loc[list(set(oneLine_index.get_level_values(1)))].groupby('symbol')
        info = {}
        for symbol, grouped in groups:

            for date in oneLine_index[oneLine_index.swaplevel().get_loc(symbol)].get_level_values(0):
                pct_change = grouped.swaplevel().loc[date:].iloc[:T_N+1].close.pct_change().iloc[-(T_N):].dropna().tolist()
                while len(pct_change) < T_N:
                    pct_change.append(np.nan)
                info[symbol, date] = np.round(pct_change, 4)
        ## 整合info
        oneLine_df = pd.DataFrame.from_dict(info, dtype='object', orient='index')   # 长度不一时无法变为df, 直接用字典的键作为index
        oneLine_df.index = pd.MultiIndex.from_tuples(oneLine_df.index)              # 变换回原来的 multiIndex
        oneLine_df.index.names = ['symbol', 'date']
        col_list = []
        for i in range(1, T_N+1):
            col_list.append(f'T+{i}')
        oneLine_df.columns = col_list

        ## to_csv
        # oneLine_df.to_csv(r'D:\Working\DJCT\Task-01\一字涨停表现_plus.csv')
        return oneLine_df



if __name__ == '__main__':

    goldmine = GOLDMINE()

    data = goldmine.get_one_line_plus(T_N=5)
    print(data)
