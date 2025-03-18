from higgsboom.user import *
ddb_config =  {"Cluster": "Research",
              "UserName": "yejueling",
              "Password": "passwd123456"}
set_higgsboom_user_config("ddb_config", ddb_config)
 
from higgsboom.data.market.cnsecurity import *
DDBSUtils = CNSecurityMarketDataUtils()
from higgsboom.data.market.cnfutures import *
futures_utils = CNFuturesMarketDataUtils()


if __name__ == '__main__':   
    df = DDBSUtils.index_daily_data('000905.SH','20241226','20241227')
    print(df)

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 设定开始和结束日期
start_date = '2024-01-01'
end_date = '2024-12-31'

# 生成日期范围
date_range = pd.date_range(start=start_date, end=end_date)

# 存储所有日期的数据
results = []

# 遍历日期范围
for single_date in date_range:
    TradingDate = single_date.strftime('%Y-%m-%d')
    
    # 获取股票数据
    df = DDBSUtils.index_daily_data('399300.SZ', begin_date=TradingDate, end_date=TradingDate)
    
    # 获取期货合约列表
    mmf = futures_utils.cffex_futures_list(TradingDate)
    
    # 过滤出与IF相关的期货合约
    if_futures = [f for f in mmf if 'IF' in f]
    
    # 按末尾四个数字从小到大排列
    if_futures_sorted = sorted(if_futures, key=lambda x: int(x[2:]))
    
    # 存储 Close 值的列表
    close_values_list = []
    
    # 遍历 if_futures 列表，调用 cffex_futures_daily_data 函数
    for future in if_futures_sorted:
        # 调用函数，传入当前合约和日期
        uf = futures_utils.cffex_futures_daily_data(future, begin_date=TradingDate, end_date=TradingDate)
        
        if not uf.empty:
            close_values = uf['Close'].tolist()  # 将 Close 列转换为列表
            close_values_list.append(close_values[0])  # 假设每个 close_values 列表只包含一个值
        else:
            close_values_list.append(np.nan)  # 如果没有数据，则添加 NaN
    
    # 将 Close 值分别赋给当月、次月、当季和次季
    if len(close_values_list) >= 4:
        current_month_close = close_values_list[0]  # IF2403
        next_month_close = close_values_list[1]     # IF2404
        current_quarter_close = close_values_list[2]  # IF2406
        next_quarter_close = close_values_list[3]     # IF2409
    else:
        current_month_close = next_month_close = current_quarter_close = next_quarter_close = np.nan

    # 将 Close 值整合成数组
    close_values_array = [TradingDate, current_month_close, next_month_close, current_quarter_close, next_quarter_close]
    results.append(close_values_array)

# 创建 DataFrame
results_df = pd.DataFrame(results, columns=['TradingDate', 'current_month_close', 'next_month_close', 'current_quarter_close', 'next_quarter_close'])

# 合并股票数据
final_df = pd.merge(df[['TradingDate', 'Close']], results_df, on='TradingDate', how='left')

print(final_df)

    #改进以下代码，计算年化基差率# 定义起始日期和结束日期
start_date = '2024-01-03'
end_date = '2025-01-17'

# 假设中国的交易日为周一到周五，排除法定节假日
# 定义法定节假日列表（假设的示例，实际假日需根据当年情况调整）
holidays = [
    '2024-01-01',  # 元旦
    '2024-02-10',  # 元宵节
    '2024-02-09',
    '2024-02-11',  # 春节假期调休
    '2024-02-12',
    '2024-02-13',
    '2024-02-14',
    '2024-02-15',
    '2024-02-16',
    '2024-04-04',  # 清明节
    '2024-04-05',
    '2024-05-01',
    '2024-05-02',
    '2024-05-03',
    '2024-06-10',
    '2024-09-16',
    '2024-09-17',
    '2024-10-01',
    '2024-10-02',
    '2024-10-03',
    '2024-10-04',
    '2024-10-05',
    '2024-10-06',
    '2024-10-07',
    '2025-01-01',
    # 可以根据需要添加更多节假日
]

# 将假日转换为日期类型
holidays = pd.to_datetime(holidays)

# 定义自定义的交易日
trading_days = CustomBusinessDay(weekmask='1111100', holidays=holidays)

# 生成交易日日期范围
trading_date_range = pd.date_range(start=start_date, end=end_date, freq=trading_days)

# 输出交易日日期范围
print(trading_date_range)

# 存储所有日期的数据
results = []

# 遍历日期范围
for single_date in trading_date_range:
    TradingDate = single_date.strftime('%Y-%m-%d')
    
    # 获取股票数据
    df = DDBSUtils.index_daily_data('399300.SZ', begin_date=start_date, end_date=end_date)
    
    # 获取期货合约列表
    mmf = futures_utils.cffex_futures_list(TradingDate)
    
    # 过滤出与IF相关的期货合约
    if_futures = [f for f in mmf if 'IF' in f]
    
    # 按末尾四个数字从小到大排列
    if_futures_sorted = sorted(if_futures, key=lambda x: int(x[2:]))
    
    # 存储 Close 值的列表
    close_values_list = []
    
    # 遍历 if_futures 列表，调用 cffex_futures_daily_data 函数
    for future in if_futures_sorted:
        # 调用函数，传入当前合约和日期
        uf = futures_utils.cffex_futures_daily_data(future, begin_date=TradingDate, end_date=TradingDate)
        
        if not uf.empty:
            close_values = uf['Close'].tolist()  # 将 Close 列转换为列表
            close_values_list.append(close_values[0])  # 假设每个 close_values 列表只包含一个值
        else:
            close_values_list.append(np.nan)  # 如果没有数据，则添加 NaN
    
    # 将 Close 值分别赋给当月、次月、当季和次季
    if len(close_values_list) >= 4:
        current_month_close = close_values_list[0]  # IF2403
        next_month_close = close_values_list[1]     # IF2404
        current_quarter_close = close_values_list[2]  # IF2406
        next_quarter_close = close_values_list[3]     # IF2409
    else:
        current_month_close = next_month_close = current_quarter_close = next_quarter_close = np.nan

    # 将 Close 值整合成数组
    close_values_array = [TradingDate, current_month_close, next_month_close, current_quarter_close, next_quarter_close]
    results.append(close_values_array)

# 创建 DataFrame
results_df = pd.DataFrame(results, columns=['TradingDate', 'current_month_close', 'next_month_close', 'current_quarter_close', 'next_quarter_close'])

# 合并股票数据
final_df = pd.merge(df[['TradingDate', 'Close']], results_df, on='TradingDate', how='left')

# 计算基差
final_df['当月基差'] = (final_df['current_month_close'] - final_df['Close']) / final_df['Close']
final_df['次月基差'] = (final_df['next_month_close'] - final_df['Close']) / final_df['Close']
final_df['当季基差'] = (final_df['current_quarter_close'] - final_df['Close']) / final_df['Close']
final_df['次季基差'] = (final_df['next_quarter_close'] - final_df['Close']) / final_df['Close']
print(final_df)
