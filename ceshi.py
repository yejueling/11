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


start_date = '2024-06-11'
end_date = '2024-09-15'

# 生成日期范围
date_range = pd.date_range(start=start_date, end=end_date)

trading_days = pd.Series(date_range).dt.day_name().isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])

# 过滤出交易日
trading_dates = date_range[trading_days]

# 打印结果
print(trading_dates)


# 修改后的数据收集部分
results = []
for single_date in trading_dates:
    TradingDate = single_date.strftime('%Y-%m-%d')
    df = DDBSUtils.index_daily_data('399300.SZ', begin_date=start_date, end_date=end_date)
    mmf = futures_utils.cffex_futures_list(TradingDate)
    if_futures = [f for f in mmf if 'IF' in f]
    if_futures_sorted = sorted(if_futures, key=lambda x: int(x[2:]))
    
    close_values_list = []
    contract_codes = []  # 保存合约代码
    
    for future in if_futures_sorted:
        uf = futures_utils.cffex_futures_daily_data(future, begin_date=TradingDate, end_date=TradingDate)
        if not uf.empty:
            close_values = uf['Close'].tolist()
            close_values_list.append(close_values[0])
            contract_codes.append(future)
        else:
            close_values_list.append(np.nan)
            contract_codes.append(None)
    
    if len(close_values_list) >= 4:
        current_month_close = close_values_list[0]
        next_month_close = close_values_list[1]
        current_quarter_close = close_values_list[2]
        next_quarter_close = close_values_list[3]
        current_month_code = contract_codes[0]
        next_month_code = contract_codes[1]
        current_quarter_code = contract_codes[2]
        next_quarter_code = contract_codes[3]
    else:
        current_month_close = next_month_close = current_quarter_close = next_quarter_close = np.nan
        current_month_code = next_month_code = current_quarter_code = next_quarter_code = None
    
    results.append([
        TradingDate,
        current_month_close, next_month_close, current_quarter_close, next_quarter_close,
        current_month_code, next_month_code, current_quarter_code, next_quarter_code
    ])

results_df = pd.DataFrame(results, columns=[
    'TradingDate',
    'current_month_close', 'next_month_close', 'current_quarter_close', 'next_quarter_close',
    'current_month_code', 'next_month_code', 'current_quarter_code', 'next_quarter_code'
])

final_df = pd.merge(df[['TradingDate', 'Close']], results_df, on='TradingDate', how='left')


import datetime

def parse_contract_code(code):
    if not code or pd.isnull(code):
        return None, None
    year_part = code[2:4]
    month_part = code[4:6]
    year = 2000 + int(year_part)
    month = int(month_part)
    return year, month

def get_third_friday(year, month):
    day = 1
    count = 0
    while day <= 31:
        try:
            current_date = datetime.date(year, month, day)
            if current_date.weekday() == 4:  # 星期五
                count += 1
                if count == 3:
                    return current_date
            day += 1
        except:
            break
    return None


def calculate_basis(row, contract_type):
    code = row[f'{contract_type}_code']
    close_price = row[f'{contract_type}_close']
    if pd.isnull(code) or pd.isnull(close_price):
        return np.nan
    year, month = parse_contract_code(code)
    if not year or not month:
        return np.nan
    delivery_date = get_third_friday(year, month)
    if not delivery_date:
        return np.nan
    trading_date = pd.to_datetime(row['TradingDate']).date()
    holding_days = (delivery_date - trading_date).days
    if holding_days <= 0:
        return np.nan
    spot_price = row['Close']
    basis = (close_price - spot_price) / spot_price * (365 / holding_days)
    return basis

final_df['current_month_basis'] = final_df.apply(lambda x: calculate_basis(x, 'current_month'), axis=1)
final_df['next_month_basis'] = final_df.apply(lambda x: calculate_basis(x, 'next_month'), axis=1)
final_df['current_quarter_basis'] = final_df.apply(lambda x: calculate_basis(x, 'current_quarter'), axis=1)
final_df['next_quarter_basis'] = final_df.apply(lambda x: calculate_basis(x, 'next_quarter'), axis=1)

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
plt.rcParams['font.sans-serif'] = ['SimHei']  
plt.rcParams['axes.unicode_minus'] = False  
plt.figure(figsize=(12, 6))
plt.plot(final_df['TradingDate'], final_df['current_month_basis'], label='当月基差率')
plt.plot(final_df['TradingDate'], final_df['next_month_basis'], label='次月基差率')
plt.plot(final_df['TradingDate'], final_df['current_quarter_basis'], label='当季基差率')
plt.plot(final_df['TradingDate'], final_df['next_quarter_basis'], label='次季基差率')
plt.title('年化基差率')
plt.xlabel('日期')
plt.ylabel('年化基差率')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()