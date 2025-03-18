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
start_date = '2024-03-01'
end_date = '2024-03-14'

# 生成日期范围
date_range = pd.date_range(start=start_date, end=end_date)

trading_days = pd.Series(date_range).dt.day_name().isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'])

# 过滤出交易日
trading_dates = date_range[trading_days]

# 打印结果
print(trading_dates)

# 存储所有日期的数据
results = []

# print(final_df)# 遍历日期范围
for single_date in trading_dates:
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



# 定义到期日
expiry_dates = {
    'current_month': '2024-03-15',
    'next_month': '2024-04-19',
    'current_quarter': '2024-06-21',
    'next_quarter': '2024-09-20'
}

# 计算年化基差率
for index, row in final_df.iterrows():
    trading_date = row['TradingDate']
    spot_price = row['Close']
    
    # 计算持仓天数
    days_to_expiry = {
        'current_month': (pd.to_datetime(expiry_dates['current_month']) - pd.to_datetime(trading_date)).days,
        'next_month': (pd.to_datetime(expiry_dates['next_month']) - pd.to_datetime(trading_date)).days,
        'current_quarter': (pd.to_datetime(expiry_dates['current_quarter']) - pd.to_datetime(trading_date)).days,
        'next_quarter': (pd.to_datetime(expiry_dates['next_quarter']) - pd.to_datetime(trading_date)).days
    }
    
    # 计算年化基差率
    annual_basis_rate = {
        'current_month': ((row['current_month_close'] - spot_price) / spot_price) * (365 / days_to_expiry['current_month']),
        'next_month': ((row['next_month_close'] - spot_price) / spot_price) * (365 / days_to_expiry['next_month']),
        'current_quarter': ((row['current_quarter_close'] - spot_price) / spot_price) * (365 / days_to_expiry['current_quarter']),
        'next_quarter': ((row['next_quarter_close'] - spot_price) / spot_price) * (365 / days_to_expiry['next_quarter'])
    }
    
    # 将年化基差率添加到DataFrame中
    final_df.loc[index, 'current_month_basis_rate'] = annual_basis_rate['current_month']
    final_df.loc[index, 'next_month_basis_rate'] = annual_basis_rate['next_month']
    final_df.loc[index, 'current_quarter_basis_rate'] = annual_basis_rate['current_quarter']
    final_df.loc[index, 'next_quarter_basis_rate'] = annual_basis_rate['next_quarter']

# 打印结果
print(final_df[['TradingDate', 'current_month_basis_rate', 'next_month_basis_rate', 'current_quarter_basis_rate', 'next_quarter_basis_rate']])

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置字体为黑体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题

# 画图
plt.figure(figsize=(10, 6))

# 确保TradingDate是datetime类型
final_df['TradingDate'] = pd.to_datetime(final_df['TradingDate'])

# 绘制年化基差率曲线
plt.plot(final_df['TradingDate'], final_df['current_month_basis_rate'], label='当月年化基差率', color='blue', linestyle='-')
plt.plot(final_df['TradingDate'], final_df['next_month_basis_rate'], label='次月年化基差率', color='orange', linestyle='-')
plt.plot(final_df['TradingDate'], final_df['current_quarter_basis_rate'], label='当季年化基差率', color='green', linestyle='-')
plt.plot(final_df['TradingDate'], final_df['next_quarter_basis_rate'], label='次季年化基差率', color='red', linestyle='-')

# 添加网格线
plt.grid(True, linestyle='--', alpha=0.7)

# 设置日期格式
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=2))  # 每隔2天显示一个日期

# 设置标题和标签
plt.title('年化基差率随时间变化', fontsize=14)
plt.xlabel('交易日期', fontsize=12)
plt.ylabel('年化基差率 (%)', fontsize=12)

# 设置图例
plt.legend(loc='upper left', fontsize=10)

# 自动调整布局
plt.tight_layout()

# 显示图表
plt.show()