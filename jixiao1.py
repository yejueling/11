import argparse
from higgsboom.user import *
ddb_config =  {"Cluster": "Research",
                "UserName": "yejueling",
                "Password": "passwd123456"}
set_higgsboom_user_config("ddb_config", ddb_config)

from higgsboom.data.market.cnsecurity import *
DDBSUtils = CNSecurityMarketDataUtils()
from higgsboom.data.market.cnfutures import *
futures_utils = CNFuturesMarketDataUtils()

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from higgsboom.funcutil.datetime import *
from higgsboom.data.tradinglog.tradinglog import *

# 使用 argparse 解析命令行参数
parser = argparse.ArgumentParser(description='TWAP绩效计算')
parser.add_argument('-d', '--date', type=str, required=True, help='输入日期，格式为 YYYYMMDD')
args = parser.parse_args()

# 根据输入的日期构建 CSV 文件路径
input_date = args.date
csv_file_path = f"C:/Users/16532/Desktop/{input_date}_sh_trade.csv"

# 读取 CSV 文件并指定列名
df = pd.read_csv(csv_file_path)

# 输出数据
print(df)

# 修复时间解析警告
df['TradeTime'] = pd.to_datetime(
    df['TradeTime'], 
    format='%H:%M:%S.%f',  # 格式匹配毫秒
    errors='coerce'
)

# # 清理无效时间数据（可选）
# df = df.dropna(subset=['TradeTime'])

# 修复 datetime.strptime 错误
# start_time = datetime.strptime("09:30:00", "%H:%M:%S").time()
# end_time = datetime.strptime("14:56:40", "%H:%M:%S").time()

# df['TradeTime'] = pd.to_datetime(df['TradeTime'], errors='coerce')

# # 检查是否所有时间都被成功解析
# if df['TradeTime'].isna().any():
#     print("Warning: Some TradeTime values could not be converted to datetime.")

start_time = datetime.datetime.strptime("09:30:00", "%H:%M:%S").time()
end_time = datetime.datetime.strptime("10:30:00", "%H:%M:%S").time()

# 筛选条件：交易时间的time部分 >= start_time 且 <= end_time
filtered_df = df[(df['TradeTime'].dt.time >= start_time) & (df['TradeTime'].dt.time <= end_time)]
policy = 'alpha_clear'
if policy == 'alpha_clear':
    filtered_df = filtered_df[filtered_df['ClientStrategyId'].str.contains('alpha_clear', case=False, na=False)]
elif policy == 'alpha_build':
    filtered_df = filtered_df[filtered_df['ClientStrategyId'].str.contains('alpha_build', case=False, na=False)]

# 输出结果
print(filtered_df)

def weighted_average(group):
    # 计算买入和卖出的加权平均价格
    buy_mask = group['Direction'] == 'BUY'
    sell_mask = group['Direction'] == 'SELL'
    
    # 买入
    buy_volume = None
    avg_buy_price = None
    if buy_mask.any():
        buy_volume = group.loc[buy_mask, 'Volume'].sum()
        buy_price_volume = (group.loc[buy_mask, 'Price'] * group.loc[buy_mask, 'Volume']).sum()
        avg_buy_price = buy_price_volume / buy_volume if buy_volume != 0 else None
    else:
        buy_volume = 0  # 如果没有买入记录，设置总量为0
    
    # 卖出
    sell_volume = None
    avg_sell_price = None
    if sell_mask.any():
        sell_volume = group.loc[sell_mask, 'Volume'].sum()
        sell_price_volume = (group.loc[sell_mask, 'Price'] * group.loc[sell_mask, 'Volume']).sum()
        avg_sell_price = sell_price_volume / sell_volume if sell_volume != 0 else None
    else:
        sell_volume = 0  # 如果没有卖出记录，设置总量为0
    
    return pd.Series({
        'Average Buy Price': avg_buy_price,
        'Average Sell Price': avg_sell_price,
        'Total Buy Volume': buy_volume,
        'Total Sell Volume': sell_volume
    })

# 按 InstrumentId 分组并应用函数
result = filtered_df.groupby('InstrumentId').apply(weighted_average).reset_index()
# 将所有 NaN 替换为 0
result.fillna(0, inplace=True)

# 输出结果
print(result)

# 遍历每个 InstrumentId，计算对应的 TWAP
twap_results = []

for instrument_id in result['InstrumentId']:
    # 获取当前 InstrumentId 的数据
    mf_instrument = DDBSUtils.stock_aligned_taq_data(instrument_id, input_date)  # 使用输入的日期
    mf_instrument['UpdateTime'] = pd.to_datetime(mf_instrument['UpdateTime'], format='%H:%M:%S') 
    
    # 过滤数据
    filtered_mf_instrument = mf_instrument[(mf_instrument['UpdateTime'].dt.time >= start_time) & (mf_instrument['UpdateTime'].dt.time <= end_time)]
    
    # 计算TWAP
    if not filtered_mf_instrument.empty:
        filtered_mf_instrument = filtered_mf_instrument.copy()
        filtered_mf_instrument.loc[:, 'TWAP'] = (filtered_mf_instrument['BuyPrice01'] + filtered_mf_instrument['SellPrice01']) / 2
        twap_instrument = filtered_mf_instrument['TWAP'].mean()
        twap_results.append({'InstrumentId': instrument_id, 'TWAP': twap_instrument})
    else:
        twap_results.append({'InstrumentId': instrument_id, 'TWAP': None})

# 将结果转换为 DataFrame
twap_df = pd.DataFrame(twap_results)
print(twap_df)

# 合并结果
final_result = pd.merge(result, twap_df, on='InstrumentId', how='left')

# 计算报单收益
final_result['Order Buy Profit'] = (
    (final_result['TWAP'] - final_result['Average Buy Price']) * final_result['Total Buy Volume'] 
)
final_result['Order Sell Profit'] = ( 
    (final_result['Average Sell Price'] - final_result['TWAP']) * final_result['Total Sell Volume']
)

# 确保列的存在和数值类型
final_result['Order Buy Profit'] = pd.to_numeric(final_result.get('Order Buy Profit', 0), errors='coerce')
final_result['Order Sell Profit'] = pd.to_numeric(final_result.get('Order Sell Profit', 0), errors='coerce')

# 填充可能的 NaN 值为 0
final_result['Order Buy Profit'] = final_result['Order Buy Profit'].fillna(0)
final_result['Order Sell Profit'] = final_result['Order Sell Profit'].fillna(0)

# 计算 Order Profit
final_result['Order Profit'] = final_result['Order Buy Profit'] + final_result['Order Sell Profit']

# 输出最终结果
print(final_result)
Order_Profit = (final_result['Order Profit']).sum()
money = (final_result['Average Buy Price'] * final_result['Total Buy Volume'] + final_result['Average Sell Price'] * final_result['Total Sell Volume']).sum()
Order_Profit_ratio = Order_Profit / money
print(f"报单收益: {Order_Profit:.7f}")
print(f"报单收益率: {Order_Profit_ratio:.7f}")

# # 保存结果
# final_result.to_csv('final_result.csv', index=False)