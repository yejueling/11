import argparse
from higgsboom.user import *
import datetime as dt
from datetime import datetime, time, timedelta
import pandas as pd

# 配置用户信息
ddb_config = {"Cluster": "Research", "UserName": "yejueling", "Password": "passwd123456"}
set_higgsboom_user_config("ddb_config", ddb_config)

from higgsboom.data.market.cnsecurity import *
DDBSUtils = CNSecurityMarketDataUtils()
from higgsboom.data.market.cnfutures import *
futures_utils = CNFuturesMarketDataUtils()

from higgsboom.funcutil.datetime import *
from higgsboom.data.tradinglog.tradinglog import *

# 使用 argparse 解析命令行参数
parser = argparse.ArgumentParser(description='TWAP绩效计算')
parser.add_argument('-d', '--date', type=str, required=True, help='输入日期，格式为 YYYYMMDD')
args = parser.parse_args()

# 根据输入的日期构建 CSV 文件路径
input_date = args.date
csv_file_path = f"C:/Users/16532/Desktop/{input_date}_sh_trade.csv"

# 读取 CSV 文件
try:
    df = pd.read_csv(csv_file_path)
except FileNotFoundError:
    print(f"文件 {csv_file_path} 未找到，请检查路径或日期是否正确！")
    exit()

# 修复 time 解析警告
df['TradeTime'] = pd.to_datetime(df['TradeTime'], format='%H:%M:%S.%f', errors='coerce')

# 定义交易时间范围
start_time = datetime.datetime.strptime("09:30:00", "%H:%M:%S").time()
end_time = datetime.datetime.strptime("10:30:00", "%H:%M:%S").time()

# 筛选条件：交易时间的 time 部分 >= start_time 且 <= end_time
filtered_df = df[
    (df['TradeTime'].dt.time >= start_time) & 
    (df['TradeTime'].dt.time <= end_time)
]

policy = 'alpha_clear'
if policy == 'alpha_clear':
    filtered_df = filtered_df[
        filtered_df['ClientStrategyId'].str.contains('alpha_clear', case=False, na=False)
    ]
elif policy == 'alpha_build':
    filtered_df = filtered_df[
        filtered_df['ClientStrategyId'].str.contains('alpha_build', case=False, na=False)
    ]

# 计算加权平均价格
def weighted_average(group):
    buy_mask = group['Direction'] == 'BUY'
    sell_mask = group['Direction'] == 'SELL'
    
    # 买入
    buy_volume = group.loc[buy_mask, 'Volume'].sum()
    avg_buy_price = (group.loc[buy_mask, 'Price'] * group.loc[buy_mask, 'Volume']).sum() / buy_volume if buy_volume != 0 else 0
    
    # 卖出
    sell_volume = group.loc[sell_mask, 'Volume'].sum()
    avg_sell_price = (group.loc[sell_mask, 'Price'] * group.loc[sell_mask, 'Volume']).sum() / sell_volume if sell_volume != 0 else 0
    
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

# 计算 TWAP
twap_results = []
for instrument_id in result['InstrumentId']:
    try:
        mf_instrument = DDBSUtils.stock_aligned_taq_data(instrument_id, input_date)
        mf_instrument['UpdateTime'] = pd.to_datetime(mf_instrument['UpdateTime'], format='%H:%M:%S', errors='coerce')
        
        filtered_mf = mf_instrument[
            (mf_instrument['UpdateTime'].dt.time >= start_time) & 
            (mf_instrument['UpdateTime'].dt.time <= end_time)
        ]
        
        if not filtered_mf.empty:
            filtered_mf['TWAP'] = (filtered_mf['BuyPrice01'] + filtered_mf['SellPrice01']) / 2
            twap_avg = filtered_mf['TWAP'].mean()
            twap_results.append({'InstrumentId': instrument_id, 'TWAP': twap_avg})
        else:
            twap_results.append({'InstrumentId': instrument_id, 'TWAP': None})
    except Exception as e:
        print(f"Error processing {instrument_id}: {e}")
        twap_results.append({'InstrumentId': instrument_id, 'TWAP': None})

twap_df = pd.DataFrame(twap_results)

# 合并结果
final_result = pd.merge(result, twap_df, on='InstrumentId', how='left')

# 计算报单收益
final_result['Order Buy Profit'] = (final_result['TWAP'] - final_result['Average Buy Price']) * final_result['Total Buy Volume']
final_result['Order Sell Profit'] = (final_result['Average Sell Price'] - final_result['TWAP']) * final_result['Total Sell Volume']
final_result['Order Profit'] = final_result['Order Buy Profit'] + final_result['Order Sell Profit']

# 确保列存在且为数值类型
final_result[['Order Buy Profit', 'Order Sell Profit', 'Order Profit']] = final_result[['Order Buy Profit', 'Order Sell Profit', 'Order Profit']].fillna(0)

# 计算总收益和收益率
Order_Profit = final_result['Order Profit'].sum()
money = (final_result['Average Buy Price'] * final_result['Total Buy Volume'] + final_result['Average Sell Price'] * final_result['Total Sell Volume']).sum()
Order_Profit_ratio = Order_Profit / money if money != 0 else 0

print(f"报单收益: {Order_Profit:.7f}")
print(f"报单收益率: {Order_Profit_ratio:.7f}")

# 保存结果
final_result.to_csv('final_result5.csv', index=False)