from higgsboom.user import *
from higgsboom.data.market.cnsecurity import *
from higgsboom.data.market.cnfutures import *
import pandas as pd
import numpy as np
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from higgsboom.funcutil.datetime import *
from higgsboom.data.tradinglog.tradinglog import *
# 配置用户信息
ddb_config = {
        "Cluster": "Research",
        "UserName": "yejueling",
        "Password": "passwd123456"
    }
set_higgsboom_user_config("ddb_config", ddb_config)

    # 初始化工具
DDBSUtils = CNSecurityMarketDataUtils()

def main():

    # 读取 CSV 文件并指定列名
    df = pd.read_csv("C:/Users/16532/Desktop/20250225_sh_trade.csv")
    hf = pd.read_csv("C:/Users/16532/Desktop/20250225_sh_masterorder.csv")

    # 输出数据
    print(df)
    # 提取第一个交易时间
    first_trading_time = hf['trading_time'].iloc[0]  # 获取第一行的 trading_time

    # 去掉中括号并分割字符串
    start_time, end_time = first_trading_time.strip("[]").split(",")

    # 去掉多余的空格
    start_time = start_time.strip()
    end_time = end_time.strip()

    start_time = datetime.datetime.strptime(start_time, '%H:%M:%S').time()
    end_time = datetime.datetime.strptime(end_time, '%H:%M:%S').time()


    # 打印开始时间和结束时间
    print(f"开始时间: {start_time}")
    print(f"结束时间: {end_time}")

    # 修复时间解析
    df['TradeTime'] = pd.to_datetime(df['TradeTime'], errors='coerce')

    # 检查是否所有时间都被成功解析
    if df['TradeTime'].isna().any():
        print("Warning: Some TradeTime values could not be converted to datetime.")

    # # 筛选条件：交易时间的 time 部分 >= start_time 且 <= end_time
    # filtered_df = df[(df['TradeTime'].dt.time >= start_time) & (df['TradeTime'].dt.time <= end_time)]

    # 筛选策略
    filtered_df = df[df['ClientStrategyId'].str.contains('alpha_clear', case=False, na=False)|
                     df['ClientStrategyId'].str.contains('alpha_build', case=False, na=False)]

    # 输出结果
    print(filtered_df)

    # 计算加权平均价格
    def weighted_average(group):
        buy_mask = group['Direction'] == 'BUY'
        sell_mask = group['Direction'] == 'SELL'

        # 买入
        buy_volume = group.loc[buy_mask, 'Volume'].sum() if buy_mask.any() else 0
        avg_buy_price = (group.loc[buy_mask, 'Price'] * group.loc[buy_mask, 'Volume']).sum() / buy_volume if buy_volume != 0 else None

        # 卖出
        sell_volume = group.loc[sell_mask, 'Volume'].sum() if sell_mask.any() else 0
        avg_sell_price = (group.loc[sell_mask, 'Price'] * group.loc[sell_mask, 'Volume']).sum() / sell_volume if sell_volume != 0 else None

        # 合并逻辑：如果 avg_buy_price 或 avg_sell_price 非零，则取非零值
        if avg_buy_price is not None and buy_volume != 0:
            avg_price = avg_buy_price
            volume = buy_volume
            direction = 'BUY'  # 添加 Direction 为 'BUY'
        elif avg_sell_price is not None and sell_volume != 0:
            avg_price = avg_sell_price
            volume = sell_volume
            direction = 'SELL'  # 添加 Direction 为 'SELL'
        else:
            avg_price = None
            volume = 0
            direction = None  # 如果都没有，则 Direction 为 None

        return pd.Series({
            'Average Price': avg_price,
            'Total Volume': volume,
            'Direction': direction  # 添加 Direction 列
        })

    # 按 InstrumentId 分组并应用函数
    result = filtered_df.groupby('InstrumentId').apply(weighted_average, include_groups=False).reset_index()
    result.fillna(0, inplace=True)

    # 输出结果
    print(result)

    # 遍历每个 InstrumentId，计算对应的 TWAP
    twap_results = []

    for instrument_id in result['InstrumentId']:
        # 获取当前 InstrumentId 的数据
        mf_instrument = DDBSUtils.stock_aligned_taq_data(instrument_id, '20250225')
        mf_instrument['UpdateTime'] = pd.to_datetime(mf_instrument['UpdateTime'], format='%H:%M:%S')

        # 过滤数据
        filtered_mf_instrument = mf_instrument[
            (mf_instrument['UpdateTime'].dt.time >= start_time) & 
            (mf_instrument['UpdateTime'].dt.time <= end_time)
        ]

        # 如果过滤后的数据为空，跳过当前 InstrumentId
        if filtered_mf_instrument.empty:
            twap_results.append({'InstrumentId': instrument_id, 'TWAP': None})
            continue

        # 涨跌停时处理 BuyPrice01 和 SellPrice01 的逻辑
        filtered_mf_instrument['BuyPrice01'] = np.where(
            filtered_mf_instrument['BuyPrice01'] == 0,
            filtered_mf_instrument['SellPrice01'],
            filtered_mf_instrument['BuyPrice01']
        )

        filtered_mf_instrument['SellPrice01'] = np.where(
            filtered_mf_instrument['SellPrice01'] == 0,
            filtered_mf_instrument['BuyPrice01'],
            filtered_mf_instrument['SellPrice01']
        )

        # 计算 TWAP
        filtered_mf_instrument['TWAP'] = (filtered_mf_instrument['BuyPrice01'] + filtered_mf_instrument['SellPrice01']) / 2
        twap_instrument = filtered_mf_instrument['TWAP'].mean()

        twap_results.append({'InstrumentId': instrument_id, 'TWAP': twap_instrument})

    # 将结果转换为 DataFrame
    twap_df = pd.DataFrame(twap_results)
    print(twap_df)

    # 合并结果
    final_result = pd.merge(result, twap_df, on='InstrumentId', how='left')

    # 计算报单收益
    if final_result['Direction'].iloc[0] == 'BUY':
        final_result['Order Profit'] = (
            (final_result['TWAP'] - final_result['Average Price']) * final_result['Total Volume']
        )
    elif final_result['Direction'].iloc[0] == 'SELL':
        final_result['Order Profit'] = (
            (final_result['Average Price'] - final_result['TWAP']) * final_result['Total Volume']
        )

    # 输出最终结果
    print(final_result)

    # 计算总收益和收益率
    Order_Profit = final_result['Order Profit'].sum()
    money = (final_result['Average Price'] * final_result['Total Volume']).sum()
    Order_Profit_ratio = Order_Profit / money if money != 0 else 0

    print(f"成交额: {money:.7f}")
    print(f"报单收益: {Order_Profit:.7f}")
    print(f"报单收益率: {Order_Profit_ratio:.7f}")

    # 保存结果
    final_result.to_csv('final_result3.csv', index=False)

if __name__ == "__main__":
    main()