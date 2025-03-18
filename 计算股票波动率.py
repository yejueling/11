# 计算股票波动率
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


if __name__ == '__main__':   
    # df = DDBSUtils.index_min_data('000905.SH','20250214','20250214',data_source="TAQ")
    # print(df)
    ddf = DDBSUtils.stock_daily_minute_data('300458.SZ', '20250206', data_source="TAQ")
    print(ddf)
    # mf = DDBSUtils.astock_list('20250214', exchange="ALL")
    # print(mf)

    minute_data = ddf.iloc[:, 4]
    print(minute_data)
    # 转换为 Pandas Series
data_series = pd.Series(minute_data)

# 计算收益率
returns = data_series.pct_change()

# 计算波动率（标准差）
volatility = returns.std()

# 输出结果
print(f"波动率: {volatility:.8f}")


# # 读取CSV文件
#     file_path = 'C:/Users/16532/Desktop/20250217_zjiffund1.csv'  # 替换为你的CSV文件路径
#     ddf = pd.read_csv(file_path)
#     positions = ddf.iloc[:, [2, 3]]

# # 打印结果
#     print("股票的仓位信息：")
#     print(positions)


#     df = DDBSUtils.daily_stock_data('20250217')

#     print(df)
#     print(ddf)

#     # 合并两个数据源
#     mer = pd.merge(ddf[['Symbol', 'Pos']],
#                    df[['InstrumentId', 'Close']],
#                    left_on='Symbol', right_on='InstrumentId', how='left')
#     mer['Close'] = mer['Close'].fillna(0)
#     print(mer)

#     # 计算 Pos * Close 并相加
#     total_value = (mer['Pos'] * mer['Close']).sum()

# # 输出结果
#     print(f"0217总市值: {total_value}")


#     mdf = DDBSUtils.daily_stock_data('20250214')


#     # 合并两个数据源
#     mmer = pd.merge(ddf[['Symbol', 'Pos']],
#                    mdf[['InstrumentId', 'Close']],
#                    left_on='Symbol', right_on='InstrumentId', how='left')
#     mmer['Close'] = mmer['Close'].fillna(0)
#     print(mmer)

#     # 计算 Pos * Close 并相加
#     mtotal_value = (mmer['Pos'] * mmer['Close']).sum()

# # 输出结果
#     print(f"0214总市值: {mtotal_value}")


