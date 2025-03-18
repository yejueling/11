import pandas as pd
from higgsboom.user import *
from higgsboom.data.market.cnsecurity import *
DDBSUtils = CNSecurityMarketDataUtils()
ddb_config =  {"Cluster": "Research",
              "UserName": "yejueling",
              "Password": "passwd123456"}
set_higgsboom_user_config("ddb_config", ddb_config)
# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)
from datetime import datetime




def generate_stock_position_csv(name, amount, date, stock_index, trading_time_twap, trading_time_t0, auction_vol, debt_available_vol, return_vol, can_limit_open):
    """
    封装后的函数，用于生成股票仓位的 CSV 文件。
    
    参数：
        amount (float): 总金额，用于计算股票仓位。
        date (str): 日期，格式为 'YYYYMMDD'。
        stock_index (str): 股票指数代码，例如 '000905.SH'。
        policy (str): 策略标识，默认值为 'clear'（可选值：clear, T0, build）。
        percent: 做T占总量的百分比。
        auction_vol: 开盘集合竞价买卖的量, 只对TWAP有效, T0应为0
        debt_available_vol: 融券量
        return_vol: 今天所需还券量
        is_t0_strict: 检查yd是否和柜台一致, 只有没有全部做T时才为False
        can_limit_open:是否被动单, 主动单为False
    """
    # 获取股票数据
    df = DDBSUtils.daily_stock_data(date)
    ddf = DDBSUtils.index_daily_weight_data(stock_index, date)
    print(df)
    print(ddf)

    # 合并两个数据源
    mer = pd.merge(ddf[['TradingDate', 'StockID', 'Weight']],
                   df[['InstrumentId', 'Close']],
                   left_on='StockID', right_on='InstrumentId', how='left')
    mer['Close'] = mer['Close'].fillna(0)

    # 导入底仓数据

    # 读取CSV文件
    file_path = 'C:/Users/16532/Desktop/exchange_pos.csv'
    data = pd.read_csv(file_path)

    # 选择特定列
    selected_data = data[['InstrumentId', 'LongYdAvailable']]

    # 显示数据
    print(selected_data)
    # 合并 LongYdAvailable 列到 mer
    mer = pd.merge(mer, selected_data, on='InstrumentId', how='left')
    mer = mer.dropna(subset=['LongYdAvailable'])

    # 计算交易量
    
    mer['amount'] = (mer['Weight'] * amount).astype(int)
    mer['vol'] = (mer['amount'] / mer['Close'] / 100).astype(int)
    voll = (mer.iloc[:, -1]  / 100).astype(int) * 100
    mer['voll'] = voll.astype(int)
    mer['LongYdAvailable'] = mer['LongYdAvailable'].astype(int)




    
    # 调整列顺序
    columns = mer.columns.tolist()
    columns[1], columns[2] = columns[2], columns[1]
    mer = mer[columns]
    new_mer = mer.copy()

    for index, row in mer.iterrows():
        if row['LongYdAvailable'] < row['voll']:
            policy = 'build'
            build_vol = row['voll'] - row['LongYdAvailable']
            T0_vol = row['LongYdAvailable']
        elif row['LongYdAvailable'] > row['voll']:
            policy = 'clear'
            clear_vol = row['LongYdAvailable'] - row['voll']
            T0_vol = row['voll']
        else:
            policy = 'T0'  
            T0_vol = row['LongYdAvailable']



        mer.at[index, 'T0_vol'] = T0_vol


        
   
        # yd_long_vol = new_mer.at[index, 'LongYdAvailable']
        new_mer.at[index, 'long_vol'] = T0_vol # 在 T0 时，保持现有逻辑
        new_mer.at[index, 'tod_long_vol'] = 0
        new_mer.at[index, 'yd_long_vol'] =  T0_vol

        if 'build_vol' in locals():
            mer.at[index, 'Build_Vol'] = build_vol
        if 'clear_vol' in locals():
            mer.at[index, 'Clear_Vol'] = clear_vol



        if policy == 'build':
            mer.at[index, 'long_vol'] = build_vol # 更新 long_vol
            mer.at[index, 'tod_long_vol'] = 0
            mer.at[index, 'yd_long_vol'] = 0
        elif policy == 'clear':
            mer.at[index, 'long_vol'] = clear_vol # 更新 long_vol
            mer.at[index, 'tod_long_vol'] = 0
            mer.at[index, 'yd_long_vol'] = clear_vol




   # T0母单
    new_mer['long_vol'] = new_mer['long_vol'].astype(int)
    new_mer['tod_long_vol'] = new_mer['tod_long_vol'].astype(int)
    new_mer['yd_long_vol'] = new_mer['yd_long_vol'].astype(int)

    new_mer['trading_time'] = trading_time_t0
    new_mer['policy'] = 'T0'  


    new_mer.insert(1, 'product_id', f'{name}{policy.capitalize()}')  # 插入产品 ID
    new_mer.insert(2, 'parent_key', '')  # 插入 parent_key

    # 获取当前时间的时分
    current_time = datetime.now().strftime("%H%M")  # 格式化当前时间
    new_mer.iloc[:, 2] = new_mer.iloc[:, 4].astype(str) + f'{policy}_{current_time}_{name}{policy.capitalize()}'


    total_sum = (new_mer['Close'] * new_mer['long_vol']).sum()
    print("Close * long_vol 的总和:", total_sum)

    new_mer = new_mer.drop(['TradingDate','StockID', 'Weight', 'Close', 'amount', 'vol', 'voll'], axis=1)

    # 添加其他列
    new_mer['auction_vol'] = auction_vol
    new_mer['debt_available_vol'] = debt_available_vol
    new_mer['return_vol'] = return_vol
    if new_mer.at[index, 'LongYdAvailable'] != new_mer.at[index, 'long_vol']:
        new_mer['is_t0_strict'] = False
    else:  
        new_mer['is_t0_strict'] = True

    if new_mer.at[index, 'policy'] == 'T0':
        new_mer['max_pos_ratio'] = 0.3
    else:  
        new_mer['max_pos_ratio'] = 0.1

    new_mer['can_limit_open'] = can_limit_open

    # 保存为 CSV 文件
    current_time_1 = datetime.now().strftime("%Y%m%d_%H%M%S")
    if new_mer.at[index, 'policy'] == 'T0':
        filename = f'ShannonStockPosition.{current_time_1}_t0.csv'
    else:  
        filename = f'ShannonStockPosition.{current_time_1}_twap.csv'
    new_mer.to_csv(filename, index=False, encoding='utf-8')

    print("生成的文件名:", filename)
    print(new_mer)




    # TWAP母单

    mer['long_vol'] = mer['long_vol'].astype(int)
    mer['tod_long_vol'] = mer['tod_long_vol'].astype(int)
    mer['yd_long_vol'] = mer['yd_long_vol'].astype(int)
    mer['trading_time'] = trading_time_twap  
    mer['policy'] = policy  


    mer.insert(1, 'product_id', f'{name}{policy.capitalize()}')  # 插入产品 ID
    mer.insert(2, 'parent_key', '')  # 插入 parent_key

    # 获取当前时间的时分
    current_time = datetime.now().strftime("%H%M")  # 格式化当前时间
    mer.iloc[:, 2] = mer.iloc[:, 4].astype(str) + f'{policy}_{current_time}_{name}{policy.capitalize()}'

    total_sum = (mer['Close'] * mer['long_vol']).sum()
    print("Close * long_vol 的总和:", total_sum)

    mer = mer.drop(['TradingDate','StockID', 'Weight', 'Close', 'amount', 'vol', 'voll', 'T0_vol', 'Clear_Vol'], axis=1)

    # 添加其他列
    mer['auction_vol'] = auction_vol
    mer['debt_available_vol'] = debt_available_vol
    mer['return_vol'] = return_vol
    if mer.at[index, 'LongYdAvailable'] != mer.at[index, 'long_vol']:
        mer['is_t0_strict'] = False
    else:  
        mer['is_t0_strict'] = True
    if policy == 'T0':
        mer['max_pos_ratio'] = 0.3
    else:  
        mer['max_pos_ratio'] = 0.1
    mer['can_limit_open'] = can_limit_open

    # 保存为 CSV 文件
    current_time_1 = datetime.now().strftime("%Y%m%d_%H%M%S")
    if policy == 'T0':
        filename = f'ShannonStockPosition.{current_time_1}_t0.csv'
    else:  
        filename = f'ShannonStockPosition.{current_time_1}_twap.csv'
    mer.to_csv(filename, index=False, encoding='utf-8')

    print("生成的文件名:", filename)
    print(mer)


# 示例调用
if __name__ == '__main__':
    generate_stock_position_csv(name = 'Csi500', 
                                amount=50000000, 
                                date='20241212', 
                                stock_index='000905.SH', 
                                trading_time_twap = '[09:30:00,10:30:00]', 
                                trading_time_t0 = '[09:30:00,11:30:00];[13:00:00,15:00:00]',  
                                auction_vol = 0, 
                                debt_available_vol = 0, 
                                return_vol = 0, 
                                can_limit_open = False
                                )




