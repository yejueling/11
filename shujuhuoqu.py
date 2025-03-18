import pandas as pd
import os
from higgsboom.data.market.cnsecurity import CNSecurityMarketDataUtils
from higgsboom.data.tradinglog.tradinglog import DDBTradeLogDataUtils
from datetime import datetime

# 初始化市场数据工具
DDBSUtils = CNSecurityMarketDataUtils()

def generate_stock_position_csv(
    name, 
    amount, 
    date, 
    stock_index, 
    trading_time_twap, 
    trading_time_t0, 
    auction_vol, 
    debt_available_vol, 
    return_vol, 
    can_limit_open
):
    """
    生成股票仓位的 CSV 文件。
    
    参数：
        name (str): 产品名称或标识。
        amount (float): 总金额，用于计算股票仓位。
        date (str): 日期，格式为 'YYYYMMDD'。
        stock_index (str): 股票指数代码，例如 '000905.SH'。
        trading_time_twap (str): TWAP 交易时间区间。
        trading_time_t0 (str): T0 交易时间区间。
        auction_vol (int): 开盘集合竞价买卖的量。
        debt_available_vol (int): 融券量。
        return_vol (int): 今天所需还券量。
        can_limit_open (bool): 是否被动单，主动单为False。
    """
    # 获取当前工作目录
    current_dir = os.getcwd()
    exchange_pos_path = os.path.join(current_dir, "C:/Users/16532/Desktop/exchange_pos.csv")

    # 获取市场数据
    daily_data = DDBSUtils.daily_stock_data(date)
    index_data = DDBSUtils.index_daily_weight_data(stock_index, date)

    # 合并数据
    merged_data = pd.merge(
        index_data[["TradingDate", "StockID", "Weight"]],
        daily_data[["InstrumentId", "Close"]],
        left_on="StockID",
        right_on="InstrumentId",
        how="left"
    ).fillna(0)

    # 导入底仓数据
    bottom_data = pd.read_csv(exchange_pos_path)[["InstrumentId", "LongYdAvailable"]]
    merged_data = pd.merge(
        merged_data, 
        bottom_data, 
        on="InstrumentId", 
        how="left"
    ).dropna(subset=["LongYdAvailable"])

    # 计算交易量
    merged_data["amount"] = (merged_data["Weight"] * amount).round().astype(int)
    merged_data["vol"] = (merged_data["amount"] / merged_data["Close"] / 100).round().astype(int)
    merged_data["voll"] = ((merged_data["vol"] / 100) // 1 * 100).astype(int)
    merged_data["LongYdAvailable"] = merged_data["LongYdAvailable"].round().astype(int)

    # 调整列顺序
    columns = merged_data.columns.tolist()
    columns[1], columns[2] = columns[2], columns[1]
    merged_data = merged_data[columns]

    # 确定策略和交易量
    policy_dict = {}
    for index, row in merged_data.iterrows():
        if row["LongYdAvailable"] < row["voll"]:
            policy = "build"
            build_vol = row["voll"] - row["LongYdAvailable"]
            T0_vol = row["LongYdAvailable"]
        elif row["LongYdAvailable"] > row["voll"]:
            policy = "clear"
            clear_vol = row["LongYdAvailable"] - row["voll"]
            T0_vol = row["voll"]
        else:
            policy = "T0"
            T0_vol = row["LongYdAvailable"]

        merged_data.at[index, "T0_vol"] = T0_vol

        if policy == "build":
            policy_dict[index] = {
                "Build_Vol": build_vol,
                "long_vol": build_vol,
                "tod_long_vol": 0,
                "yd_long_vol": 0
            }
        elif policy == "clear":
            policy_dict[index] = {
                "Clear_Vol": clear_vol,
                "long_vol": clear_vol,
                "tod_long_vol": 0,
                "yd_long_vol": clear_vol
            }
        else:
            policy_dict[index] = {
                "long_vol": T0_vol,
                "tod_long_vol": T0_vol,
                "yd_long_vol": T0_vol
            }

    # 更新交易量和策略信息
    for index, data in policy_dict.items():
        merged_data.at[index, "Build_Vol"] = data.get("Build_Vol", None)
        merged_data.at[index, "Clear_Vol"] = data.get("Clear_Vol", None)
        merged_data.at[index, "long_vol"] = data["long_vol"]
        merged_data.at[index, "tod_long_vol"] = data["tod_long_vol"]
        merged_data.at[index, "yd_long_vol"] = data["yd_long_vol"]

    # 生成母单数据
    def create_mother_order(order_data, trading_time, policy_type):
        order = order_data.copy()
        order["trading_time"] = trading_time
        order["policy"] = policy_type

        # 添加额外列
        order.insert(1, "product_id", f"{name}{policy.capitalize()}")
        order.insert(2, "parent_key", "")

        # 生成文件名前缀
        current_time_str = datetime.now().strftime("%H%M")
        order.iloc[:, 2] = order.iloc[:, 4].astype(str) + f"{policy_type}_{current_time_str}_{name}{policy.capitalize()}"

        # 添加其他列
        order["auction_vol"] = auction_vol
        order["debt_available_vol"] = debt_available_vol
        order["return_vol"] = return_vol
        order["is_t0_strict"] = (order["LongYdAvailable"] == order["long_vol"]).all()
        order["max_pos_ratio"] = 0.3 if policy_type == "T0" else 0.1
        order["can_limit_open"] = can_limit_open

        # 删除不必要的列
        columns_to_drop = [
            "TradingDate", 
            "StockID", 
            "Weight", 
            "amount", 
            "vol", 
            "voll", 
            "LongYdAvailable", 
            "T0_vol"
        ]
        if policy_type == "T0":
            columns_to_drop.extend(["Build_Vol", "Clear_Vol"])
        order = order.drop(columns=columns_to_drop, axis=1, errors="ignore")

        # 计算总和
        total_sum = (order["Close"] * order["long_vol"]).sum()
        print(f"Close * long_vol 的总和 ({policy_type}):", total_sum)

        return order
    
    # 确保 T0 和 TWAP 的数据框独立
    t0_data = merged_data.copy()
    twap_data = merged_data.copy()

    # 处理 T0 母单
    t0_order = create_mother_order(t0_data, trading_time_t0, "T0")

    # 处理 TWAP 母单
    twap_order = create_mother_order(twap_data, trading_time_twap, "TWAP")

    # 保存母单数据到 CSV
    def save_order(order, suffix):
        current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ShannonStockPosition.{current_time_str}_{suffix}.csv"
        order.to_csv(filename, index=False, encoding="utf-8")
        print(f"生成的文件名: {filename}")
        print(order)
    
    save_order(t0_order, "t0")
    save_order(twap_order, "twap")

# 示例调用
if __name__ == "__main__":
    generate_stock_position_csv(
        name="Csi500",
        amount=1000000,
        date="20241212",
        stock_index="000905.SH",
        trading_time_twap="[09:30:00,10:30:00]",
        trading_time_t0="[09:30:00,11:30:00];[13:00:00,15:00:00]",
        auction_vol=0,
        debt_available_vol=0,
        return_vol=0,
        can_limit_open=False,
    )