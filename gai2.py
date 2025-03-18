from higgsboom.user import *
from higgsboom.data.market.cnsecurity import *
from higgsboom.data.market.cnfutures import *
# 配置用户信息
ddb_config = {
        "Cluster": "Research",
        "UserName": "yejueling",
        "Password": "passwd123456"
    }
set_higgsboom_user_config("ddb_config", ddb_config)
import pandas as pd
import numpy as np
import datetime
from typing import Tuple, Optional
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from higgsboom.funcutil.datetime import *
from higgsboom.data.tradinglog.tradinglog import *
DDBSUtils = CNSecurityMarketDataUtils()


class TradeAnalyzer:
    def __init__(self, trade_path, master_order_path, trade_date):
        self.trade_path = trade_path
        self.master_order_path = master_order_path
        self.trade_date = trade_date
        self.df = None
        self.hf = None
        self.start_time = None
        self.end_time = None

    def load_data(self):
        """加载交易数据和母单数据"""
        try:
            self.df = pd.read_csv(self.trade_path)
            self.hf = pd.read_csv(self.master_order_path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"数据文件未找到: {e}")

    def parse_trading_time(self):
        """解析交易时间范围"""
        if self.hf.empty:
            raise ValueError("主订单数据为空，无法解析交易时间")

        time_str = self.hf['trading_time'].iloc[0].strip("[]")
        start_str, end_str = map(str.strip, time_str.split(","))
        
        try:
            self.start_time = datetime.datetime.strptime(start_str, '%H:%M:%S').time()
            self.end_time = datetime.datetime.strptime(end_str, '%H:%M:%S').time()
        except ValueError as e:
            raise ValueError(f"时间格式错误: {e}")

    def preprocess_data(self):
        """筛选TWAP交易数据"""
        filtered_df = self.df[
            self.df['ClientStrategyId'].str.contains('alpha_clear|alpha_build', case=False, na=False)
        ]
        return filtered_df


    def calculate_average(group, df_mask, direction):
        """计算平均价格和总量"""
        if not df_mask.any():
            return None, 0
        volume = group.loc[df_mask, 'Volume'].sum()
        avg_price = (group.loc[df_mask, 'Price'] * group.loc[df_mask, 'Volume']).sum() / volume
        return avg_price, volume

    @staticmethod
    def calculate_weighted_average(group):
        """计算加权平均价格"""
        buy_mask = group['Direction'] == 'BUY'
        sell_mask = group['Direction'] == 'SELL'

        avg_buy, buy_vol = TradeAnalyzer.calculate_average(group, buy_mask, 'BUY')
        avg_sell, sell_vol = TradeAnalyzer.calculate_average(group, sell_mask, 'SELL')

        if avg_buy is not None and buy_vol != 0:
            return pd.Series({
                'Average Price': avg_buy,
                'Total Volume': buy_vol,
                'Direction': 'BUY'
            })
        elif avg_sell is not None and sell_vol != 0:
            return pd.Series({
                'Average Price': avg_sell,
                'Total Volume': sell_vol,
                'Direction': 'SELL'
            })
        else:
            return pd.Series({
                'Average Price': None,
                'Total Volume': 0,
                'Direction': None
            })

    def process_trades(self):
        """调用calculate_weighted_average计算个股平均成交价格"""
        filtered_df = self.preprocess_data()
        result = filtered_df.groupby('InstrumentId').apply(self.calculate_weighted_average, include_groups=False).reset_index()
        return result.fillna(0)

    def calculate_twap(self, instrument_id):
        """计算单个Instrument的TWAP基准"""
        mf_data = DDBSUtils.stock_aligned_taq_data(instrument_id, self.trade_date)
        if mf_data.empty:
            return None
        # 筛选交易时间
        mf_data['UpdateTime'] = pd.to_datetime(mf_data['UpdateTime'], format='%H:%M:%S')
        time_filter = (mf_data['UpdateTime'].dt.time >= self.start_time) & \
                     (mf_data['UpdateTime'].dt.time <= self.end_time)
        filtered_data = mf_data[time_filter]

        if filtered_data.empty:
            return None

        # 处理涨跌停价格
        filtered_data = filtered_data.assign(
            BuyPrice01 = np.where(filtered_data['BuyPrice01'] == 0, 
                               filtered_data['SellPrice01'], 
                               filtered_data['BuyPrice01']),
            SellPrice01 = np.where(filtered_data['SellPrice01'] == 0,
                                filtered_data['BuyPrice01'],
                                filtered_data['SellPrice01'])
        )

        return (filtered_data['BuyPrice01'] + filtered_data['SellPrice01']).mean() / 2

    def generate_final_report(self, result_df):
        """生成明细"""
        twap_results = []
        for instrument_id in result_df['InstrumentId']:
            twap = self.calculate_twap(instrument_id)
            twap_results.append({'InstrumentId': instrument_id, 'TWAP': twap})

        twap_df = pd.DataFrame(twap_results)
        final_df = pd.merge(result_df, twap_df, on='InstrumentId', how='left')

        # 计算报单收益
        def _calculate_profit(row: pd.Series) -> float:
            if row['Direction'] == 'BUY':
                return (row['TWAP'] - row['Average Price']) * row['Total Volume']
            elif row['Direction'] == 'SELL':
                return (row['Average Price'] - row['TWAP']) * row['Total Volume']
            return 0.0

        final_df['Order Profit'] = final_df.apply(_calculate_profit, axis=1)
        return final_df

    def run_analysis(self):
        """执行输出"""
        self.load_data()
        self.parse_trading_time()
        
        processed_data = self.process_trades()
        final_report = self.generate_final_report(processed_data)
        
        # 计算总报单收益、成交额和报单收益率
        total_profit = final_report['Order Profit'].sum()
        total_volume = (final_report['Average Price'] * final_report['Total Volume']).sum()
        profit_ratio = total_profit / total_volume if total_volume != 0 else 0

        print("明细:")
        print(final_report)
        print(f"\n成交额: {total_volume:.2f}")
        print(f"报单收益: {total_profit:.2f}")
        print(f"报单收益率: {profit_ratio:.7f}")

        # 保存结果
        final_report.to_csv('final_result.csv', index=False)
        # print("分析结果已保存至 final_result.csv")

if __name__ == "__main__":
    analyzer = TradeAnalyzer(
        trade_path="C:/Users/16532/Desktop/20250225_sh_trade.csv",
        master_order_path="C:/Users/16532/Desktop/20250225_sh_masterorder.csv",
        trade_date="20250225"
    )
    analyzer.run_analysis()