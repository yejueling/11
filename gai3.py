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
    def __init__(self, trade_path: str, master_order_path: str, trade_date: str):
        self.trade_path = trade_path
        self.master_order_path = master_order_path
        self.trade_date = trade_date
        self.df = None
        self.hf = None
        self.start_time = None
        self.end_time = None

    def load_data(self) -> None:
        """加载数据 (合并异常处理)"""
        try:
            self.df = pd.read_csv(self.trade_path)
            self.hf = pd.read_csv(self.master_order_path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"文件缺失: {e}") from e

    def parse_trading_time(self) -> None:
        """解析交易时间 (使用向量化操作)"""
        if self.hf.empty:
            raise ValueError("主订单数据为空")
        
        # 使用split扩展列替代多次字符串处理
        time_parts = (
            self.hf['trading_time'].iloc[0]
            .strip("[]").split(", ")
        )
        self.start_time, self.end_time = [
            datetime.strptime(t, '%H:%M:%S').time()
            for t in time_parts
        ]

    def preprocess_data(self) -> pd.DataFrame:
        """预处理数据 (简化时间转换)"""
        self.df['TradeTime'] = pd.to_datetime(
            self.df['TradeTime'], 
            format='%H:%M:%S.%f', 
            errors='coerce'
        )
        # 筛选策略数据
        return self.df[
            self.df['ClientStrategyId']
            .str.contains('alpha_clear|alpha_build', case=False, na=False)
        ]

    def _calculate_avg_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算平均价格 (向量化实现)"""
        # 按方向分组计算
        grouped = df.groupby('Direction').agg(TotalVolume=('Volume', 'sum'), WeightedPrice=('Price', lambda x: (x * df.loc[x.index, 'Volume']).sum() / df.loc[x.index, 'Volume'].sum())).reset_index()
        
        # 选择成交量最大的方向
        main_direction = grouped.loc[grouped['TotalVolume'].idxmax()] if not grouped.empty else None
        
        return pd.Series({
            'Average Price': main_direction['WeightedPrice'] if main_direction is not None else None,
            'Total Volume': main_direction['TotalVolume'] if main_direction is not None else 0,
            'Direction': main_direction.name if main_direction is not None else None
        })

    def process_trades(self) -> pd.DataFrame:
        """处理交易数据 (简化分组逻辑)"""
        filtered_df = self.preprocess_data()
        return filtered_df.groupby('InstrumentId').apply(
            self._calculate_avg_prices
        ).reset_index().fillna(0)

    def _process_twap_data(self, mf_data: pd.DataFrame) -> Optional[float]:
        """TWAP计算逻辑 (向量化处理)"""
        mf_data['UpdateTime'] = pd.to_datetime(mf_data['UpdateTime'], format='%H:%M:%S')
        filtered = mf_data.query(
            "@self.start_time <= UpdateTime.dt.time <= @self.end_time"
        )
        if filtered.empty:
            return None
        
        # 处理涨跌停价格
        filtered = filtered.assign(
            BuyPrice01 = filtered['BuyPrice01'].mask(filtered['BuyPrice01'] == 0, filtered['SellPrice01']),
            SellPrice01 = filtered['SellPrice01'].mask(filtered['SellPrice01'] == 0, filtered['BuyPrice01'])
        )
        return filtered[['BuyPrice01', 'SellPrice01']].mean(axis=1).mean()

    def generate_final_report(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """生成报告 (消除循环)"""
        # 向量化计算TWAP
        result_df['TWAP'] = result_df['InstrumentId'].apply(
            lambda x: DDBSUtils.stock_aligned_taq_data(x, self.trade_date)
            .pipe(self._process_twap_data)
        )
        
        # 向量化计算收益
        buy_mask = result_df['Direction'] == 'BUY'
        sell_mask = result_df['Direction'] == 'SELL'
        result_df['Order Profit'] = (
            (result_df['TWAP'] - result_df['Average Price']) * result_df['Total Volume'] * buy_mask +
            (result_df['Average Price'] - result_df['TWAP']) * result_df['Total Volume'] * sell_mask
        )
        return result_df

    def run_analysis(self) -> None:
        """主流程 (简化输出)"""
        self.load_data()
        self.parse_trading_time()
        
        final_df = self.process_trades().pipe(self.generate_final_report)
        
        # 计算汇总指标
        total_volume = (final_df['Average Price'] * final_df['Total Volume']).sum()
        total_profit = final_df['Order Profit'].sum()
        
        print(f"成交额: {total_volume:.2f}\n"
              f"报单收益: {total_profit:.2f}\n"
              f"收益率: {total_profit / total_volume:.2%}")

        final_df.to_csv('final_result.csv', index=False)

if __name__ == "__main__":
    analyzer = TradeAnalyzer(
        trade_path="C:/Users/16532/Desktop/20250225_sh_trade.csv",
        master_order_path="C:/Users/16532/Desktop/20250225_sh_masterorder.csv",
        trade_date="20250225"
    )
    analyzer.run_analysis()