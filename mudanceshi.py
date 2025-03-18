import pandas as pd
from higgsboom.user import *
from higgsboom.data.market.cnsecurity import *
from datetime import datetime
from typing import Dict, Any

# 初始化市场数据工具
DDBSUtils = CNSecurityMarketDataUtils()
DDB_CONFIG = {
    "Cluster": "Research",
    "UserName": "yejueling",
    "Password": "passwd123456"
}
set_higgsboom_user_config("ddb_config", DDB_CONFIG)

# 常量定义
POSITION_FILE_PATH = 'C:/Users/16532/Desktop/exchange_pos.csv'
COLUMN_ORDER = ['TradingDate', 'StockID', 'Weight', 'InstrumentId', 'Close', 'LongYdAvailable']

class PositionGenerator:
    def __init__(self, params: Dict[str, Any]):
        """初始化生成器参数"""
        self.name = params['name']
        self.amount = params['amount']
        self.date = params['date']
        self.stock_index = params['stock_index']
        self.trading_time_twap = params['trading_time_twap']
        self.trading_time_t0 = params['trading_time_t0']
        self.auction_vol = params['auction_vol']
        self.debt_available_vol = params['debt_available_vol']
        self.return_vol = params['return_vol']
        self.can_limit_open = params['can_limit_open']
        
        # 中间数据存储
        self.merged_data = None
        self.t0_data = None
        self.twap_data = None

    def _load_base_data(self) -> None:
        """加载基础数据"""
        # 加载市场数据
        stock_data = DDBSUtils.daily_stock_data(self.date)
        index_weights = DDBSUtils.index_daily_weight_data(self.stock_index, self.date)
        
        # 合并数据
        self.merged_data = pd.merge(
            index_weights[['TradingDate', 'StockID', 'Weight']],
            stock_data[['InstrumentId', 'Close']],
            left_on='StockID', right_on='InstrumentId',
            how='left'
        ).fillna({'Close': 0})

        # 加载持仓数据
        position_data = pd.read_csv(POSITION_FILE_PATH)[['InstrumentId', 'LongYdAvailable']]
        self.merged_data = pd.merge(
            self.merged_data, 
            position_data, 
            on='InstrumentId', 
            how='left'
        ).dropna(subset=['LongYdAvailable'])

    def _calculate_position(self) -> None:
        """计算目标仓位"""
        # 资金分配计算
        self.merged_data = self.merged_data.assign(
            amount=lambda x: (x['Weight'] * self.amount).astype(int),
            vol=lambda x: (x['amount'] / x['Close'] / 100).astype(int),
            voll=lambda x: (x['vol'] // 100 * 100).astype(int),
            LongYdAvailable=lambda x: x['LongYdAvailable'].astype(int)
        )

        # 调整列顺序
        self.merged_data = self.merged_data[COLUMN_ORDER + ['voll']]

    def _determine_policy(self, row: pd.Series) -> Dict[str, Any]:
        """确定单个标的的交易策略"""
        policy_map = {
            'build': lambda r: {'policy': 'build', 'build_vol': r['voll'] - r['LongYdAvailable'], 'T0_vol': r['LongYdAvailable']},
            'clear': lambda r: {'policy': 'clear', 'clear_vol': r['LongYdAvailable'] - r['voll'], 'T0_vol': r['voll']},
            'T0': lambda r: {'policy': 'T0', 'T0_vol': r['LongYdAvailable']}
        }
        
        if row['LongYdAvailable'] < row['voll']:
            return policy_map['build'](row)
        elif row['LongYdAvailable'] > row['voll']:
            return policy_map['clear'](row)
        return policy_map['T0'](row)

    def _generate_order_data(self, df: pd.DataFrame, policy_type: str) -> pd.DataFrame:
        """生成订单数据模板"""
        df = df.copy()
        current_time = datetime.now().strftime("%H%M")
        
        # 公共字段设置
        df.insert(1, 'product_id', f"{self.name}{policy_type.capitalize()}")
        df.insert(2, 'parent_key', df.iloc[:, 4].astype(str) + 
                  f"{policy_type}_{current_time}_{self.name}{policy_type.capitalize()}")
        
        # 计算验证指标
        total_value = (df['Close'] * df['long_vol']).sum()
        print(f"Close * long_vol 总和: {total_value:.2f}")
        
        return df.drop(columns=['TradingDate', 'StockID', 'Weight', 'Close', 'amount', 'vol', 'voll'])

    def _build_t0_orders(self) -> None:
        """生成T0订单"""
        t0_data = self.merged_data.copy()
        t0_data['policy'] = 'T0'
        t0_data['trading_time'] = self.trading_time_t0
        t0_data['max_pos_ratio'] = 0.3
        
        # 策略特定字段
        t0_data[['long_vol', 'tod_long_vol', 'yd_long_vol']] = t0_data.apply(
            lambda r: [r['T0_vol'], 0, r['T0_vol']], 
            axis=1, result_type='expand'
        )
        
        self.t0_data = self._generate_order_data(t0_data, 'T0')

    def _build_twap_orders(self) -> None:
        """生成TWAP订单"""
        def apply_policy(row):
            policy = self._determine_policy(row)
            return pd.Series({
                'policy': policy['policy'],
                'long_vol': policy.get('build_vol') or policy.get('clear_vol') or 0,
                'tod_long_vol': 0,
                'yd_long_vol': policy.get('clear_vol') or 0
            })
            
        twap_data = self.merged_data.copy()
        twap_data = twap_data.join(twap_data.apply(apply_policy, axis=1))
        twap_data['trading_time'] = self.trading_time_twap
        twap_data['max_pos_ratio'] = 0.1
        
        self.twap_data = self._generate_order_data(twap_data, twap_data['policy'].iloc[0])

    def _add_common_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加公共字段"""
        return df.assign(
            auction_vol=self.auction_vol,
            debt_available_vol=self.debt_available_vol,
            return_vol=self.return_vol,
            is_t0_strict=df['LongYdAvailable'] == df['long_vol'],
            can_limit_open=self.can_limit_open
        ).drop(columns=['LongYdAvailable'])

    def generate_orders(self) -> None:
        """执行完整生成流程"""
        self._load_base_data()
        self._calculate_position()
        
        # 生成两种订单
        self._build_t0_orders()
        self._build_twap_orders()
        
        # 添加公共字段
        self.t0_data = self._add_common_columns(self.t0_data)
        self.twap_data = self._add_common_columns(self.twap_data)

    def save_results(self) -> None:
        """保存结果文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        t0_filename = f'ShannonStockPosition.{timestamp}_t0.csv'
        self.t0_data.to_csv(t0_filename, index=False, encoding='utf-8')
        print(f"生成T0文件: {t0_filename}")
        
        twap_filename = f'ShannonStockPosition.{timestamp}_twap.csv'
        self.twap_data.to_csv(twap_filename, index=False, encoding='utf-8')
        print(f"生成TWAP文件: {twap_filename}")

if __name__ == '__main__':
    config = {
        'name': 'Csi500',
        'amount': 3000000,
        'date': '20241212',
        'stock_index': '000905.SH',
        'trading_time_twap': '[09:30:00,10:30:00]',
        'trading_time_t0': '[09:30:00,11:30:00];[13:00:00,15:00:00]',
        'auction_vol': 0,
        'debt_available_vol': 0,
        'return_vol': 0,
        'can_limit_open': False
    }
    
    generator = PositionGenerator(config)
    generator.generate_orders()
    generator.save_results()