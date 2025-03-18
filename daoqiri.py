import pandas as pd
import numpy as np
import pandas as pd
from datetime import timedelta
# 设定年份
year = 2024

# 定义每个月的期货到期日（通常是每个月的第三个星期五）
def get_monthly_expiry(year):
    monthly_expiry = []
    for month in range(1, 13):
        # 找到该月的第三个星期五
        third_friday = pd.date_range(start=f'{year}-{month:02d}-01', 
                                      end=f'{year}-{month:02d}-31', 
                                      freq='W-FRI')[2]  # 选择第三个星期五
        monthly_expiry.append(third_friday)
    return monthly_expiry


def get_monthly_expiry(year):
    monthly_expiry_dates = []
    
    for month in range(1, 13):  # 1到12月
        # 获取该月的第一天
        first_day = pd.Timestamp(f'{year}-{month:02d}-01')
        
        # 获取该月的第一个星期五
        first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
        
        # 计算第三个星期五
        third_friday = first_friday + timedelta(weeks=2)
        
        monthly_expiry_dates.append(third_friday)
    
    return monthly_expiry_dates

# 示例调用
year = 2024
expiry_dates = get_monthly_expiry(year)
print(expiry_dates)

# 定义每个季度的期货到期日（通常是每个季度的最后一个工作日）
def get_quarterly_expiry(year):
    quarterly_expiry = []
    for quarter in range(1, 5):
        # 定义每个季度的结束月份
        if quarter == 1:
            end_month = 3
        elif quarter == 2:
            end_month = 6
        elif quarter == 3:
            end_month = 9
        else:
            end_month = 12
        
        # 找到该季度的最后一个工作日
        last_day = pd.date_range(start=f'{year}-{end_month:02d}-01', 
                                  end=f'{year}-{end_month:02d}-31', 
                                  freq='B')[-1]  # 选择最后一个工作日
        quarterly_expiry.append(last_day)
    return quarterly_expiry

import pandas as pd

def get_quarterly_expiry(year):
    # 每个季度的最后一个月
    quarterly_months = [3, 6, 9, 12]
    expiry_dates = []

    for month in quarterly_months:
        # 获取每个季度的最后一天
        last_day = pd.Period(f'{year}-{month:02d}').end_time
        expiry_dates.append(last_day)

    return expiry_dates

# 示例用法
year = 2024
quarterly_expiry_dates = get_quarterly_expiry(year)
print(quarterly_expiry_dates)

# 获取2024年每个月的到期日
monthly_expiry_dates = get_monthly_expiry(year)

# 获取2024年每个季度的到期日
quarterly_expiry_dates = get_quarterly_expiry(year)

# 合并结果
expiry_dates = {
    'monthly_expiry': monthly_expiry_dates,
    'quarterly_expiry': quarterly_expiry_dates
}

# 打印结果
print("2024年每个月的到期日:")
for date in expiry_dates['monthly_expiry']:
    print(date.strftime('%Y-%m-%d'))

print("\n2024年每个季度的到期日:")
for date in expiry_dates['quarterly_expiry']:
    print(date.strftime('%Y-%m-%d'))