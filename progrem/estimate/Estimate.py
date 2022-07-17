

import pandas as pd
from Statistics import *

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

# 读取资金曲线数据
equity_curve = pd.read_pickle(
    '/Users/yanjichao/develop/python/class/xbx-coin-2020_part4/data/cls-4.1.1/equity_curve.pkl')
print(equity_curve)


# 计算每笔交易
# trade = transfer_equity_curve_to_trade(equity_curve)
# print(trade)

# 计算各类统计指标
# r, monthly_return = strategy_evaluate(equity_curve, trade)

# print(r)
# print(monthly_return)
