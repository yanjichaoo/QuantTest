import pandas as pd
from datetime import timedelta
from SignalsOptimize import *
from PositionOptimize import *
from Evaluate import *
import matplotlib.pyplot as plt
import numpy as np

from progrem.estimate.Statistics import *

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数

# =====手工设定策略参数
symbol = 'ETH-USDT_5m'
# para = [750, 3.0]
para = [400, 2.0]

face_value = 0.01  # btc是0.01，不同的币种要进行不同的替换
c_rate = 5 / 10000  # 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
slippage = 1 / 1000  # 滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
leverage_rate = 1
min_margin_ratio = 1 / 100  # 最低保证金率，低于就会爆仓
rule_type = '15T'
drop_days = 10  # 币种刚刚上线10天内不交易

# =====读入数据
df = pd.read_hdf('/Users/yanjichao/develop/quant/history_candle_data/%s.h5' % symbol, key='df')
# 任何原始数据读入都进行一下排序、去重，以防万一
df.sort_values(by=['candle_begin_time'], inplace=True)
df.drop_duplicates(subset=['candle_begin_time'], inplace=True)
df.reset_index(inplace=True, drop=True)

# =====转换为其他分钟数据
rule_type = '15T'
period_df = df.resample(rule=rule_type, on='candle_begin_time', label='left', closed='left').agg(
    {'open': 'first',
     'high': 'max',
     'low': 'min',
     'close': 'last',
     'volume': 'sum',
     })
period_df.dropna(subset=['open'], inplace=True)  # 去除一天都没有交易的周期
period_df = period_df[period_df['volume'] > 0]  # 去除成交量为0的交易周期
period_df.reset_index(inplace=True)
df = period_df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
df = df[df['candle_begin_time'] >= pd.to_datetime('2017-08-17')]
# df = df[df['candle_begin_time'] <= pd.to_datetime('2017-10-11')]
# df = df[df['candle_begin_time'] >= pd.to_datetime('2021-11-11')]
df = df[df['candle_begin_time'] <= pd.to_datetime('2022-07-05')]
# df = df[df['candle_begin_time'] <= pd.to_datetime('2017-12-10')]

df.reset_index(inplace=True, drop=True)

# =====计算交易信号
df = signal_simple_bolling(df, para)

# =====计算实际持仓
df = position_for_OKEx_future(df)

# =====计算资金曲线
# 选取相关时间。币种上线10天之后的日期
t = df.iloc[0]['candle_begin_time'] + timedelta(days=drop_days)
df = df[df['candle_begin_time'] > t]
# 计算资金曲线
df = equity_curve_for_OKEx_USDT_future_next_open(df, slippage=slippage, c_rate=c_rate, leverage_rate=leverage_rate,
                                                 face_value=face_value, min_margin_ratio=min_margin_ratio)
# print(df)
print('策略最终收益：', df.iloc[-1]['equity_curve'])

# 计算每笔交易
trade = transfer_equity_curve_to_trade(df)
# print(trade)

# 计算各类统计指标
result, monthly_return = strategy_evaluate(df, trade)

print(result)
# print(monthly_return)
plt.plot(df['median'], "g")
plt.plot(df['upper'], "r")
plt.plot(df['lower'], "b")
plt.plot(df['close'], 'c')
plt.plot(df['new_median'], 'k')

df['buy'].fillna(value=0, inplace=True)
df['sell'].fillna(value=0, inplace=True)
plt.plot(df['buy'], 'm')
plt.plot(df['sell'], 'm')
# plt.show()
