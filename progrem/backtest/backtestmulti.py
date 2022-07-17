
from multiprocessing import get_context

import pandas as pd
from datetime import timedelta
from multiprocessing.pool import Pool
from datetime import datetime
from Signals import *
from Position import *
from Evaluate import *

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

# =====参数设定
# 手工设定策略参数
symbol = 'ETH-USDT_5m'

face_value = 0.01  # btc是0.01，不同的币种要进行不同的替换
c_rate = 5 / 10000  # 手续费，commission fees，默认为万分之5。不同市场手续费的收取方法不同，对结果有影响。比如和股票就不一样。
slippage = 1 / 1000  # 滑点 ，可以用百分比，也可以用固定值。建议币圈用百分比，股票用固定值
leverage_rate = 2
min_margin_ratio = 1 / 100  # 最低保证金率，低于就会爆仓
rule_type = '15T'
drop_days = 10  # 币种刚刚上线10天内不交易

# =====读入数据
df = pd.read_hdf('/Users/yanjichao/develop/quant/history_candle_data//%s.h5' % symbol, key='df')
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
# start_time = '2017-08-01'
start_time = '2021-11-11'
end_time = '2022-07-05'
df = df[df['candle_begin_time'] >= pd.to_datetime(start_time)]
df = df[df['candle_begin_time'] <= pd.to_datetime(end_time)]
df.reset_index(inplace=True, drop=True)

# =====获取策略参数组合
para_list = signal_simple_bolling_para_list()


# =====单次循环
def calculate_by_one_loop(para):
    _df = df.copy()
    # 计算交易信号
    _df = signal_simple_bolling(_df, para=para)
    # 计算实际持仓
    _df = position_for_OKEx_future(_df)
    # 计算资金曲线
    # 选取相关时间。币种上线10天之后的日期
    t = _df.iloc[0]['candle_begin_time'] + timedelta(days=drop_days)
    _df = _df[_df['candle_begin_time'] > t]
    # 计算资金曲线
    _df = equity_curve_for_OKEx_USDT_future_next_open(_df, slippage=slippage, c_rate=c_rate,
                                                      leverage_rate=leverage_rate,
                                                      face_value=face_value, min_margin_ratio=min_margin_ratio)
    # 计算收益
    rtn = pd.DataFrame()
    rtn.loc[0, 'para'] = str(para)
    r = _df.iloc[-1]['equity_curve']
    rtn.loc[0, 'equity_curve'] = r
    print(para, '策略最终收益：', r)
    return rtn


# =====并行提速
p = get_context("fork").Pool(8)
df_list = p.map(calculate_by_one_loop, para_list)
p.close()
# 合并为一个大的DataFrame
para_curve_df = pd.concat(df_list, ignore_index=True)

# =====输出
para_curve_df.sort_values(by='equity_curve', ascending=False, inplace=True)
print(para_curve_df)
para_curve_df.to_csv('/Users/yanjichao/develop/quant/history_candle_data/kc-%s-%s-%s-%s.csv' % (
    symbol, start_time, end_time, leverage_rate), index=False)
