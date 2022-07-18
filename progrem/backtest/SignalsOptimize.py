import pandas as pd
import numpy as np


# =====简单布林策略
# 策略
def signal_simple_bolling(df, para=[200, 2]):
    """
    :param df:
    :param para: n, m
    :return:

    # 布林线策略
    # 布林线中轨：n天收盘价的移动平均线
    # 布林线上轨：n天收盘价的移动平均线 + m * n天收盘价的标准差
    # 布林线上轨：n天收盘价的移动平均线 - m * n天收盘价的标准差
    # 当收盘价由下向上穿过上轨的时候，做多；然后由上向下穿过中轨的时候，平仓。
    # 当收盘价由上向下穿过下轨的时候，做空；然后由下向上穿过中轨的时候，平仓。
    """

    # ===策略参数
    n = int(para[0])
    m = para[1]

    # ===计算指标
    # 计算均线
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    # 计算上轨、下轨道
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof代表标准差自由度
    df['upper'] = df['median'] + m * df['std']
    df['lower'] = df['median'] - m * df['std']

    # ===计算信号
    # 找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
    condition3 = df['close'] > df['close'].shift(2000)
    df.loc[condition1 & condition2 & condition3, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # 找出做多平仓信号
    condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
    condition3 = df['close'] < df['close'].shift(2000)
    df.loc[condition1 & condition2 & condition3, 'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

    # 找出做空平仓信号
    condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1,
                                                           skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行

    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    # ===删除无关变量
    # df.drop(['median', 'std', 'upper', 'lower', 'signal_long', 'signal_short'], axis=1, inplace=True)

    # ===删除无关变量 为了看图  暂时保留上中下
    df.drop(['std', 'signal_long', 'signal_short'], axis=1, inplace=True)

    return df


# 策略参数组合
def signal_simple_bolling_para_list(m_list=range(10, 1000, 10), n_list=[i / 10 for i in list(np.arange(5, 50, 1))]):
    """
    产生布林 策略的参数范围
    :param m_list:
    :param n_list:
    :return:
    """
    para_list = []

    for m in m_list:
        for n in n_list:
            para = [m, n]
            para_list.append(para)

    return para_list


# =====简单海龟策略
# 策略
def signal_simple_turtle(df, para=[20, 10]):
    """
    今天收盘价突破过去20天中的收盘价和开盘价中的最高价，做多。今天收盘价突破过去10天中的收盘价的最低价，平仓。
    今天收盘价突破过去20天中的收盘价和开盘价中的的最低价，做空。今天收盘价突破过去10天中的收盘价的最高价，平仓。
    :param para: [参数1, 参数2]
    :param df:
    :return:
    """
    n1 = int(para[0])
    n2 = int(para[1])

    df['open_close_high'] = df[['open', 'close']].max(axis=1)
    df['open_close_low'] = df[['open', 'close']].min(axis=1)
    # 最近n1日的最高价、最低价
    df['n1_high'] = df['open_close_high'].rolling(n1, min_periods=1).max()
    df['n1_low'] = df['open_close_low'].rolling(n1, min_periods=1).min()
    # 最近n2日的最高价、最低价
    df['n2_high'] = df['open_close_high'].rolling(n2, min_periods=1).max()
    df['n2_low'] = df['open_close_low'].rolling(n2, min_periods=1).min()

    # ===找出做多信号
    # 当天的收盘价 > n1日的最高价，做多
    condition = (df['close'] > df['n1_high'].shift(1))
    # 将买入信号当天的signal设置为1
    df.loc[condition, 'signal_long'] = 1
    # ===找出做多平仓
    # 当天的收盘价 < n2日的最低价，多单平仓
    condition = (df['close'] < df['n2_low'].shift(1))
    # 将卖出信号当天的signal设置为0
    df.loc[condition, 'signal_long'] = 0

    # ===找出做空信号
    # 当天的收盘价 < n1日的最低价，做空
    condition = (df['close'] < df['n1_low'].shift(1))
    df.loc[condition, 'signal_short'] = -1
    # ===找出做空平仓
    # 当天的收盘价 > n2日的最高价，做空平仓
    condition = (df['close'] > df['n2_high'].shift(1))
    # 将卖出信号当天的signal设置为0
    df.loc[condition, 'signal_short'] = 0

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1,
                                                           skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    # 将无关的变量删除
    df.drop(
        ['n1_high', 'n1_low', 'n2_high', 'n2_low', 'signal_long', 'signal_short', 'open_close_high', 'open_close_low'],
        axis=1, inplace=True)

    return df


# 策略参数组合
def signal_simple_turtle_para_list(n1_list=range(10, 1000, 10), n2_list=range(10, 1000, 10)):
    para_list = []

    for n1 in n1_list:
        for n2 in n2_list:
            if n2 > n1:
                continue
            para = [n1, n2]
            para_list.append(para)

    return para_list


# =====肯特纳通道策略
# 策略
def signal_simple_kc(df, para=[500, 500]):
    """
    :param df:
    :param para: n, m
    :return:

    中轨是一条移动平均线，可以选择简单移动平均线（MA）和指数移动平均线（EMA），不同点是指数移动平均线对最新的价格更敏感，这里推荐用EMA。
    上下轨是通过中线和平均真实振幅（ATR）计算出来的。
    上轨=中轨+2*ATR下轨=中轨-2*ATR
    最初中轨用到的是10SMA ,优化后改为中线EMA的周期选择20，ATR周期是10。
    """

    # ===策略参数
    m = int(para[0])
    n = int(para[1])

    df['p1'] = abs(df['high'] - df['low'])
    df['p2'] = abs(df['high'] - df['close'].shift(1))
    df['p3'] = abs(df['close'].shift(1) - df['low'])

    df['median'] = df['close'].ewm(m, adjust=False).mean()

    df['tr'] = df[['p1', 'p2', 'p3']].max(axis=1)
    df['atr'] = df['tr'].rolling(n).mean()

    df['upper'] = df['median'] + 2 * df['atr']
    df['lower'] = df['median'] - 2 * df['atr']

    # ===计算信号
    # 找出做多信号
    condition1 = df['close'] > df['upper']  # 当前K线的收盘价 > 上轨
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)  # 之前K线的收盘价 <= 上轨
    df.loc[condition1 & condition2, 'signal_long'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # 找出做多平仓信号
    condition1 = df['close'] < df['median']  # 当前K线的收盘价 < 中轨
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2, 'signal_long'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 找出做空信号
    condition1 = df['close'] < df['lower']  # 当前K线的收盘价 < 下轨
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)  # 之前K线的收盘价 >= 下轨
    df.loc[condition1 & condition2, 'signal_short'] = -1  # 将产生做空信号的那根K线的signal设置为-1，-1代表做空

    # 找出做空平仓信号
    condition1 = df['close'] > df['median']  # 当前K线的收盘价 > 中轨
    condition2 = df['close'].shift(1) <= df['median'].shift(1)  # 之前K线的收盘价 <= 中轨
    df.loc[condition1 & condition2, 'signal_short'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    # 合并做多做空信号，去除重复信号
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, min_count=1,
                                                           skipna=True)  # 若你的pandas版本是最新的，请使用本行代码代替上面一行
    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']
    df['op'] = df['signal']
    # ===删除无关变量
    # df.drop(['median', 'p1', 'p2', 'p3', 'tr', 'atr', 'upper', 'lower', 'signal_long', 'signal_short'], axis=1,
    #         inplace=True)
    df.drop(['p1', 'p2', 'p3', 'tr', 'atr', 'signal_long', 'signal_short'], axis=1,
            inplace=True)

    return df


# 策略参数组合
def signal_simple_kc_para_list(m_list=range(100, 5000, 50), n_list=range(100, 5000, 50)):
    """
    产生布林 策略的参数范围
    :param m_list:
    :param n_list:
    :return:
    """
    para_list = []

    for m in m_list:
        for n in n_list:
            para = [m, n]
            para_list.append(para)

    return para_list
