import pandas as pd


# 由交易信号产生实际持仓
def position_for_OKEx_future(df):
    """
    根据signal产生实际持仓。考虑各种不能买入卖出的情况。
    所有的交易都是发生在产生信号的K线的结束时
    :param df:
    :return:
    """

    # ===由signal计算出实际的每天持有仓位
    # 在产生signal的k线结束的时候，进行买入
    df['signal'].fillna(method='ffill', inplace=True)
    df['signal'].fillna(value=0, inplace=True)  # 将初始行数的signal补全为0
    df['pos'] = df['signal'].shift()
    df['pos'].fillna(value=0, inplace=True)  # 将初始行数的pos补全为0

    # ===对无法买卖的时候做出相关处理
    # 例如：下午4点清算，无法交易；股票、期货当天涨跌停的时候无法买入；股票的t+1交易制度等等。
    # 当前周期持仓无法变动的K线
    condition = (df['candle_begin_time'].dt.hour == 16) & (df['candle_begin_time'].dt.minute == 0)
    df.loc[condition, 'pos'] = None
    # pos为空的时，不能买卖，只能和前一周期保持一致。
    df['pos'].fillna(method='ffill', inplace=True)

    # 在实际操作中，不一定会直接跳过4点这个周期，而是会停止N分钟下单。此时可以注释掉上面的代码。
    # =====对每次交易进行分组
    # =====找出开仓、平仓的k线
    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(1)  # 当前周期和上个周期持仓方向不一样。
    open_pos_condition = condition1 & condition2

    condition1 = df['pos'] != 0  # 当前周期不为空仓
    condition2 = df['pos'] != df['pos'].shift(-1)  # 当前周期和下个周期持仓方向不一样。
    close_pos_condition = condition1 & condition2

    df.loc[open_pos_condition, 'start_time'] = df['candle_begin_time']
    df['start_time'].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, 'start_time'] = pd.NaT

    df.loc[df['pos'] != df['pos'].shift(1), 'diff_time'] = 1
    df['diff_time'].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, 'diff_time'] = pd.NaT
    df['持仓天数'] = df.groupby('start_time')['signal'].cumsum()
    df['持仓天数'].fillna(0, inplace=True)
    # 持仓天数最后一天少1

    df['rolling_num'] = pd.NaT
    num = 540
    min_num = 100

    df['new_pos'] = df['pos']
    flag = False
    current_start_time = 0
    for index, row in df.iterrows():
        if row['持仓天数'] != 0:
            df.loc[index, 'rolling_num'] = max(min_num, num - abs(row['持仓天数']))
            rolling_num = max(min_num, num - abs(row['持仓天数']))
            price_num = 0
            # for i in range(min(index, int(rolling_num))):
            #     price_num += (df.loc[index - i]['close'])
            # df.loc[index, 'new_median'] = price_num / min(index, rolling_num)
            df_temp = df.loc[(index - rolling_num): (index + 1)]
            df.loc[index, 'new_median'] = df_temp['close'].mean()
            # 如果上一次持有的开仓时间跟本次的开仓时间一致  说明还在一个周期内  做收盘价和动态均线判断
            if index == 2937:
                print(1)
            if current_start_time == row['start_time']:
                # 如果本次开仓已经提前结束  仓位置为空 本地判定结束
                if flag:
                    df.loc[index, 'new_pos'] = 0
                else:
                    # 做多时 提前平仓
                    if (row['close'] < df_temp['close'].mean()) & (row['pos'] == 1):
                        df.loc[index, 'new_pos'] = 0
                        # 本次开仓结束
                        flag = True
                    # 做空时 提前平仓
                    elif (row['close'] > df_temp['close'].mean()) and (row['pos'] == -1):
                        df.loc[index, 'new_pos'] = 0
                        # 本次开仓结束
                        flag = True
                    else:
                        flag = False
            else:
                # 如果上次开仓时间跟本次不一样 说明是新开仓 标志位重置
                flag = False
        current_start_time = row['start_time']

    df['pos'] = df['new_pos']
    # ===将数据存入hdf文件中
    # 删除无关中间变量
    df.drop(['signal'], axis=1, inplace=True)

    return df
