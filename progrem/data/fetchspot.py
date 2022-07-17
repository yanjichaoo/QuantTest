import pandas as pd
import ccxt
import time
import os
import datetime

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, end_time, path):

    # ===开始抓取数据
    df_list = []
    start_time_since = int(time.mktime(start_time.timetuple()) * 1000)
    while True:
        # 获取数据

        df = exchange.fetch_ohlcv(symbol=symbol, timeframe=time_interval, since=start_time_since, limit=None)
        # 整理数据
        df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
        # 合并数据
        df_list.append(df)
        # 新的since
        t = pd.to_datetime(df.iloc[-1][0], unit='ms')
        start_time_since = exchange.parse8601(str(t))
        # 判断是否挑出循环
        if t >= end_time or df.shape[0] <= 1:
            break
        # 抓取间隔需要暂停2s，防止抓取过于频繁
        time.sleep(2)

    # ===合并整理数据
    df = pd.concat(df_list, ignore_index=True)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]  # 整理列的顺序
    # 选取数据时间段
    df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]

    # 去重、排序
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ===保存数据到文件
    # 创建交易所文件夹
    path = os.path.join(path, exchange.id)
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建spot文件夹
    path = os.path.join(path, 'spot')
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建日期文件夹
    path = os.path.join(path, str(pd.to_datetime(start_time).date()))
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 拼接文件目录
    file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
    path = os.path.join(path, file_name)
    # 保存数据
    df.to_csv(path, index=False)


start_time = '2021-06-01 00:00:00'
start_time_date = pd.to_datetime(start_time)
end_time_date = datetime.datetime.now()

path = '/Users/yanjichao/develop/quant/history_candle_data'
error_list = []

exchange = ccxt.binance({
    'proxies': {
        'http': 'http://localhost:1087',
        'https': 'http://localhost:1087',
    },
})

target_list = ['BTC/USDT', 'ETH/USDT', 'EOS/USDT', 'LTC/USDT']

while start_time_date <= end_time_date:
    # 遍历交易对
    for symbol in target_list:
        # 遍历时间周期
        for time_interval in ['5m', '15m']:
            print(exchange.id, symbol, time_interval, start_time_date)
            # 抓取数据并且保存
            try:
                save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time_date,
                                                    start_time_date + datetime.timedelta(days=1), path)
            except Exception as e:
                print(e)
                error_list.append('_'.join([exchange.id, symbol, time_interval]))
    start_time_date += datetime.timedelta(days=1)

print(error_list)
