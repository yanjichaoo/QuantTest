import pandas as pd
import ccxt
import time
import os

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, end_time, path, dic):
    """
    将某个交易所在指定日期指定交易对的K线数据，保存到指定文件夹
    :param exchange: ccxt交易所
    :param symbol: 指定交易对，例如'BTC/USDT'
    :param time_interval: K线的时间周期
    :param start_time: 指定日期，格式为'2020-03-16 00:00:00'
    :param end_time: 指定日期，格式为'2020-03-16 00:00:00'
    :param path: 文件保存根目录
    :return:
    """

    # ===开始抓取数据
    df_list = []

    end_time_since = exchange.parse8601(end_time)

    while True:
        # 获取数据
        params = {
            'instId': symbol,
            'bar': time_interval,
            'after': str(end_time_since),
            'limit': 300
        }
        data = exchange.public_get_market_candles(params=params)

        # 整理数据
        df = pd.DataFrame(data['data'], dtype=float)  # 将数据转换为dataframe
        # 合并数据
        df_list.append(df)
        # 新的since
        t = pd.to_datetime(df.iloc[-1][0], unit='ms')
        end_time_since = exchange.parse8601(str(t))
        # 判断是否挑出循环
        if t < pd.to_datetime(start_time) or df.shape[0] <= 1:
            break
        # 抓取间隔需要暂停2s，防止抓取过于频繁
        time.sleep(2)

    # ===合并整理数据
    df = pd.concat(df_list, ignore_index=True)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume', 6: 'volccy'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'volccy']]  # 整理列的顺序
    # 排序
    df.sort_values('candle_begin_time', inplace=True)
    # 选取数据时间段
    df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]

    # 去重
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)

    df.reset_index(drop=True, inplace=True)

    # ===保存数据到文件
    # 创建交易所文件夹
    path = os.path.join(path, exchange.id)
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建spot文件夹
    path = os.path.join(path, dic)
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


exchange = ccxt.okex5({
    'proxies': {
        'http': 'http://localhost:1087',
        'https': 'http://localhost:1087',
    },
})

start_time = '2022-07-03 00:00:00'
end_time = '2022-07-04 00:00:00'
path = '/Users/yanjichao/develop/quant/history_candle_data'

swap_list = ['BTC-USDT-SWAP', 'BTC-USD-SWAP']
future_list = ['BTC-USDT-220708', 'BTC-USDT-220715', 'BTC-USDT-220930', 'BTC-USDT-221230', 'BTC-USD-220708',
               'BTC-USD-220715', 'BTC-USD-220930', 'BTC-USD-221230']
bar = '5m'

for swap in swap_list:
    print(swap)
    save_spot_candle_data_from_exchange(exchange, swap, bar, start_time, end_time, path, 'swap')

for future in future_list:
    print(future)
    save_spot_candle_data_from_exchange(exchange, future, bar, start_time, end_time, path, 'future')
