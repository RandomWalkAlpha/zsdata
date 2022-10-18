import pandas as pd
pd.set_option('display.max_columns', None)
from sse.sse_md_generator import *

if __name__ == '__main__':
    order_data = pd.read_parquet('level2-md/20211213/v_sse_order.pq')
    trade_data = pd.read_parquet('level2-md/20211213/v_sse_trade.pq')
    # trade_data = pd.read_parquet('level2-md/20211213/v_sse_trade.pq')
    #print(order_data[(order_data['OrderNO'] == 27812) & (order_data['SecurityID'] == '600807')])
    #print(order_data[(order_data['OrderNO'] == 130768) & (order_data['SecurityID'] == '600807')])
    #print(order_data[(order_data['OrderType'] == 'D') & (order_data['SecurityID'] == '600807')])
    order_data = order_data[(order_data['SecurityID'] == '600807')]
    order_data = order_data.head(100)
    print(order_data)

    trade_data = trade_data[(trade_data['SecurityID'] == '600807')]
    trade_data = trade_data.head(100)
    print(trade_data)

    transaction = generate_transaction_df(trade_data, order_data)
    print(transaction)

    withdraw = generate_withdraw_df(order_data)
    print(withdraw)
