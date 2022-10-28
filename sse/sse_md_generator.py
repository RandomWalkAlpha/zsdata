import pandas as pd
from pandas import DataFrame
from pandas.core.indexing import _iLocIndexer

PIVOT_TIMESTAMP = 20210514


# 20210514 -> 1620921600000000000


def generate_transaction_df(sse_trade_pq_path: str, sse_order_pq_path: str = None) -> DataFrame:
    # path: /nfs-data/service/level2-md/stock-hft/20211213/v_sse_order.pq
    sep = sse_trade_pq_path.rfind('/')
    timestamp = eval(sse_trade_pq_path[sep - 8: sep])
    sse_trade_df = pd.read_parquet(sse_trade_pq_path)
    trade_qty_remain = {}

    transaction_df = DataFrame(
        data=None,
        columns=[
            'TradeIndex', 'TradeSecurityID', 'TradeTime', 'TradePrice', 'TradeQty',
            'OrderBuyNo', 'OrderBuyTime', 'OrderBuyPrice', 'OrderBuyQty', 'OrderBuyRemainQty',
            'OrderSellNo', 'OrderSellTime', 'OrderSellPrice', 'OrderSellQty', 'OrderSellRemainQty',
        ]
    )

    if timestamp >= PIVOT_TIMESTAMP:
        sse_order_df = pd.read_parquet(sse_order_pq_path)

        for _, trade_row in sse_trade_df.iterrows():
            security_id = trade_row['SecurityID']
            trade_buy_no, trade_sell_no = trade_row['TradeBuyNo'], trade_row['TradeSellNo']
            buy_record, sell_record = None, None
            buy_qty_remain, sell_qty_remain = 0, 0
            buy_rec_not_find, sell_rec_not_find = 0, 0
            try:
                buy_record = sse_order_df[
                    (sse_order_df['OrderNO'] == trade_buy_no) & (sse_order_df['SecurityID'] == security_id)
                    ].iloc[-1]
                if (security_id, buy_record['OrderNO']) in trade_qty_remain:
                    buy_qty_remain = abs(trade_qty_remain[(security_id, buy_record['OrderNO'])] - trade_row['TradeQty'])
                else:
                    buy_qty_remain = abs(buy_record['Balance'] - trade_row['TradeQty'])
                trade_qty_remain[(security_id, buy_record['OrderNO'])] = buy_qty_remain
            except:
                buy_rec_not_find = 1

            try:
                sell_record = sse_order_df[
                    (sse_order_df['OrderNO'] == trade_sell_no) & (sse_order_df['SecurityID'] == security_id)
                    ].iloc[-1]
                if (security_id, sell_record['OrderNO']) in trade_qty_remain:
                    sell_qty_remain = abs(
                        trade_qty_remain[(security_id, sell_record['OrderNO'])] - trade_row['TradeQty'])
                else:
                    sell_qty_remain = abs(sell_record['Balance'] - trade_row['TradeQty'])
                trade_qty_remain[(security_id, sell_record['OrderNO'])] = sell_qty_remain
            except:
                sell_rec_not_find = 1

            if buy_rec_not_find == sell_rec_not_find == 0:
                transaction_data = {
                    'TradeIndex': trade_row['TradeIndex'],
                    'TradeSecurityID': trade_row['SecurityID'],
                    'TradeTime': trade_row['LocalTime'],
                    'TradePrice': trade_row['TradePrice'],
                    'TradeQty': trade_row['TradeQty'],
                    'OrderBuyNo': buy_record['OrderNO'],
                    'OrderBuyTime': buy_record['LocalTime'],
                    'OrderBuyPrice': buy_record['OrderPrice'],
                    'OrderBuyQty': buy_record['Balance'],
                    'OrderBuyRemainQty': buy_qty_remain,
                    'OrderSellNo': sell_record['OrderNO'],
                    'OrderSellTime': sell_record['LocalTime'],
                    'OrderSellPrice': sell_record['OrderPrice'],
                    'OrderSellQty': sell_record['Balance'],
                    'OrderSellRemainQty': sell_qty_remain,
                }

            elif buy_rec_not_find == 1:
                transaction_data = {
                    'TradeIndex': trade_row['TradeIndex'],
                    'TradeSecurityID': trade_row['SecurityID'],
                    'TradeTime': trade_row['LocalTime'],
                    'TradePrice': trade_row['TradePrice'],
                    'TradeQty': trade_row['TradeQty'],
                    'OrderBuyNo': trade_row['TradeBuyNo'],
                    'OrderBuyTime': None,
                    'OrderBuyPrice': trade_row['TradePrice'],
                    'OrderBuyQty': trade_row['TradeQty'],
                    'OrderBuyRemainQty': 0,
                    'OrderSellNo': sell_record['OrderNO'],
                    'OrderSellTime': sell_record['LocalTime'],
                    'OrderSellPrice': sell_record['OrderPrice'],
                    'OrderSellQty': sell_record['Balance'],
                    'OrderSellRemainQty': sell_qty_remain,
                }

            elif sell_rec_not_find == 1:
                transaction_data = {
                    'TradeIndex': trade_row['TradeIndex'],
                    'TradeSecurityID': trade_row['SecurityID'],
                    'TradeTime': trade_row['LocalTime'],
                    'TradePrice': trade_row['TradePrice'],
                    'TradeQty': trade_row['TradeQty'],
                    'OrderBuyNo': buy_record['OrderNO'],
                    'OrderBuyTime': buy_record['LocalTime'],
                    'OrderBuyPrice': buy_record['OrderPrice'],
                    'OrderBuyQty': buy_record['Balance'],
                    'OrderBuyRemainQty': buy_qty_remain,
                    'OrderSellNo': trade_row['TradeSellNo'],
                    'OrderSellTime': None,
                    'OrderSellPrice': trade_row['TradePrice'],
                    'OrderSellQty': trade_row['TradeQty'],
                    'OrderSellRemainQty': 0,
                }
            transaction_df = transaction_df.append(transaction_data, ignore_index=True)
    else:
        for _, trade_row in sse_trade_df.iterrows():
            transaction_data = {
                'TradeIndex': trade_row['TradeIndex'],
                'TradeSecurityID': trade_row['SecurityID'],
                'TradeTime': trade_row['LocalTime'],
                'TradePrice': trade_row['TradePrice'],
                'TradeQty': trade_row['TradeQty'],
                'OrderBuyNo': trade_row['TradeBuyNo'],
                'OrderBuyTime': None,
                'OrderBuyPrice': trade_row['TradePrice'],
                'OrderBuyQty': trade_row['TradeQty'],
                'OrderBuyRemainQty': None,
                'OrderSellNo': trade_row['TradeSellNo'],
                'OrderSellTime': None,
                'OrderSellPrice': trade_row['TradePrice'],
                'OrderSellQty': trade_row['TradeQty'],
                'OrderSellRemainQty': None,
            }
            transaction_df = transaction_df.append(transaction_data, ignore_index=True)

    return transaction_df


def generate_cancel_df(sse_order_pq_path: str) -> DataFrame:
    sse_order_df = pd.read_parquet(sse_order_pq_path)
    cancel_no = 1
    cancel_df = DataFrame(
        data=None,
        columns=[
            'CancelNo', 'CancelSecurityID', 'CancelTime', 'CancelQty',
            'OrderBSFlag', 'OrderNo', 'OrderTime', 'OrderPrice', 'OrderQty', 'RemainQty'
        ]
    )

    for _, order_row in sse_order_df.iterrows():
        if order_row['OrderType'] != 'D':
            continue

        prev_order_data = sse_order_df[
            (sse_order_df['OrderNO'] == order_row['OrderNO']) & (sse_order_df['OrderType'] == 'A')
            ].iloc[-1]

        cancel_data = {
            'CancelNo': cancel_no,
            'CancelSecurityID': order_row['SecurityID'],
            'CancelTime': order_row['LocalTime'],
            'CancelQty': order_row['Balance'],
            'OrderBSFlag': order_row['OrderBSFlag'],
            'OrderNo': order_row['OrderNO'],
            'OrderTime': prev_order_data['LocalTime'],
            'OrderPrice': order_row['OrderPrice'],
            'OrderQty': prev_order_data['Balance'],
            'RemainQty': abs(order_row['Balance'] - prev_order_data['Balance']),
        }
        cancel_df = cancel_df.append(cancel_data, ignore_index=True)
        cancel_no += 1

    return cancel_df
