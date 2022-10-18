import pandas as pd
import time
import pytz
import datetime
from pandas import DataFrame


def generate_transaction_df(sse_trade_df: DataFrame, sse_order_df: DataFrame) -> DataFrame:
    transaction_df = DataFrame(
        data=None,
        columns=[
            'ID', 'TradeIndex', 'TradePrice', 'TradeQty', 'TradeBuyNo',
            'OrderBuyTime', 'OrderBuyPrice', 'OrderBuyQty', 'OrderBuyRemainQty',
            'OrderSellNo', 'OrderSellTime', 'OrderSellPrice', 'OrderSellQty', 'OrderSellRemainQty',
        ]
    )

    for _, trade_row in sse_trade_df.iterrows():
        # timestamp = int(trade_row['LocalTime'] / 1000000)
        # print(timestamp, type(timestamp))
        # d = datetime.datetime.fromtimestamp(timestamp, pytz.timezone('Asia/Shanghai'))
        # print(d.strftime("%Y-%m-%d %H:%M:%S.%f"))

        security_id = trade_row['SecurityID']
        trade_buy_no, trade_sell_no = trade_row['TradeBuyNo'], trade_row['TradeSellNo']
        buy_record = sse_order_df[
            (sse_order_df['OrderNO'] == trade_buy_no) & (sse_order_df['SecurityID'] == security_id)
            ].iloc[-1]

        sell_record = sse_order_df[
            (sse_order_df['OrderNO'] == trade_sell_no) & (sse_order_df['SecurityID'] == security_id)
            ].iloc[-1]

        transaction_data = {
            'TradeIndex': trade_row['TradeIndex'],
            'TradeTime': trade_row['LocalTime'],
            'TradePrice': trade_row['TradePrice'],
            'TradeQty': trade_row['TradeQty'],
            'OrderBuyNo': buy_record['OrderNO'],
            'OrderBuyTime': buy_record['LocalTime'],
            'OrderBuyPrice': buy_record['OrderPrice'],
            'OrderBuyQty': buy_record['Balance'],
            'OrderBuyRemainQty': buy_record['Balance'] - trade_row['TradeQty'],
            'OrderSellNo': sell_record['OrderNO'],
            'OrderSellTime': sell_record['LocalTime'],
            'OrderSellPrice': sell_record['OrderPrice'],
            'OrderSellQty': sell_record['Balance'],
            'OrderSellRemainQty': sell_record['Balance'] - trade_row['TradeQty'],
        }
        transaction_df = transaction_df.append(transaction_data, ignore_index=True)

    return transaction_df


def generate_withdraw_df(sse_order_df: DataFrame) -> DataFrame:
    withdraw_no = 1
    withdraw_df = DataFrame(
        data=None,
        columns=[
            'No', 'CancelTime', 'Qty', 'OrderBSFlag', 'OrderNo', 'OrderTime', 'OrderPrice', 'OrderQty', 'RemainQty'
        ]
    )
    for _, order_row in sse_order_df.iterrows():
        if order_row['OrderType'] != 'D':
            continue
        withdraw_data = {
            # Should these following 3 fields be in trade table ?
            # https://www.sseinfo.com/services/assortment/document/interface/c/5645523.pdf
            # Page 9
            'No': withdraw_no,
            'CancelTime': order_row['LocalTime'],
            'Qty': order_row['Balance'],

            'OrderBSFlag': order_row['OrderBSFlag'],
            'OrderNo': order_row['OrderNO'],
            'OrderTime': order_row['LocalTime'],
            'OrderPrice': order_row['OrderPrice'],
            'OrderQty': order_row['Balance'],
            'RemainQty': None,
        }
        withdraw_df = withdraw_df.append(withdraw_data, ignore_index=True)
        withdraw_no += 1

    return withdraw_df
