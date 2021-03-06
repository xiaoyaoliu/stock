#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import sys
sys.path.append("/data/stock/")

import libs.common as common
import sys
import time
import pandas as pd
import tushare as ts
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import sqlalchemy
import datetime

"""
交易数据

http://tushare.org/trading.html#id2

股市交易时间为每周一到周五上午时段9:30-11:30，下午时段13:00-15:00。 周六、周日上海证券交易所、深圳证券交易所公告的休市日不交易。

"""

def stat_stock_basics(tmp_datetime):
    data = ts.get_stock_basics()
    if not data is None and len(data) > 0:
        # data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        common.insert_db(data, "ts_stock_basics", False, "`code`,`name`")
    else:
        print("no data . stock_basics")

def stat_stock_profit(tmp_datetime, max_year=18):
    """
    以后每年7月份取一下上年的年报即可，历史数据不必再取
    经验: 19年4月份取18年的年报是不全的，所以延到7月取
    """
    # cur_year = int((tmp_datetime).strftime("%Y"))
    cur_year = 2005
    # i = cur_year - max_year
    i = 2001
    MAX_RETRY_TIME = 3
    retry_time = 0
    while i < cur_year:
        try:
            data = ts.get_profit_data(i, 4)
        except IOError:
            data = None
        if not data is None and len(data) > 0:
            print("\nyear done", i)
            # data = data.drop_duplicates(subset="code", keep="last")
            data.insert(0, "year", [i] * len(data))
            data.head(n=1)
            common.insert_db(data, "ts_stock_profit", False, "`year`,`code`")
            i += 1
            retry_time = 0
        else:
            print("\nno data . stock_profit year", i)
            retry_time += 1
            if retry_time > MAX_RETRY_TIME:
                i += 1
                retry_time = 0

        time.sleep(5)  # 停止5秒

def stat_stock_report(tmp_datetime, max_year=11):
    """
    以后每年7月份取一下上年的年报即可，历史数据不必再取
    经验: 19年4月份取18年的年报是不全的，所以延到7月取
    """
    cur_year = int((tmp_datetime).strftime("%Y"))
    # cur_year = 2005
    i = cur_year - max_year
    # i = 2001
    MAX_RETRY_TIME = 3
    retry_time = 0
    while i < cur_year:
        try:
            data = ts.get_report_data(i, 4)
        except IOError:
            data = None
        if not data is None and len(data) > 0:
            print("\nyear done", i)
            # data = data.drop_duplicates(subset="code", keep="last")
            data.insert(0, "year", [i] * len(data))
            data.head(n=1)
            common.insert_db(data, "ts_stock_report", False, "`year`,`code`")
            i += 1
            retry_time = 0
        else:
            print("\nno data . stock_report year", i)
            retry_time += 1
            if retry_time > MAX_RETRY_TIME:
                i += 1
                retry_time = 0

        time.sleep(5)  # 停止5秒


# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    # tmp_datetime = common.run_with_args(stat_stock_basics)
    # tmp_datetime = common.run_with_args(stat_stock_profit)
    # tmp_datetime = common.run_with_args(stat_stock_report)
