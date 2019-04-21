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

def stat_index_all(tmp_datetime):
    datetime_str = (tmp_datetime).strftime("%Y-%m-%d")
    datetime_int = (tmp_datetime).strftime("%Y%m%d")
    print("datetime_str:", datetime_str)
    print("datetime_int:", datetime_int)


    data = ts.get_index()
    # 处理重复数据，保存最新一条数据。最后一步处理，否则concat有问题。
    if not data is None and len(data) > 0:
        # 插入数据库。
        # del data["reason"]
        data["date"] = datetime_int  # 修改时间成为int类型。
        data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        common.insert_db(data, "ts_index_all", False, "`date`,`code`")
    else:
        print("no data .")

    print(datetime_str)

def stat_today_all(tmp_datetime):
    datetime_str = (tmp_datetime).strftime("%Y-%m-%d")
    datetime_int = (tmp_datetime).strftime("%Y%m%d")
    print("datetime_str:", datetime_str)
    print("datetime_int:", datetime_int)
    data = ts.get_today_all()
    # 处理重复数据，保存最新一条数据。最后一步处理，否则concat有问题。
    if not data is None and len(data) > 0:
        # 插入数据库。
        # del data["reason"]
        data["date"] = datetime_int  # 修改时间成为int类型。
        data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        common.insert_db(data, "ts_today_all", False, "`date`,`code`")
    else:
        print("no data .")

    time.sleep(5)  # 停止5秒

    print(datetime_str)

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

def stat_pro_basics(tmp_datetime):
    pro = ts.pro_api()
    data = pro.stock_basic(list_status='L')
    if not data is None and len(data) > 0:
        # data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        common.insert_db(data, "ts_pro_basics", False, "`ts_code`")
    else:
        print("no data . stock_basics")

def stat_fina_indicator(tmp_datetime, max_year=11):
    sql_1 = """
    SELECT `ts_code` FROM ts_pro_basics
    """
    data = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
    data = data.drop_duplicates(subset="ts_code", keep="last")
    print("######## len data ########:", len(data))
    pro = ts.pro_api()
    cur_year = int((tmp_datetime).strftime("%Y"))
    start_year = cur_year - max_year
    start_date = "%s1231" % start_year

    for ts_code in data.ts_code:
        try:
            data = pro.fina_indicator(ts_code=ts_code, start_date=start_date)
        except IOError:
            data = None
        if not data is None and len(data) > 0:
            print("\ndone", ts_code)
            data.head(n=1)
            try:
                common.insert_db(data, "ts_pro_fina_indicator", False, "`ts_code`,`end_date`")
            except sqlalchemy.exc.IntegrityError:
                pass
        else:
            print("\nno data . stock_report year", ts_code)
        time.sleep(1)


# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    # tmp_datetime = common.run_with_args(stat_index_all)
    # time.sleep(5)  # 停止5秒
    # tmp_datetime = common.run_with_args(stat_today_all)
    # time.sleep(5)  # 停止5秒
    # tmp_datetime = common.run_with_args(stat_stock_basics)
    # tmp_datetime = common.run_with_args(stat_stock_profit)
    # tmp_datetime = common.run_with_args(stat_stock_report)
    common.run_with_args(stat_fina_indicator)
