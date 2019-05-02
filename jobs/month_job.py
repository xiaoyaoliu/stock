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


def stat_pro_basics(tmp_datetime):
    pro = ts.pro_api()
    data = pro.stock_basic(list_status='L')
    if not data is None and len(data) > 0:
        # data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        common.insert_db(data, "ts_pro_basics", False, "`ts_code`")
    else:
        print("no data . stock_basics")

def stat_fina(tmp_datetime, method, max_year=11):
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
            data = getattr(pro, method)(ts_code=ts_code, start_date=start_date)
        except IOError:
            data = None
        if not data is None and len(data) > 0:
            print("\ndone", ts_code)
            data.head(n=1)
            data = data.drop_duplicates(subset=["ts_code", 'end_date'], keep="last")
            try:
                common.insert_db(data, "ts_pro_%s" % method, False, "`ts_code`,`end_date`")
            except sqlalchemy.exc.IntegrityError:
                pass
        else:
            print("\nno data . method=%s ts_code=%s" % (method, ts_code))
        # Exception: 抱歉，您每分钟最多访问该接口80次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
        time.sleep(1)

def stat_fina_indicator(tmp_datetime):
    stat_fina(tmp_datetime, "fina_indicator", 11)


def stat_income(tmp_datetime):
    stat_fina(tmp_datetime, "income", 11)


def stat_balancesheet(tmp_datetime):
    stat_fina(tmp_datetime, "balancesheet", 11)


def stat_dividend(tmp_datetime):
    # pass
    stat_fina(tmp_datetime, "dividend", 11)

def update_last_10_years():
    # tmp_datetime = common.run_with_args(stat_stock_basics)
    # tmp_datetime = common.run_with_args(stat_stock_profit)
    # tmp_datetime = common.run_with_args(stat_stock_report)
    common.run_with_args(stat_pro_basics)
    common.run_with_args(stat_fina_indicator)
    common.run_with_args(stat_balancesheet)
    common.run_with_args(stat_income)
    common.run_with_args(stat_dividend)

def update_current_year():
    """
    TODO 按需更新其他几个表
    """
    common.run_with_args(stat_pro_basics)


# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    update_current_year()
