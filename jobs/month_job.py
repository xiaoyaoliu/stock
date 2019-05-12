#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import sys
sys.path.append("/data/stock/")

import logging
# logging.basicConfig(filename='',level=logging.DEBUG)
# create logger
logger = logging.getLogger('month_job')
logger.setLevel(logging.DEBUG)


# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# add formatter to ch
ch.setFormatter(formatter)

# 文件日志
file_handler = logging.FileHandler("/data/logs/month_job_py.log")
file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
file_handler.setLevel(logging.DEBUG)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(file_handler)

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
    """
    Pandas：让你像写SQL一样做数据分析（一）: https://www.cnblogs.com/en-heng/p/5630849.html
    """
    pro = ts.pro_api()
    data = pro.stock_basic(list_status='L')
    sql_1 = """
    SELECT `ts_code` FROM ts_pro_basics
    """
    exist_data = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
    exist_data = exist_data.drop_duplicates(subset="ts_code", keep="last")
    exist_set = set(exist_data.ts_code)

    if not data is None and len(data) > 0:
        data = data.drop_duplicates(subset="ts_code", keep="last")
        data.head(n=1)
        data = data[-data['ts_code'].isin(exist_set)]
        if len(data) > 0:
            common.insert_db(data, "ts_pro_basics", False, "`ts_code`")
    else:
        logger.debug("no data . stock_basics")

def stat_fina(tmp_datetime, method, max_year=11):
    sql_1 = """
    SELECT `ts_code` FROM ts_pro_basics
    """
    data = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
    data = data.drop_duplicates(subset="ts_code", keep="last")
    logger.debug("######## len data ########: %s", len(data))
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
            logger.info("\ndone", ts_code)
            data.head(n=1)
            data = data.drop_duplicates(subset=["ts_code", 'end_date'], keep="last")
            try:
                common.insert_db(data, "ts_pro_%s" % method, False, "`ts_code`,`end_date`")
            except sqlalchemy.exc.IntegrityError:
                pass
        else:
            logger.debug("\nno data . method=%s ts_code=%s", method, ts_code)
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

def stat_current_fina(tmp_datetime, method):
    sql_1 = """
    SELECT `ts_code` FROM ts_pro_basics
    """
    basic_data = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
    basic_data = basic_data.drop_duplicates(subset="ts_code", keep="last")
    pro = ts.pro_api()
    # 每年都取上一年的财报
    cur_year = int((tmp_datetime).strftime("%Y")) - 1
    cur_date = "%s1231" % cur_year
    table_name = "ts_pro_%s" % method
    sql_exist = """
    SELECT `ts_code` FROM %s WHERE `end_date`='%s'
    """ % (table_name, cur_date)
    exist_data = pd.read_sql(sql=sql_exist, con=common.engine(), params=[])
    logger.info("[%s][mysql][%s]Begin: 已获取%s财报的公司共有%s家", tmp_datetime, table_name, cur_date, len(exist_data.ts_code))

    exist_set = set(exist_data.ts_code)

    new_code = []

    for i, ts_code in enumerate(basic_data.ts_code):
        if ts_code in exist_set:
            continue
        try:
            data = getattr(pro, method)(ts_code=ts_code, start_date=cur_date)
        except IOError:
            data = None
        if not data is None and len(data) > 0:
            logger.info("Table %s: insert %s, %s(%s) / %s", table_name, ts_code, i, len(exist_data) + len(new_code), len(basic_data))
            data.head(n=1)
            data = data.drop_duplicates(subset=["ts_code", 'end_date'], keep="last")
            sql_date = """
                SELECT `end_date` FROM %s WHERE `ts_code`='%s'
                """ % (table_name, ts_code)
            exist_dates = pd.read_sql(sql=sql_date, con=common.engine(), params=[])
            date_set = set(exist_dates.end_date)
            data = data[-data['end_date'].isin(date_set)]
            if len(data) > 0:
                try:
                    common.insert_db(data, table_name, False, "`ts_code`,`end_date`")
                    new_code.append(ts_code)
                except sqlalchemy.exc.IntegrityError:
                    pass
        else:
            logger.debug("no data . method=%s ts_code=%s", method, ts_code)
        # Exception: 抱歉，您每分钟最多访问该接口80次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
        time.sleep(1)

    logger.info("[%s][mysql][%s]End: 新发布%s财报的公司共有%s家", tmp_datetime, table_name, cur_date, len(new_code))

def stat_fina_indicator_current(tmp_datetime):
    stat_current_fina(tmp_datetime, "fina_indicator")


def stat_income_current(tmp_datetime):
    stat_current_fina(tmp_datetime, "income")


def stat_balancesheet_current(tmp_datetime):
    stat_current_fina(tmp_datetime, "balancesheet")



def stat_dividend_current(tmp_datetime, method="dividend"):
    sql_1 = """
    SELECT `ts_code` FROM ts_pro_basics
    """
    basic_data = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
    basic_data = basic_data.drop_duplicates(subset="ts_code", keep="last")
    pro = ts.pro_api()
    # 每年都取上一年的财报
    cur_year = int((tmp_datetime).strftime("%Y")) - 1
    cur_date = "%s1231" % cur_year
    table_name = "ts_pro_%s" % method
    sql_exist = """
    SELECT `ts_code` FROM %s WHERE `end_date`='%s' AND `div_proc`='实施'
    """ % (table_name, cur_date)
    exist_data = pd.read_sql(sql=sql_exist, con=common.engine(), params=[])
    logger.info("[%s][mysql][%s]Begin: 已获取%s财报的公司共有%s家", tmp_datetime, table_name, cur_date, len(exist_data.ts_code))

    exist_set = set(exist_data.ts_code)

    new_code = []

    for i, ts_code in enumerate(basic_data.ts_code):
        if ts_code in exist_set:
            continue
        try:
            data = getattr(pro, method)(ts_code=ts_code, start_date=cur_date)
        except IOError:
            data = None
        if not data is None and len(data) > 0:
            clear_sql = """
            DELETE * from %s WHERE `div_proc`='实施' and `tc_code`=%s
            """ % (table_name, ts_code)
            common.insert(clear_sql)
            logger.info("Table %s: insert %s, %s(%s) / %s", table_name, ts_code, i, len(exist_data) + len(new_code), len(basic_data))
            data.head(n=1)
            data = data.drop_duplicates(subset=["ts_code", 'end_date'], keep="last")
            sql_date = """
                SELECT `end_date` FROM %s WHERE `ts_code`='%s'
                """ % (table_name, ts_code)
            exist_dates = pd.read_sql(sql=sql_date, con=common.engine(), params=[])
            date_set = set(exist_dates.end_date)
            data = data[-data['end_date'].isin(date_set)]
            if len(data) > 0:
                try:
                    common.insert_db(data, table_name, False, "`ts_code`,`end_date`")
                    new_code.append(ts_code)
                except sqlalchemy.exc.IntegrityError:
                    pass
        else:
            logger.debug("no data . method=%s ts_code=%s", method, ts_code)
        # Exception: 抱歉，您每分钟最多访问该接口80次，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。
        time.sleep(1)

    logger.info("[%s][mysql][%s]End: 新发布%s财报的公司共有%s家", tmp_datetime, table_name, cur_date, len(new_code))


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
    TODO 分红数据需要检查div_proc字段，为“实施”的时候才不更新，否则需要更新。div字段还有: "预案"、停止实施、未通过，股东大会通过，股东提议，预披露
    """
    common.run_with_args(stat_dividend_current)
    common.run_with_args(stat_pro_basics)
    common.run_with_args(stat_fina_indicator_current)
    common.run_with_args(stat_balancesheet_current)
    common.run_with_args(stat_income_current)



# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    logger.info('begin')
    update_current_year()
