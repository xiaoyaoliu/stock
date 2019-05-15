#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import sys
sys.path.append("/data/stock/")

import libs.common as common
import os
import time
import pandas as pd
import tushare as ts
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import datetime
import shutil


####### 使用 5.pdf，先做 基本面数据 的数据，然后在做交易数据。
#
def stat_all(tmp_datetime):
    datetime_str = (tmp_datetime).strftime("%Y-%m-%d")
    datetime_int = (tmp_datetime).strftime("%Y%m%d")

    cache_dir = common.bash_stock_tmp % (datetime_str[0:7], datetime_str)
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
        print("remove cache dir force :", cache_dir)

    print("datetime_str:", datetime_str)
    print("datetime_int:", datetime_int)
    data = ts.top_list(datetime_str)
    # 处理重复数据，保存最新一条数据。最后一步处理，否则concat有问题。
    #
    if not data is None and len(data) > 0:
        # 插入数据库。
        # del data["reason"]
        data["date"] = datetime_int  # 修改时间成为int类型。
        data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        common.insert_db(data, "ts_top_list", False, "`date`,`code`")
    else:
        print("no data .")

    print(datetime_str)


def stat_pro_basics(tmp_datetime):
    """
    Pandas：让你像写SQL一样做数据分析（一）: https://www.cnblogs.com/en-heng/p/5630849.html
    """
    pro = ts.pro_api()
    cur_day = tmp_datetime.strftime("%Y%M%D")
    data = pro.daily_basic(trade_date=cur_day)
    try:
        sql_1 = """
        SELECT `ts_code` FROM ts_pro_daily WHERE `trade_date`='%s'
        """ % cur_day
        exist_data = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
        exist_data = exist_data.drop_duplicates(subset="ts_code", keep="last")
        exist_set = set(exist_data.ts_code)
    except sqlalchemy.exc.ProgrammingError:
        exist_set = set()

    if not data is None and len(data) > 0:
        data = data.drop_duplicates(subset="ts_code", keep="last")
        data.head(n=1)
        # data = data[-data['ts_code'].isin(exist_set)]
        if len(data) > 0:
            common.insert_db(data, "ts_pro_daily", False, "`ts_code`, `trade_date`")
    else:
        logger.debug("no data . stock_basics")


# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    # tmp_datetime = common.run_with_args(stat_all)
    tmp_datetime = common.run_with_args(stat_pro_basics)
