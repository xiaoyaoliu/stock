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
import sqlalchemy
import datetime
import shutil

import logging
# logging.basicConfig(filename='',level=logging.DEBUG)
# create logger
logger = logging.getLogger('daily_job')
logger.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# add formatter to ch
ch.setFormatter(formatter)

# 文件日志
file_handler = logging.FileHandler("/data/logs/daily_job_py.log")
file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
file_handler.setLevel(logging.DEBUG)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(file_handler)



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

def get_cur_day(tmp_datetime):
    cur_day = int(tmp_datetime.strftime("%Y%m%d"))
    cur_hour = int(tmp_datetime.strftime("%H"))
    print(cur_hour)
    if cur_hour < 17:
        cur_day = int((tmp_datetime - datetime.timedelta(days=1)).strftime("%Y%m%d"))
    return cur_day


def stat_pro_basics(tmp_datetime):
    """
    Pandas：让你像写SQL一样做数据分析（一）: https://www.cnblogs.com/en-heng/p/5630849.html
    """
    pro = ts.pro_api()
    cur_day = get_cur_day(tmp_datetime)
    data = pro.daily_basic(trade_date=cur_day)
    try:
        sql_1 = """
        DELETE FROM ts_pro_daily WHERE `trade_date`='%s'
        """ % cur_day
        common.insert(sql_1)
    except sqlalchemy.exc.ProgrammingError:
        pass

    if not data is None and len(data) > 0:
        data = data.drop_duplicates(subset="ts_code", keep="last")
        data.head(n=1)
        if len(data) > 0:
            common.insert_db(data, "ts_pro_daily", False, "`ts_code`, `trade_date`")
    else:
        logger.debug("no data . stock_basics")

def daily_common(cur_day, res_table, standard, pe):
    """
        不在此列表里的建议卖出
    """

    sql_pro = """
    select *, (pb * pe) as standard from (select tb_res.ts_code, name, area, industry, market, list_date, (total_mv * 10000 / ledger_asset) as pb, (total_mv * 10000 / average_income) as pe, (average_cash_div_tax / (total_mv / total_share)) as div_ratio from {res_table} tb_res INNER JOIN
    ts_pro_daily on tb_res.ts_code = ts_pro_daily.ts_code AND trade_date='{cur_day}') ts_res WHERE (pb * pe) < {standard} AND div_ratio > 0.02 AND pe < {pe}
    ORDER BY (pb * pe) ASC, div_ratio DESC, pe ASC, pb ASC
""".format(
        res_table=res_table,
        cur_day = cur_day,
        standard=standard,
        pe=pe
    )

    data = pd.read_sql(sql=sql_pro, con=common.engine(), params=[])
    data = data.drop_duplicates(subset="ts_code", keep="last")
    logger.debug(res_table)
    return data


def daily_defensive(tmp_datetime):
    """
    6. 适度的市盈率，当期股价不应该高于过去3年平均利润的15倍
        股价比较动态, 这个指标要每周跑一次了。

    7. 适度的股价资产比
        当期股价不应该超过最后报告的资产账面值的1.5倍。根据经验法则，我们建议，市盈率与价格账面值之比的乘积不应该超过22.5.
        (例如 市盈率15, 1.5倍的价格账面值; 9倍的市盈率和2.5倍的资产价值)
        资产账面值:
        《国际评估准则》指出，企业的账面价值,
        是企业资产负债表上体现的企业全部资产(扣除折旧、损耗和摊销)与企业全部负债之间的差额，与账面资产、净值和股东权益是同义的。

        账面价值 = total_assets - total_liab

        # 检查下你关心的列是不是double，改变你关心的列的类型
        ALTER TABLE table_name MODIFY COLUMN column_name datatype;
        查看当前类型:  show columns from ts_pro_fina_indicator;
        例如: alter table ts_pro_fina_indicator modify column gross_margin REAL;
    """

    cur_day = get_cur_day(tmp_datetime)
    print(cur_day)

    # 由于defensive的ROE是15，高成长，所以买入放宽标准到40, 卖出标准为66, 市盈率25是极限。
    data_def = daily_common(cur_day, "ts_res_defensive", 66, 25)
    logger.debug(data_def)
    # 由于buffett的ROE是10年连续20，牛逼的成长，所以买入放宽标准到60, 卖出标准放宽到80。 市盈率30是极限
    data_buf = daily_common(cur_day, "ts_res_buffett", 80, 30)
    logger.debug(data_buf)


def daily_divdend(tmp_datetime):
    """
    第8章 投资者与市场波动

    从根本上讲，价格波动对真正的投资者只有一个重要含义，即它们使得投资者有机会在价格大幅下降时做出理智的购买决策，同时有机会在价格大幅上升时做出理智的抛售决策。
    在除此之外的其他时间里，投资者最好忘记股市的存在，更多地关注自己的股息回报和企业的经营结果

    这里，主要关注股息回报，要税前分红高于余额宝(2.38%)，所以标准为: 3%

    # 最近3年ROE为10以上的企业，中等成长，严格执行标准22.5
    daily_common(cur_day, "ts_res_defensive_weak", 22.5)
    """

    cur_day = get_cur_day(tmp_datetime)
    # 最近3年ROE为10以上的企业，中等成长，严格执行标准22.5
    res_table = "ts_res_defensive_weak"
    standard = 22.5
    sql_pro = """
    select * from (select tb_res.ts_code, name, area, industry, market, list_date, (total_mv * 10000 / ledger_asset) as pb, (total_mv * 10000 / average_income) as pe, (average_cash_div_tax / (total_mv / total_share)) as div_ratio from {res_table} tb_res INNER JOIN
    ts_pro_daily on tb_res.ts_code = ts_pro_daily.ts_code AND trade_date='{cur_day}') ts_res WHERE (pb * pe) < {standard} AND div_ratio > 0.03
    ORDER BY div_ratio DESC, pe ASC, pb ASC
""".format(
        res_table=res_table,
        cur_day = cur_day,
        standard=standard
    )

    data = pd.read_sql(sql=sql_pro, con=common.engine(), params=[])
    data = data.drop_duplicates(subset="ts_code", keep="last")
    logger.debug(res_table)
    logger.debug(data)


# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    # tmp_datetime = common.run_with_args(stat_all)
    tmp_datetime = common.run_with_args(stat_pro_basics)
    tmp_datetime = common.run_with_args(daily_defensive)
    tmp_datetime = common.run_with_args(daily_divdend)
