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
from jinja2 import Template

import logging
# logging.basicConfig(filename='',level=logging.DEBUG)
# create logger
logger = logging.getLogger('daily_job')
logger.setLevel(logging.DEBUG)

MAILS = [
    "zhangxukim@qq.com",
]

WEEK_MAILS = [
    "707136301@qq.com",  # 曹晓龙
]

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

class ResData(object):
    pass


def get_cur_day(tmp_datetime):
    # 4 == Friday
    Friday = 4
    if tmp_datetime.weekday() > Friday:
        # Saturday And Sunday
        days = tmp_datetime.weekday() - Friday
        cur_day = int((tmp_datetime - datetime.timedelta(days=days)).strftime("%Y%m%d"))
    else:
        cur_day = int(tmp_datetime.strftime("%Y%m%d"))
        cur_hour = int(tmp_datetime.strftime("%H"))
        if cur_hour < 17:
            cur_day = int((tmp_datetime - datetime.timedelta(days=1)).strftime("%Y%m%d"))
    return cur_day
    # return 20190606


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

def daily_common(cur_day, res_table, standard, pe, div_standard, pb, sort_by_standard=True):
    """
        不在此列表里的建议卖出
    """
    if sort_by_standard:
        sort_str = "(pb * pe) ASC, "
    else:
        sort_str = ""
    sql_pro = """
    select *, (pb * pe) as standard from (select tb_res.ts_code, name, area, industry, market, list_date, GREATEST(ts_pro_daily.pb, total_mv * 10000 / div_ledger_asset) as pb, (total_mv * 10000 / average_income) as pe, (average_cash_div_tax / (total_mv / total_share)) as div_ratio from {res_table} tb_res INNER JOIN
    ts_pro_daily on tb_res.ts_code = ts_pro_daily.ts_code AND trade_date='{cur_day}') ts_res WHERE (pb * pe) < {standard} AND div_ratio > {div_standard} AND pe < {pe} and pb < {pb}
    ORDER BY {sort_custom}div_ratio DESC, pb ASC, pe ASC
""".format(
        res_table=res_table,
        cur_day = cur_day,
        standard=standard,
        pe=pe,
        div_standard=div_standard,
        pb=pb,
        sort_custom=sort_str
    )

    data = pd.read_sql(sql=sql_pro, con=common.engine(), params=[])
    data = data.drop_duplicates(subset="ts_code", keep="last")
    logger.debug(res_table)
    return data


def daily_defensive(tmp_datetime, res_data):
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

    logger.debug("不在下面列表里的，请考虑卖出")
    # 由于defensive的ROE是15，高成长，所以买入放宽标准到40, 卖出标准为60, 市盈率25是极限。
    data_def = daily_common(cur_day, "ts_res_defensive", 60, 25, 0.025, 3.5)
    logger.debug(data_def)
    res_data.defensive = data_def.to_html()
    # 由于buffett的ROE是10年连续20，牛逼的成长，所以买入放宽标准到65, 卖出标准放宽到100。 市盈率30是极限
    data_buf = daily_common(cur_day, "ts_res_buffett", 100, 30, 0.02, 6.0)
    logger.debug(data_buf)
    res_data.buffett = data_buf.to_html()


def daily_divdend(tmp_datetime, res_data):
    """
    第8章 投资者与市场波动

    从根本上讲，价格波动对真正的投资者只有一个重要含义，即它们使得投资者有机会在价格大幅下降时做出理智的购买决策，同时有机会在价格大幅上升时做出理智的抛售决策。
    在除此之外的其他时间里，投资者最好忘记股市的存在，更多地关注自己的股息回报和企业的经营结果

    这里，主要关注股息回报，要税前分红高于余额宝(2.38%)，所以标准为: 3%

    # 最近3年ROE为10以上的企业，中等成长，严格执行标准22.5
    """

    cur_day = get_cur_day(tmp_datetime)
    # 最近3年ROE为10以上的企业，中等成长，严格执行标准22.5。
    # ts_res_defensive_weak中的企业净资产排除了部分非流动资产，市净率偏高

    data = daily_common(cur_day, "ts_res_defensive_weak", 22.5, 12, 0.038, 2.5, False)
    logger.debug(data)
    res_data.dividend = data.to_html()


def daily_positive(tmp_datetime, res_data):
    """
    破净股
    """
    cur_day = get_cur_day(tmp_datetime)
    # 最近3年ROE为5 以上的企业，低成长，主要寻找低市净率的企业
    data = daily_common(cur_day, "ts_res_positive", 15, 12, 0.03, 1.1, False)
    logger.debug(data)
    res_data.positive = data.to_html()


def save_then_mail(tmp_datetime, res_data):
    html_template = Template("""
<h3>防御型建议</h3>
<p>买入: standard &lt;&nbsp; <strong>40</strong></p>
<p>卖出: 不在下表中的股票</p>
{{ defensive }}
<p>&nbsp;</p>
<h3>ROE20建议</h3>
<p>买入: standard &lt;&nbsp; <strong>65</strong></p>
<p>卖出: 不在下表中的</p>
{{ buffett }}
<p>&nbsp;</p>
<h3>高分红 中成长建议</h3>
<p>买入: 感兴趣的</p>
<p>卖出: 不在下表中的</p>
{{dividend }}
<p>&nbsp;</p>
<h3>破净股建议，适合老手</h3>
<p>买入: 感兴趣的</p>
<p>卖出: 不在下表中的</p>
{{positive }}
    """)
    res = html_template.render(
        defensive=res_data.defensive,
        buffett=res_data.buffett,
        dividend=res_data.dividend,
        positive=res_data.positive
    )
    datetime_str = (tmp_datetime).strftime("%Y%m%d")
    filename = "/data/logs/mail_%s.html" % datetime_str
    with open(filename, 'w') as fout:
        fout.write(res)
    title = "A投资建议 %s" % datetime_str
    notify_mails = MAILS

    Wednesday = 2
    if tmp_datetime.weekday() == Wednesday:
        notify_mails += WEEK_MAILS

    mail_cmd = 'mail -a "Content-type: text/html" -s "{title}" {mails} < {filename}'.format(
        title=title,
        filename=filename,
        mails = ' '.join(notify_mails)
    )
    import subprocess
    subprocess.call(mail_cmd, shell=True)

# main函数入口
if __name__ == '__main__':
    # 使用方法传递。

    tmp_datetime = common.run_with_args(stat_pro_basics)
    res_data = ResData()
    tmp_datetime = common.run_with_args(daily_defensive, res_data)
    tmp_datetime = common.run_with_args(daily_divdend, res_data)
    tmp_datetime = common.run_with_args(daily_positive, res_data)
    tmp_datetime = common.run_with_args(save_then_mail, res_data)


