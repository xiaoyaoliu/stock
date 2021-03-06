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
    try:
        sql_1 = """
        SELECT `ts_code` FROM ts_pro_basics
        """
        exist_data = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
        exist_data = exist_data.drop_duplicates(subset="ts_code", keep="last")
        exist_set = set(exist_data.ts_code)
    except sqlalchemy.exc.ProgrammingError:
        exist_set = set()

    if not data is None and len(data) > 0:
        data = data.drop_duplicates(subset="ts_code", keep="last")
        data.head(n=1)
        data = data[-data['ts_code'].isin(exist_set)]
        if len(data) > 0:
            common.insert_db(data, "ts_pro_basics", False, "`ts_code`")
    else:
        logger.debug("no data . stock_basics")

def InsertOrUpdateData(data, ts_code, table_name, i, total_num, sqlCol):
    if not data is None and len(data) > 0:
        # logger.info("Table %s: insert %s, %s(%s) / %s", table_name, ts_code, i, len(exist_data) + len(new_code), len(basic_data))
        data.head(n=1)
        data = data.drop_duplicates(subset=["ts_code", 'end_date'], keep="last")
        try:
            sql_date = """
                SELECT `end_date` FROM %s WHERE `ts_code`='%s'
                """ % (table_name, ts_code)
            exist_dates = pd.read_sql(sql=sql_date, con=common.engine(), params=[])
            date_set = set(exist_dates.end_date)
            data_to_update = data[data['end_date'].isin(date_set)]
            data = data[-data['end_date'].isin(date_set)]
        except sqlalchemy.exc.ProgrammingError:
            pass
        if len(data) > 0:
            try:
                common.insert_db(data, table_name, False, "`ts_code`,`end_date`")
            except sqlalchemy.exc.IntegrityError:
                pass
        if len(data_to_update) > 0:
                for i, row in data_to_update.iterrows():
                    common.update_sql(table_name, row, sqlCol)

        logger.info("Table %s: insert %s, %s / %s", table_name, ts_code, i, total_num)
        return ts_code
    else:
        logger.debug("no data . table=%s ts_code=%s", table_name, ts_code)

def stat_fina(tmp_datetime, method, max_year=11):
    sql_1 = """
    SELECT `ts_code` FROM ts_pro_basics
    """
    data_basic = pd.read_sql(sql=sql_1, con=common.engine(), params=[])
    data_basic = data_basic.drop_duplicates(subset="ts_code", keep="last")
    logger.debug("######## len data_basic ########: %s", len(data_basic))
    pro = ts.pro_api()
    cur_year = int((tmp_datetime).strftime("%Y"))
    start_year = cur_year - max_year
    start_date = "%s1231" % start_year
    table_name = "ts_pro_%s" % method
    sqlCol = common.get_columns(table_name)
    fields = sqlCol.columns

    for i, ts_code in enumerate(data_basic.ts_code):
        try:
            data = getattr(pro, method)(ts_code=ts_code, start_date=start_date, fields=','.join(fields))
        except IOError:
            data = None
            logger.info("\ndone %s", ts_code)
        result = InsertOrUpdateData(data, ts_code, table_name, i, len(data_basic), sqlCol)
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

    try:
        sql_exist = """
        SELECT `ts_code` FROM %s WHERE `end_date`='%s'
        """ % (table_name, cur_date)
        exist_data = pd.read_sql(sql=sql_exist, con=common.engine(), params=[])
        logger.info("[%s][mysql][%s]Begin: 已获取%s财报的公司共有%s家", tmp_datetime, table_name, cur_date, len(exist_data.ts_code))

        exist_set = set(exist_data.ts_code)
    except sqlalchemy.exc.ProgrammingError:
        exist_set = set()

    new_code = []

    sqlCol = common.get_columns(table_name)

    for i, ts_code in enumerate(basic_data.ts_code):
        if ts_code in exist_set:
            continue
        try:
            data = getattr(pro, method)(ts_code=ts_code, start_date=cur_date, fields=','.join(sqlCol.columns))
        except IOError:
            data = None

        result = InsertOrUpdateData(data, ts_code, table_name, i, len(basic_data), sqlCol)
        if result:
            new_code.append(ts_code)
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
    try:
        sql_exist = """
        SELECT `ts_code` FROM %s WHERE `end_date`='%s' AND `div_proc`='实施'
        """ % (table_name, cur_date)
        exist_data = pd.read_sql(sql=sql_exist, con=common.engine(), params=[])
        logger.info("[%s][mysql][%s]Begin: 已获取%s财报的公司共有%s家", tmp_datetime, table_name, cur_date, len(exist_data.ts_code))

        exist_set = set(exist_data.ts_code)
    except sqlalchemy.exc.ProgrammingError:
        exist_set = set()

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
            DELETE from %s WHERE `div_proc`!='实施' and `ts_code`='%s'
            """ % (table_name, ts_code)
            common.insert(clear_sql)
            logger.info("Table %s: insert %s, %s(%s) / %s", table_name, ts_code, i, len(exist_data) + len(new_code), len(basic_data))
            data.head(n=1)
            data = data.drop_duplicates(subset=["ts_code", 'end_date'], keep="last")
            try:
                sql_date = """
                    SELECT `end_date` FROM %s WHERE `ts_code`='%s'
                    """ % (table_name, ts_code)
                exist_dates = pd.read_sql(sql=sql_date, con=common.engine(), params=[])
                date_set = set(exist_dates.end_date)
                data = data[-data['end_date'].isin(date_set)]
            except sqlalchemy.exc.ProgrammingError:
                pass
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
    """
    common.run_with_args(stat_pro_basics)
    common.run_with_args(stat_fina_indicator_current)
    common.run_with_args(stat_balancesheet_current)
    common.run_with_args(stat_income_current)
    common.run_with_args(stat_dividend_current)

def gen_res_common(table_name, sql_pro, cur_year):
    data = pd.read_sql(sql=sql_pro, con=common.engine(), params=[])
    data = data.drop_duplicates(subset="ts_code", keep="last")
    data.insert(0, "year", [cur_year] * len(data))
    logger.debug(data)

    data.head(n=1)
    data = data.drop_duplicates(subset=["ts_code", 'year'], keep="last")

    old_len = 0
    try:
        sql_date = """
            SELECT `ts_code` FROM %s WHERE `year`='%s'
        """ % (table_name, cur_year)
        exist_dates = pd.read_sql(sql=sql_date, con=common.engine(), params=[])
        old_len = len(exist_dates)
        clear_sql = """
            DELETE from %s WHERE `year`='%s'
        """ % (table_name, cur_year)
        common.insert(clear_sql)
    except sqlalchemy.exc.ProgrammingError:
        pass

    if len(data) > 0:
        try:
            common.insert_db(data, table_name, False, "`ts_code`,`year`")
        except sqlalchemy.exc.IntegrityError:
            pass
    if old_len >= len(data):
        logger.debug("guess %s: no new stock is found", table_name)


def defensive_main(tmp_datetime, max_year=10):
    """
    总体思路:
    由于6, 7和当前股价有关，所以肯定是放在最后的
    2, 4依赖pro接口，所以作为第二步
    1, 3, 5使用普通接口的数据即可分析，所以作为第一个里程碑

    里程碑1: 获取符合1, 3, 4, 5的公司列表，本周完成。 今后每年运行一次
    里程碑2: 使用列表，再过滤掉不符合2的公司。 今后每年运行一次
    里程碑3: 根据当前股价，计算符合6，7的公司，今后每周三运行一次

    单元测试: 以2018年手动确认的结果为测试用例

    每周将符合条件的股票，以邮件的方式发到我的qq邮箱


    1. 适当的企业规模。工业企业年销售额不低于1亿美元(8亿人民币左右)；公用事业企业，总资产不低于5000万美元(4亿人民币左右)

    https://tushare.pro/document/2?doc_id=79
    按照官方建议，都改用pro接口吧: https://tushare.pro/document/2?doc_id=79


    1.1 总资产totalAssets(万元) 在表ts_stock_basics,
        考虑通货膨胀，这里总资产暂时设置为40亿人民币
        2019年4月10日，A股共3609家上市公司，总资产超过40亿的公司有1819家，占比50.4%
        sql方法: SELECT name from ts_stock_basics WHERE totalAssets > 400000;
        balancesheet.total_assets
        pro方法: select ts_code from ts_pro_balancesheet where end_date = "20181231" and total_assets > 4010001000 ;
    1.2 年销售额，使用business_income, 营业收入(百万元)
        考虑通货膨胀，这里暂时设置为80亿人民币，且最近3年都超过80亿人民币，每个季度的财报不低于20亿人民币

        2017年，3605家公司发布年度财报，只有659家的年营业收入超过80亿元人民币，占18%
        1年的sql方法: select name, business_income from ts_stock_profit where year=2017 AND business_income>8000;

        2015 ～2017，连续3年营收大于80亿的，只有431家（共3558家），占12%
        3年的sql方法: select code, name from ts_stock_profit where (year=2017 or year=2016 or year=2015) AND business_income>8000 group by code having count(distinct year) = 3;
        income.total_revenue >
        pro方法: select ts_code from ts_pro_income where end_date > 20160101 and end_date < 20190101 and end_date like "%1231" and total_revenue>4010001000 group by ts_code having count(distinct year(end_date)) = 3;


    2. 足够强劲的财务状况。工业企业流动资产(total_cur_assets) 应该至少是流动负债的2倍，且长期债务不应该超过流动资产净额，即"营运资本"。公用事业企业，负债不应该超过股权的两倍。
        资产负债表: https://tushare.pro/document/2?doc_id=36
        流动负债合计字段: total_cur_liab	float	流动负债合计
        负债合计字段:   total_liab	float	负债合计
        balancesheet
        截至2019年4月24日, 负债率符合要求的公司有764家
        select ts_code from ts_pro_balancesheet where end_date = "20181231" and total_cur_liab is not NULL and total_cur_assets is not NULL and (total_cur_liab <= 0 or ((total_cur_assets / total_cur_liab) > 2.0)) ;
        <上市公司财务报表解读> 扣除存货的流动资产应该大于流动负债

    3. 利润的稳定性，过去10年中，普通股每年都有一定的利润。
        每股收益 esp
        select ts_code from ts_pro_income where end_date > 20090101 and end_date < 20190101 and end_date like "%1231" and diluted_eps > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= 10;

    4. 股息记录, 至少有20年连续支付股息的记录。A股历史较短，减小到10年
        分红送股数据: https://tushare.pro/document/2?doc_id=103
        截至2019年4月24日, 2009~2018每年都分红的公司有549家, 549 / 3609 = 15.2%
        select ts_code from ts_pro_dividend where end_date > 20090101 and end_date < 20190101 and cash_div_tax > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= 10;


    5. 过去10年内，每股利润的增长至少要达到三分之一(期初与期末使用三年平均数)
        zx: 考虑到通货膨胀，10年后利润增长需要增长100%
        net_profits,净利润
        basic_eps: 每股收益应该是基本每股收益：是当期净利润除以当期在外发行的普通股的加权平均来确定，可以反应出来目前股本结构下的盈利能力。
        diluted_eps: 而摊薄每股收益是把一些潜在有可能转化成上市公司股权的股票的加权平均股数都算进来了，比如可转股债，认股权证等。因为他们在未来有可能换成股票从而摊薄上市公司每股收益。

         截至2019年4月24日, 2009~2018 diluted_eps增长超过三分之一的有504家, 504 / 3609 = 13.9%
        select t_eps1.ts_code from (select ts_code, sum(diluted_eps) as new_eps from ts_pro_income where end_date > 20160101 and end_date like "%1231" and end_date < 20190101 group by ts_code) t_eps1 INNER JOIN (select ts_code, sum(diluted_eps) as old_eps from ts_pro_income where end_date > 20090101 and end_date like "%1231" and end_date < 20120101 group by ts_code) t_eps2 ON t_eps1.ts_code = t_eps2.ts_code and old_eps is not NULL and new_eps is not NULL and old_eps > 0 and (new_eps / old_eps) > 1.33;

    5.1. 我单独加的，巴菲特标准，最近10年roe>20%。所以我私以为保守投资策略里, 最近10年的roe应该大于15%， 这一条过滤掉了康美药业(财务造假)，所以很有用
        select ts_code from ts_pro_fina_indicator where end_date > 20090101 and end_date < 20190101 and end_date like "%%1231" and roe_waa>15 group by ts_code having count(distinct year(end_date)) >= 10 and

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

    # 看起来第一版的分红数据有问题，连续10年分红且盈利的企业不全:例如 隧道股份
    sql_1 = """
    SELECT `code`, `name` FROM ts_stock_profit
    WHERE (`year`=2017 or `year`=2016 or `year`=2015) AND `business_income`>8000 AND
        `name` in (SELECT `name` from ts_stock_basics WHERE `totalAssets` > 400000 AND
            `code` in (SELECT `code` FROM ts_stock_report WHERE `year`<2018 and `year`>=2008 AND `eps`>0 and `distrib` is not NULL GROUP by `code` HAVING count(distinct `year`) >= 10
            )
        )
    GROUP by `code`
    HAVING count(distinct `year`) = 3;
"""

    cur_year = int((tmp_datetime).strftime("%Y"))
    start_year = cur_year - max_year
    half_num = int(max_year * 0.5)
    peer_num = 3


    sql_pro = """
    select ts_pro_basics.ts_code, symbol, name, area, industry, market, list_date, div_ledger_asset, ledger_asset, average_income, average_cash_div_tax from ts_pro_basics INNER JOIN
    (select ts_b.ts_code, (total_assets - total_liab - IFNULL(`lt_eqt_invest`, 0) * 0.3 - IFNULL(`invest_real_estate`, 0) * 0.3 - IFNULL(`fix_assets`, 0) * 0.3 - IFNULL(`cip`, 0) * 0.3 - IFNULL(`const_materials`, 0) * 0.3 - IFNULL(`fixed_assets_disp`, 0) * 0.8 - IFNULL(`intan_assets`, 0) * 0.8 - IFNULL(`r_and_d`, 0) * 0.2 - IFNULL(`goodwill`, 0) * 0.8 - IFNULL(`lt_amor_exp`, 0) * 0.8 - IFNULL(`oth_nca`, 0) * 0.8 - IFNULL(`defer_tax_assets`, 0) *
    0.8) as div_ledger_asset, (total_assets - total_liab) as ledger_asset, average_income, average_cash_div_tax from ts_pro_balancesheet ts_b
        INNER JOIN (select ts_eps.ts_code, average_income, average_cash_div_tax FROM
            (select t_eps1.ts_code, (new_eps / {peer_num}) as average_income from (select ts_code, sum(n_income_attr_p) as new_eps from ts_pro_income where end_date > {cur_year_peer}0101 and end_date like "%%1231" and end_date < {cur_year}0101 group by ts_code) t_eps1 INNER JOIN (select ts_code, sum(n_income_attr_p) as old_eps from ts_pro_income where end_date > {start_year}0101 and end_date like "%%1231" and end_date < {start_year_peer}0101 group by ts_code) t_eps2 ON t_eps1.ts_code = t_eps2.ts_code and old_eps is not NULL and new_eps is not NULL and
                        old_eps > 0 and (new_eps / old_eps) > 2) ts_eps
            INNER JOIN (
                    select ts_code, sum(cash_div_tax) / {peer_num} as average_cash_div_tax from ts_pro_dividend where end_date > {cur_year_peer}0101 and end_date < {cur_year}0101 and cash_div_tax > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= {peer_num}
            ) ts_dividend on ts_dividend.ts_code=ts_eps.ts_code
        ) ts_income on ts_b.ts_code = ts_income.ts_code and end_date = "{last_year}1231" and total_assets > 4010001000 and
        total_cur_liab is not NULL and total_cur_assets is not NULL and (total_cur_liab <= 0 or ((total_cur_assets / total_cur_liab) > 2.0))
        and (total_liab is NULL or (total_liab <= 0 or total_liab / total_cur_assets < 1.0))
        and (inventories is NULL or (total_cur_assets - inventories) / total_cur_liab > 1.0)
        and ts_b.ts_code in (
            select ts_code from ts_pro_fina_indicator where end_date > {half_year}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and roe>15 group by ts_code having count(distinct year(end_date)) >= {half_num} and
            ts_code in (
                select ts_code from ts_pro_income where end_date > {cur_year_peer}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and total_revenue>4010001000 group by ts_code having count(distinct year(end_date)) >= {peer_num} and
                ts_code in (select ts_code from ts_pro_income where end_date > {start_year}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and diluted_eps > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= {max_year} and
                    ts_code in (
                        select ts_code from ts_pro_dividend where end_date > {start_year}0101 and end_date < {cur_year}0101 and (cash_div_tax > 0 or stk_div > 0) and div_proc="实施" GROUP by ts_code HAVING count(distinct year(end_date)) >= {dividend_num}
                    )
                )
            )
        )
    ) ts_balancesheet on ts_pro_basics.ts_code = ts_balancesheet.ts_code
""".format(
        start_year=start_year, start_year_peer=start_year+peer_num,
        cur_year=cur_year, last_year=cur_year-1, cur_year_peer= cur_year-peer_num,
        peer_num=peer_num, max_year=max_year,
        dividend_num=max_year-1,
        half_num=half_num, half_year=cur_year-half_num
    )
    gen_res_common("ts_res_defensive", sql_pro, cur_year)



def buffett_main(tmp_datetime, max_year=10):
    """
    巴菲特的标准:
        只有净资产收益率不低于20%，而且能稳定增长的企业才能进入其研究范畴
        select t_eps1.ts_code from (select ts_code, sum(n_income_attr_p) as new_eps from ts_pro_income where end_date > 20160101 and end_date like "%%1231" and end_date < 20190101 group by ts_code) t_eps1 INNER JOIN (select ts_code, sum(n_income_attr_p) as old_eps from ts_pro_income where end_date > 20090101 and end_date like "%%1231" and end_date < 20110101 group by ts_code) t_eps2 ON t_eps1.ts_code = t_eps2.ts_code and old_eps is not NULL and new_eps is not NULL and old_eps > 0 and (new_eps / old_eps)
        > 2 and t_eps1.ts_code in (
        select ts_code from ts_pro_fina_indicator where end_date > 20090101 and end_date < 20190101 and end_date like "%%1231" and roe>20 group by ts_code having count(distinct year(end_date)) >= 10);
    """
    cur_year = int((tmp_datetime).strftime("%Y"))
    start_year = cur_year - max_year
    peer_num = 3


    sql_pro = """
    select ts_pro_basics.ts_code, symbol, name, area, industry, market, list_date, div_ledger_asset, ledger_asset, average_income, average_cash_div_tax from ts_pro_basics INNER JOIN
    (select ts_b.ts_code, (total_assets - total_liab - IFNULL(`lt_eqt_invest`, 0) * 0.3 - IFNULL(`invest_real_estate`, 0) * 0.3 - IFNULL(`fix_assets`, 0) * 0.3 - IFNULL(`cip`, 0) * 0.3 - IFNULL(`const_materials`, 0) * 0.3 - IFNULL(`fixed_assets_disp`, 0) * 0.8 - IFNULL(`intan_assets`, 0) * 0.8 - IFNULL(`r_and_d`, 0) * 0.2 - IFNULL(`goodwill`, 0) * 0.8 - IFNULL(`lt_amor_exp`, 0) * 0.8 - IFNULL(`oth_nca`, 0) * 0.8 - IFNULL(`defer_tax_assets`, 0) *
    0.8) as div_ledger_asset,
    (total_assets - total_liab) as ledger_asset, average_income, average_cash_div_tax from ts_pro_balancesheet ts_b
        INNER JOIN (select ts_eps.ts_code, average_income, average_cash_div_tax FROM
            (select t_eps1.ts_code, (new_eps / {peer_num}) as average_income from (select ts_code, sum(n_income_attr_p) as new_eps from ts_pro_income where end_date > {cur_year_peer}0101 and end_date like "%%1231" and end_date < {cur_year}0101 group by ts_code) t_eps1 INNER JOIN (select ts_code, sum(n_income_attr_p) as old_eps from ts_pro_income where end_date > {start_year}0101 and end_date like "%%1231" and end_date < {start_year_peer}0101 group by ts_code) t_eps2 ON t_eps1.ts_code = t_eps2.ts_code and old_eps is not NULL and new_eps is not NULL and
                        old_eps > 0 and (new_eps / old_eps) > 2) ts_eps
            INNER JOIN (
                    select ts_code, sum(cash_div_tax) / {peer_num} as average_cash_div_tax from ts_pro_dividend where end_date > {cur_year_peer}0101 and end_date < {cur_year}0101 and cash_div_tax > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= {peer_num}
            ) ts_dividend on ts_dividend.ts_code=ts_eps.ts_code
        ) ts_income on ts_b.ts_code = ts_income.ts_code and end_date = "{last_year}1231" and total_assets > 2010001000 and
        total_cur_liab is not NULL and total_cur_assets is not NULL and (total_cur_liab <= 0 or (total_cur_assets / total_cur_liab) > 1.3)
        and (total_liab is NULL or (total_liab <= 0 or total_liab / total_cur_assets < 1.0))
        and (inventories is NULL or (total_cur_assets - inventories) / total_cur_liab > 1.0)
        and ts_b.ts_code in (
            select ts_code from ts_pro_fina_indicator where end_date > {start_year}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and roe>20 group by ts_code having count(distinct year(end_date)) >= {max_year} and
            ts_code in (
                select ts_code from ts_pro_income where end_date="{last_year}1231" and total_revenue>2010001000
            )
        )
    ) ts_balancesheet on ts_pro_basics.ts_code = ts_balancesheet.ts_code
""".format(
        start_year=start_year, start_year_peer=start_year+peer_num,
        cur_year=cur_year, last_year=cur_year-1, cur_year_peer= cur_year-peer_num,
        peer_num=peer_num, max_year=max_year,
        dividend_num=max_year-1
    )


    gen_res_common("ts_res_buffett", sql_pro, cur_year)

def defensive_weak_main(tmp_datetime, max_year=8):
    """
    条件: 条件放宽松的defensive版本
    主要为了发现高分红的稳定企业
    """

    cur_year = int((tmp_datetime).strftime("%Y"))
    start_year = cur_year - max_year
    half_num = int(max_year * 0.5)
    peer_num = 3

    sql_pro = """
    select ts_pro_basics.ts_code, symbol, name, area, industry, market, list_date, ledger_asset, div_ledger_asset, average_income, average_cash_div_tax from ts_pro_basics INNER JOIN
    (select ts_b.ts_code, (total_assets - total_liab - IFNULL(`lt_eqt_invest`, 0) * 0.4 - IFNULL(`invest_real_estate`, 0) * 0.4 - IFNULL(`fix_assets`, 0) * 0.4 - IFNULL(`cip`, 0) * 0.4 - IFNULL(`const_materials`, 0) * 0.4 - IFNULL(`fixed_assets_disp`, 0) * 0.8 - IFNULL(`intan_assets`, 0) * 1.0 - IFNULL(`r_and_d`, 0) * 0.3 - IFNULL(`goodwill`, 0) * 1.0 - IFNULL(`lt_amor_exp`, 0) * 1.0 - IFNULL(`oth_nca`, 0) * 0.8 - IFNULL(`defer_tax_assets`, 0) * 1.0) as div_ledger_asset, (total_assets - total_liab) as
    ledger_asset, average_income, average_cash_div_tax from ts_pro_balancesheet ts_b
        INNER JOIN ( select ts_eps.ts_code, average_income, average_cash_div_tax FROM
            (select t_eps1.ts_code, (new_eps / {peer_num}) as average_income from (select ts_code, sum(n_income_attr_p) as new_eps from ts_pro_income where end_date > {cur_year_peer}0101 and end_date like "%%1231" and end_date < {cur_year}0101 group by ts_code) t_eps1
                INNER JOIN (select ts_code, sum(n_income_attr_p) as old_eps from ts_pro_income where end_date > {start_year}0101 and end_date like "%%1231" and end_date < {start_year_peer}0101 group by ts_code) t_eps2
                ON t_eps1.ts_code = t_eps2.ts_code and old_eps is not NULL and new_eps is not NULL and old_eps > 0 and (new_eps / old_eps) > 1.5
            ) ts_eps
            INNER JOIN (
                    select ts_code, sum(cash_div_tax) / {peer_num} as average_cash_div_tax from ts_pro_dividend where end_date > {cur_year_peer}0101 and end_date < {cur_year}0101 and cash_div_tax > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= {peer_num}
            ) ts_dividend on ts_dividend.ts_code=ts_eps.ts_code
        ) ts_income on ts_b.ts_code = ts_income.ts_code and end_date = "{last_year}1231" and total_assets > 2010001000 and
        total_cur_liab is not NULL and total_cur_assets is not NULL and (total_cur_liab <= 0 or ((total_cur_assets / total_cur_liab) > 1.5))
        and (total_liab is NULL or (total_liab <= 0 or total_liab / total_cur_assets < 1.0))
        and (inventories is NULL or (total_cur_assets - inventories) / total_cur_liab > 1.0)
        and ts_b.ts_code in (
            select ts_code from ts_pro_fina_indicator where end_date > {half_year}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and roe>10 group by ts_code having count(distinct year(end_date)) >= {half_num} and
            ts_code in (
                select ts_code from ts_pro_income where end_date > {cur_year_peer}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and total_revenue>2010001000 group by ts_code having count(distinct year(end_date)) >= {peer_num} and
                ts_code in (select ts_code from ts_pro_income where end_date > {start_year}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and diluted_eps > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= {max_year} and
                    ts_code in (
                        select ts_code from ts_pro_dividend where end_date > {start_year}0101 and end_date < {cur_year}0101 and (cash_div_tax > 0 or stk_div > 0) and div_proc="实施" GROUP by ts_code HAVING count(distinct year(end_date)) >= {dividend_num}
                    )
                )
            )
        )
    ) ts_balancesheet on ts_pro_basics.ts_code = ts_balancesheet.ts_code
""".format(
        start_year=start_year, start_year_peer=start_year+peer_num,
        cur_year=cur_year, last_year=cur_year-1, cur_year_peer= cur_year-peer_num,
        peer_num=peer_num, max_year=max_year,
        dividend_num=max_year-1,
        half_num=half_num, half_year=cur_year-half_num
    )
    gen_res_common("ts_res_defensive_weak", sql_pro, cur_year)

def positive_main(tmp_datetime, max_year=6):
    """
    第15章 积极型投资者的股票选
    如果人们能够按照低于其相应净流动资产(扣除所有优先求偿权，并且将固定资产和其他资产的价值看作零)的价格，获得一个分散化的普通股组合，就能得到相当满意的结果

    Q: 为什么要将固定资产看作0？
    A: Lady Pepperell床单、Jantzen泳装和派克笔这样的品牌，被看做是具有巨大价值的资产。但是现在，如果市场并不青睐某家公司，那么，不仅其著名品牌，而且其土地、建筑物和机器设备等都会变得不值钱。

    由于A股水太深，为了避免财报造假，报表还是要满足一些基本条件
    1. 由于6年时间较短，所以利润增长放宽到20%

    主要用于发现市净率比较低的企业，市净率低的公司也要满足一些基本条件

    资产扣除列表
    lt_eqt_invest   长期股权投资。投资有风险，缩水处理
    invest_real_estate 投资性房地产
    fix_assets 固定资产
    cip 在建工程
    const_materials 工程物资
    fixed_assets_disp 固定资产清理，可以理解为报废的固定资产
    intan_assets 无形资产。专利权，特许权，土地使用权等
    r_and_d 研发支出
    goodwill 商誉。可以理解为品牌
    lt_amor_exp 长期待摊费用。
    defer_tax_assets 递延所得税资产
    oth_nca 其他非流动资产

    <上市公司财务报表解读>
    # 无形资产
    是指不具有物质形态，也无法具体测算其数值的资本(固定)资产.
    包括专利权、商标、著作权、商誉、租赁权以及类似未摊销债券折扣那样的递延资产。
    如果要想对他们进行计量的话，这些资产都应该以成本价反映在资产负债表上，但是通常披露的价值纯粹就是一种人为随意确定的价值

    # 账面价值:
    在计算账面价值的一般操作实务中，是把无形资产排除在外，因而它实际上相当于"有形资产的账面价值"

    """
    cur_year = int((tmp_datetime).strftime("%Y"))
    start_year = cur_year - max_year
    half_num = int(max_year * 0.5)
    peer_num = 3

    sql_pro = """
    select ts_pro_basics.ts_code, symbol, name, area, industry, market, list_date, ledger_asset, div_ledger_asset, average_income, average_cash_div_tax from ts_pro_basics INNER JOIN
    (select ts_b.ts_code, (total_assets - total_liab - IFNULL(`lt_eqt_invest`, 0) * 0.4 - IFNULL(`invest_real_estate`, 0) * 0.4 - IFNULL(`fix_assets`, 0) * 0.4 - IFNULL(`cip`, 0) * 0.4 - IFNULL(`const_materials`, 0) * 0.4 - IFNULL(`fixed_assets_disp`, 0) * 0.8 - IFNULL(`intan_assets`, 0) * 1.0 - IFNULL(`r_and_d`, 0) * 0.3 - IFNULL(`goodwill`, 0) * 1.0 - IFNULL(`lt_amor_exp`, 0) * 1.0 - IFNULL(`oth_nca`, 0) * 0.8 - IFNULL(`defer_tax_assets`, 0) * 1.0)
        as div_ledger_asset, (total_assets - total_liab) as ledger_asset, average_income, average_cash_div_tax from ts_pro_balancesheet ts_b
        INNER JOIN ( select ts_eps.ts_code, average_income, average_cash_div_tax FROM
            (select t_eps1.ts_code, (new_eps / {peer_num}) as average_income from (select ts_code, sum(n_income_attr_p) as new_eps from ts_pro_income where end_date > {cur_year_peer}0101 and end_date like "%%1231" and end_date < {cur_year}0101 group by ts_code) t_eps1
                INNER JOIN (select ts_code, sum(n_income_attr_p) as old_eps from ts_pro_income where end_date > {start_year}0101 and end_date like "%%1231" and end_date < {start_year_peer}0101 group by ts_code) t_eps2
                ON t_eps1.ts_code = t_eps2.ts_code and old_eps is not NULL and new_eps is not NULL and old_eps > 0 and (new_eps / old_eps) > 1.2
            ) ts_eps
            INNER JOIN (
                    select ts_code, sum(cash_div_tax) / {peer_num} as average_cash_div_tax from ts_pro_dividend where end_date > {cur_year_peer}0101 and end_date < {cur_year}0101 and cash_div_tax > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= {peer_num}
            ) ts_dividend on ts_dividend.ts_code=ts_eps.ts_code
        ) ts_income on ts_b.ts_code = ts_income.ts_code and end_date = "{last_year}1231" and total_assets > 2010001000 and
        total_cur_liab is not NULL and total_cur_assets is not NULL and (total_cur_liab <= 0 or ((total_cur_assets / total_cur_liab) > 1.3))
        and (total_liab is NULL or (total_liab <= 0 or total_liab / total_cur_assets < 1.0))
        and (inventories is NULL or (total_cur_assets - inventories) / total_cur_liab > 1.0)
        and ts_b.ts_code in (
            select ts_code from ts_pro_fina_indicator where end_date > {half_year}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and roe>5 group by ts_code having count(distinct year(end_date)) >= {half_num} and
            ts_code in (
                select ts_code from ts_pro_income where end_date > {cur_year_peer}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and total_revenue>2010001000 group by ts_code having count(distinct year(end_date)) >= {peer_num} and
                ts_code in (select ts_code from ts_pro_income where end_date > {start_year}0101 and end_date < {cur_year}0101 and end_date like "%%1231" and diluted_eps > 0 GROUP by ts_code HAVING count(distinct year(end_date)) >= {max_year}
                )
            )
        )
    ) ts_balancesheet on ts_pro_basics.ts_code = ts_balancesheet.ts_code
""".format(
        start_year=start_year, start_year_peer=start_year+peer_num,
        cur_year=cur_year, last_year=cur_year-1, cur_year_peer= cur_year-peer_num,
        peer_num=peer_num, max_year=max_year,
        half_num=half_num, half_year=cur_year-half_num
    )
    gen_res_common("ts_res_positive", sql_pro, cur_year)


# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    update_current_year()
    common.run_with_args(defensive_main)
    common.run_with_args(buffett_main)
    common.run_with_args(defensive_weak_main)
    common.run_with_args(positive_main)
