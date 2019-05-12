#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import sys
sys.path.append("/data/stock/")

import libs.common as common
import pandas as pd
import numpy as np
import math
import datetime
import stockstats


# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    defensive_main()
