# utf8
r"""!/usr/bin/env python
 请求API数据
 需要在以下路径C:\Users\Haiyu 中放入.ecmwfapirc文件，文件中存入以下API信息
 51个成员数据是互补和重复的？？可能需要选好几个成员才能实现想要的数据全覆盖
 指定区域可能使数据处理报错，要GRID和AREA能整除才行,不然会数据量不一致，建议采用0.25*0.25，经纬度用整数
 常规要素139.128/144.128/164.128/165.128/166.128/167.128/168.128/169.128/175.128/228.128,
 /10/11/12/13/14/15/16/17/18/19/20/21/22/23/24/25/26/27/28/29/30/31/32/33/34/35/36/37/38/39/40/41/42/43/44/45/46/47/48/49/50
 全国经纬度  area=10/70/55/140,

 Modify ECMWFService library source code
 with open(r'F:\cy\10_years_ec\Code\database\extra\file_df.csv', 'w+', encoding='utf8') as f:
     f.write(self.connection.last.get("name"))   Daryl customed: log request id to match file name.

!!! ECMWFService library modified!!!
Goal: to record request id and request datetime.

After
self.log("Request id: " + self.connection.last.get("name"))
The following to line added.
with open(r'F:\cy\Document\extra_files\ReqLogTemp.txt', 'w+', encoding='utf8') as f:
    f.write(f'{self.connection.last.get("name")},{datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')  # Daryl customed: log request id to match file name.
"""

import pandas as pd
from multiprocessing import Process
import time
from ecmwfapi import ECMWFService
import re


def request_long(date='2022-05-01', number='0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15', param='51.128/52.128/55.128'):
    """
    :param date:
    :param number: [0-15], [16-30], [30-45], [46-50]. Don't add too much number at a time, otherwise, dataloss when parsing .grib file
    :param param: [168.128/165.128/166.128/151.128/176.128/177.128/228.128], [51.128/52.128/55.128]
    :return: None.
    """
    server = ECMWFService("mars")
    step_start = 1 if '52.128' in param else 0

    req = f'''
    retrieve,
    class=od,
    date={date},
    expver=1,
    levtype=sfc,
    method=1,
    number={number},
    origin=ecmf,
    param={param},
    step={24*step_start}/to/5160/by/24,
    stream=mmsf,
    system=5,
    time=00:00:00,
    type=fc,
    area=15/70/55/135,
    GRID=0.25/0.25
    '''

    target = f'test.grib'
    server.execute(req, target)


def request_ens(date, number, param):
    """

    :param date: dates contained in a released date, such as 2018-01-01/2018-01-02/2018-01-03
    :param number: 0(type=cf) or range(1, 51)(type=pf)
    :param param: 168.128/165.128/166.128/151.128/176.128/177.128/228.128(step_start=0) or 51.128/52.128/55.128(start_step=1)
    :return: None
    """

    server = ECMWFService("mars")
    number_type = 'cf' if number=='0' else 'pf'
    step_start = 1 if '121.128' in param else 0
    step_by = 6 if '121.128' in param else 24

    req = f'''
    retrieve,
    class=od,
    date={date},
    expver=1,
    levtype=sfc,
    param={param},
    number={number},
    step=6,
    stream=enfo,
    time=00:00:00,
    type={number_type},
    area=31.25/120.75/31.75/121.1,
    GRID=0.25/0.25
    '''
    # 31.26, 120.97
    # area=15/70/55/135, 全国数据
    # step={6*step_start}/to/360/by/{step_by},
    if number_type == 'cf':  # remove "number={number}"
        req = re.sub(' *number=[\d\/]*,\n', '', req)

    target = 'test.grib'  # do not save locally but this line is necessary
    server.execute(req, target)


def send_request(date, number, param, mode='L'):
    if mode == 'L':
        request_long(date=date, number=number, param=param)
    else:
        request_ens(date=date, number=number, param=param)


if __name__ == '__main__':
    pass


