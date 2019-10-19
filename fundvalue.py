#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import ast
import time
import json
import random
import decimal
import sqlite3
import logging
import datetime
import argparse
import requests
import operator
import contextlib
import collections
import concurrent.futures
import bs4

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36",
}

logging.basicConfig(stream=sys.stderr, format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)

re_jscomment = re.compile('/\*.*?\*/')
re_var = re.compile('\s*var (\w+)\s*=\s*(.+?)\s*;+')
re_callback = re.compile('^\s*\w+\(\s*(.+)\s*\);?$')

jquery_jsonp_name = lambda: 'jQuery%d_%d' % (
    random.randrange(1830000000000000000000, 1831000000000000000000),
    time.time() * 1000)

dec2int = lambda x: int(decimal.Decimal(x) * 10000) if x else None
dec2int100 = lambda x: int(decimal.Decimal(x) * 100) if x else None
cint = lambda x: None if x is None or x == '' else int(x)
cfloat = lambda x: None if x is None or x == '' else float(x)
ms2date = lambda s: time.strftime('%Y-%m-%d', time.gmtime(s // 1000 + 8*3600))

def make_insert(d):
    keys, values = zip(*d.items())
    return ', '.join(keys), ', '.join('?' * len(values)), values

def make_update(d):
    keys, values = zip(*d.items())
    return ', '.join(k + '=?' for k in keys), values

def make_where(d):
    keys, values = zip(*d.items())
    return ' AND '.join(k + '=?' for k in keys), values

def update_partial(cursor, table, keys, values):
    inskeys, qms, vals = make_insert(keys)
    cursor.execute("INSERT OR IGNORE INTO %s (%s) VALUES (%s)" % (
        table, inskeys, qms), vals)
    setkeys, vals1 = make_update(values)
    whrkeys, vals2 = make_where(keys)
    cursor.execute("UPDATE %s SET %s WHERE %s" % (
        table, setkeys, whrkeys), vals1 + vals2)

def parse_jsvars(js, errors='ignore', **kwargs):
    i_jsvars = iter(filter(None, re_var.split(re_jscomment.sub('', js))))
    result = {}
    for name, value in zip(i_jsvars, i_jsvars):
        try:
            result[name] = json.loads(value, **kwargs)
        except json.JSONDecodeError:
            if errors == 'literal_eval':
                result[name] = ast.literal_eval(value)
            elif errors == 'ignore':
                result[name] = None
            else:
                raise
    return result

def parse_worktime(s):
    if not s:
        return None
    yearspl = s.split('年')
    if len(yearspl) == 1:
        return int(s[:-1])
    res = round(int(yearspl[0])*365.25)
    if yearspl[1]:
        res += int(yearspl[1][1:-1])
    return res

def date_year(s):
    month, day = [int(x.strip('0')) for x in s.split('-')]
    date = datetime.date.today().replace(month=month, day=day)
    if date > datetime.date.today():
        date = date.replace(date.year-1)
    return date.isoformat()

class EMFundClient:

    def __init__(self, db='funds.db'):
        self.db = sqlite3.connect(db)
        self.init_db()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.executor = concurrent.futures.ThreadPoolExecutor(6)

    def init_db(self):
        cur = self.db.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS funds ('
            'fid TEXT PRIMARY KEY,'
            'name TEXT,'
            'type TEXT,'
            'risk TEXT,'
            'company TEXT,'
            'company_id INTEGER,'
            'since TEXT,'
            'updated TEXT,' # YYYY-MM-DD
            'star INTEGER,'
            'unitval INTEGER,'  # * 10000, 货币基金为每万份收益	
            'totalval INTEGER,' # * 10000, 货币基金为7日年化
            'rate INTEGER,' # * 10000 (100%)
            'rate_em INTEGER,' # * 10000 (100%)
            'minval INTEGER,' # * 100
            'return_1m REAL,'
            'return_3m REAL,'
            'return_6m REAL,'
            'return_1y REAL,' # %
            'return_3y REAL,' # %
            'return_all REAL,' # %
            'stockcodes TEXT,'
            'bondcodes TEXT,'
            'perf_average REAL,'
            'perf_choice REAL,' # 选证能力
            'perf_return REAL,' # 收益率
            'perf_risk REAL,'   # 抗风险
            'perf_stable REAL,' # 稳定性
            'perf_time REAL'   # 择时能力
        ')')
        cur.execute('CREATE TABLE IF NOT EXISTS managers ('
            'id INTEGER PRIMARY KEY,'
            'name TEXT,'
            'star INTEGER,'
            'updated TEXT,'
            'worktime INTEGER,' # round(365.25 * years + days)
            'fund_num INTEGER,'
            'fund_asset INTEGER,'
            'profit REAL,'
            'profit_average REAL,'
            'profit_hs300 REAL,'
            'perf_average REAL,'
            'perf_experience REAL,' # 经验值
            'perf_return REAL,' # 收益率
            'perf_risk REAL,'   # 抗风险
            'perf_stable REAL,' # 稳定性
            'perf_time REAL,'   # 择时能力
            'pic TEXT'
        ')')
        cur.execute('CREATE TABLE IF NOT EXISTS fund_managers ('
            'fid TEXT,'
            'manager INTEGER,'
            'updated TEXT,'
            'PRIMARY KEY (fid, manager)'
        ')')
        cur.execute('CREATE TABLE IF NOT EXISTS fund_simrank ('
            'fid TEXT,'
            'date TEXT,'
            'rank INTEGER,'
            'total INTEGER,'
            'PRIMARY KEY (fid, date)'
        ')')
        cur.execute('CREATE TABLE IF NOT EXISTS fund_info ('
            'fid TEXT,'
            'date TEXT,'
            'asset INTEGER,' # Data_assetAllocation or Data_fluctuationScale
            'share INTEGER,'
            'holder_company REAL,' # %
            'holder_individual REAL,' # %
            'holder_internal REAL,' # %
            'stock_ratio REAL,' # %
            'bond_ratio REAL,' # %
            'cash_ratio REAL,' # %
            'buy INTEGER,'
            'sell INTEGER,'
            'PRIMARY KEY (fid, date)'
        ')')
        cur.execute('CREATE TABLE IF NOT EXISTS fund_stockshare ('
            'fid TEXT,'
            'date TEXT,'
            'share REAL,' # %
            'PRIMARY KEY (fid, date)'
        ')')
        cur.execute('CREATE TABLE IF NOT EXISTS fund_history ('
            'fid TEXT,'
            'date TEXT,'
            'unitval INTEGER,'  # * 10000, 货币基金为每万份收益
            'totalval INTEGER,' # * 10000, 货币基金为7日年化
            'incratepct REAL,'
            'dividend TEXT,'
            'divcash INTEGER,' # * 10000
            'divval INTEGER,' # * 10000
            'buystatus TEXT,'
            'sellstatus TEXT,'
            'PRIMARY KEY (fid, date)'
        ')')
        self.db.commit()

    def fund_list(self):
        if self.db.execute("SELECT 1 FROM funds").fetchone():
            return
        req = self.session.get('http://fund.eastmoney.com/js/fundcode_search.js')
        req.raise_for_status()
        match = re_var.match(req.text)
        if not match:
            raise ValueError("can't parse fundcode_search.js: " + req.text)
        d = json.loads(match.group(2))
        cur = self.db.cursor()
        cur.executemany("INSERT OR IGNORE INTO funds (fid, name, type)"
            " VALUES (?,?,?)", map(operator.itemgetter(0, 2, 3), d))
        self.db.commit()

    def fund_info(self, fundid):
        if self.db.execute("SELECT 1 FROM funds WHERE fid=? AND updated=?", (
            fundid, time.strftime('%Y-%m-%d'))).fetchone():
            return
        pageurl = 'http://fund.eastmoney.com/%s.html' % fundid
        req = self.session.get(pageurl)
        req.raise_for_status()
        soup = bs4.BeautifulSoup(req.content, 'lxml')
        infotds = soup.find('div', class_='infoOfFund').find_all('td')
        dl02 = soup.find('dl', class_='dataItem02')
        updatedtxt = dl02.dt.p.contents[-1].strip('()')
        cfreturn = lambda s: float(s.rstrip('%')) if s != '--' else None
        d_funds = {
            'since': infotds[3].contents[-1][1:],
            'company': infotds[4].a.string,
            'company_id': infotds[4].a['href'].rsplit('/', 1)[1].split('.')[0],
            'star': cint(infotds[5].div['class'][0][4:]),
            'return_3y': cfreturn(dl02.contents[-1].find(
                'span', class_='ui-num').string),
            'return_all': cfreturn(soup.find('dl', class_='dataItem03'
                ).contents[-1].find('span', class_='ui-num').string),
        }
        if not infotds[0].contents[-1].name:
            d_funds['risk'] = infotds[0].contents[-1][5:]
        req = self.session.get(
            'http://fund.eastmoney.com/pingzhongdata/%s.js?v=%s' % (
            fundid, time.strftime('%Y%m%d%H%M%S')), headers={"Referer": pageurl})
        req.raise_for_status()
        js = parse_jsvars(req.text, 'ignore', parse_float=decimal.Decimal)
        if js['ishb']:
            d_funds['updated'] = date_year(updatedtxt)
            d_funds['unitval'] = dec2int(
                soup.find('dl', class_='dataItem01').dd.span.string)
            d_funds['totalval'] = dec2int(dl02.dd.span.string.rstrip('%'))
        else:
            d_funds['updated'] = updatedtxt
            d_funds['unitval'] = dec2int(dl02.dd.span.string)
            d_funds['totalval'] = dec2int(
                soup.find('dl', class_='dataItem03').dd.span.string)
            d_funds['stockcodes'] = ','.join(js['stockCodes']) or None
            d_funds['bondcodes'] = js['zqCodes'] or None
        d_funds['name'] = js['fS_name']
        d_funds['rate'] = dec2int100(js['fund_sourceRate'])
        d_funds['rate_em'] = dec2int100(js['fund_Rate'])
        d_funds['minval'] = dec2int100(js['fund_minsg'])
        d_funds['return_1m'] = cfloat(js['syl_1y'])
        d_funds['return_3m'] = cfloat(js['syl_3y'])
        d_funds['return_6m'] = cfloat(js['syl_6y'])
        d_funds['return_1y'] = cfloat(js['syl_1n'])
        js_perf = js.get('Data_performanceEvaluation')
        if js_perf:
            if js_perf['avr'] != '暂无数据':
                d_funds['perf_average'] = cfloat(js_perf['avr'])
            if js_perf['data']:
                d_funds['perf_choice'] = cfloat(js_perf['data'][0])
                d_funds['perf_return'] = cfloat(js_perf['data'][1])
                d_funds['perf_risk'] = cfloat(js_perf['data'][2])
                d_funds['perf_stable'] = cfloat(js_perf['data'][3])
                d_funds['perf_time'] = cfloat(js_perf['data'][4])
        cur = self.db.cursor()
        update_partial(cur, 'funds', {'fid': fundid}, d_funds)
        cur.execute("DELETE FROM fund_managers WHERE fid=?", (fundid,))
        for manager in js['Data_currentFundManager']:
            managerid = int(manager['id'])
            d_manager = {
                'name': manager['name'],
                'star': cint(manager['star']),
                'updated': manager['power']['jzrq'],
                'worktime': parse_worktime(manager['workTime']),
                'pic': manager['pic']
            }
            if manager['power']['avr'] != '暂无数据':
                d_manager['perf_average'] = cfloat(manager['power']['avr'])
            if manager['power']['data']:
                d_manager['perf_experience'] = cfloat(manager['power']['data'][0])
                d_manager['perf_return'] = cfloat(manager['power']['data'][1])
                d_manager['perf_risk'] = cfloat(manager['power']['data'][2])
                d_manager['perf_stable'] = cfloat(manager['power']['data'][3])
                d_manager['perf_time'] = cfloat(manager['power']['data'][4])
            if manager.get('fundSize'):
                d_manager['fund_num'] = int(
                    manager['fundSize'].split('(')[1].split('只')[0])
                d_manager['fund_asset'] = int(decimal.Decimal(
                    manager['fundSize'].split('亿')[0]).scaleb(8))
            with contextlib.suppress(KeyError, IndexError):
                d_manager['profit'] = cfloat(manager['profit']['series'
                    ][0]['data'][0]['y'])
            with contextlib.suppress(KeyError, IndexError):
                d_manager['profit_average'] = cfloat(manager['profit']['series'
                    ][0]['data'][1]['y'])
            with contextlib.suppress(KeyError, IndexError):
                d_manager['profit_hs300'] = cfloat(manager['profit']['series'
                    ][0]['data'][2]['y'])
            update_partial(cur, 'managers', {'id': managerid}, d_manager)
            cur.execute("REPLACE INTO fund_managers VALUES (?,?,?)", (
                fundid, managerid, d_funds['updated']
            ))
        for row in js['Data_rateInSimilarType']:
            cur.execute("INSERT OR IGNORE INTO fund_simrank VALUES (?,?,?,?)", (
                fundid, ms2date(row['x']), cint(row['y']), cint(row['sc'])
            ))
        d_finfo = collections.defaultdict(dict)
        jsgraph = js.get('Data_fundSharesPositions', [])
        for row in jsgraph:
            cur.execute("INSERT OR IGNORE INTO fund_stockshare VALUES (?,?,?)",
                (fundid, ms2date(row[0]), cfloat(row[1])))
        jsgraph = js['Data_fluctuationScale']
        for k, row in zip(jsgraph['categories'], jsgraph['series']):
            d_finfo[k]['asset'] = int(row['y'].scaleb(8))
        jsgraph = js['Data_holderStructure']
        for k, row in zip(jsgraph['categories'],
            zip(*(r['data'] for r in jsgraph['series']))):
            for kh, val in zip(
                ('holder_company', 'holder_individual', 'holder_internal'), row):
                d_finfo[k][kh] = cfloat(val)
        if js['ishb']:
            jsgraph = js['Data_assetAllocationCurrency']
        else:
            jsgraph = js['Data_assetAllocation']
        for k, row in zip(jsgraph['categories'],
            zip(*(r['data'] for r in jsgraph['series']))):
            if js['ishb']:
                k = date_year(k)
            for kh, val in zip(
                ('stock_ratio', 'bond_ratio', 'cash_ratio', 'asset'), row):
                if kh == 'asset':
                    d_finfo[k][kh] = int(val.scaleb(8))
                else:
                    d_finfo[k][kh] = cfloat(val)
        if js['ishb']:
            jsgraph = js['Data_assetAllocation']
        else:
            jsgraph = js['Data_buySedemption']
        for k, row in zip(jsgraph['categories'],
            zip(*(r['data'] for r in jsgraph['series']))):
            for kh, val in zip(('buy', 'sell', 'share'), row):
                d_finfo[k][kh] = int(val.scaleb(8))
        for k, row in sorted(d_finfo.items()):
            update_partial(cur, 'fund_info', {'fid': fundid, 'date': k}, row)
        self.db.commit()

    def fund_name(self, fundid):
        result = self.db.execute(
            "SELECT name, type FROM funds WHERE fid=?", (fundid,)).fetchone()
        return result

    def fund_history(self, fundid):
        cur = self.db.cursor()
        pageidx = 1
        rownum = cur.execute("SELECT count(*) FROM fund_history WHERE fid=?",
            (fundid,)).fetchone()[0]
        #rownum = 0
        totalnum = 1000000
        while rownum < totalnum:
            print('%s %d/%d' % (fundid, rownum, totalnum))
            req = self.session.get(
                'http://api.fund.eastmoney.com/f10/lsjz?'
                'callback=%s&fundCode=%s&pageIndex=%d&pageSize=100'
                '&startDate=&endDate=&_=%d' % (
                jquery_jsonp_name(), fundid, pageidx, time.time() * 1000),
                headers={"Referer":
                'http://fundf10.eastmoney.com/jjjz_%s.html' % fundid})
            req.raise_for_status()
            match = re_callback.match(req.text)
            d = json.loads(match.group(1))
            totalnum = d['TotalCount']
            for row in d['Data']['LSJZList']:
                cur.execute("INSERT OR IGNORE INTO fund_history "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)", (
                    fundid, row['FSRQ'], dec2int(row['DWJZ']),
                    dec2int(row['LJJZ']), cfloat(row['JZZZL']),
                    row['FHSP'], dec2int(row['FHFCZ']), dec2int(row['FHFCBZ']),
                    row['SGZT'], row['SHZT'],
                ))
                rownum += 1
            pageidx = int(rownum / 100) + 1
            self.db.commit()

    def __del__(self):
        self.db.commit()
        self.db.close()


if __name__ == '__main__':
    fc = EMFundClient(sys.argv[1])
    print('Getting fund list...')
    fc.fund_list()
    for fid in sys.argv[2:]:
        fname, ftype = fc.fund_name(fid)
        print(fid, fname, ftype)
        retry = 3
        while 1:
            try:
                fc.fund_info(fid)
                fc.fund_history(fid)
                break
            except Exception:
                retry -= 1
                if not retry:
                    raise
