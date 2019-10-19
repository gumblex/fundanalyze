#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import re
import sys
import sqlite3
import collections

import pulp
import pandas
import numpy as np
import pypfopt.cla
import pypfopt.risk_models
import pypfopt.base_optimizer
import pypfopt.expected_returns
import pypfopt.efficient_frontier
import pypfopt.discrete_allocation
import pypfopt.hierarchical_risk_parity

RISK_FREE_RATE = 0.0275

re_kind = re.compile('[A-C]$')

def make_in(values):
    return '(%s)' % ','.join('?' * len(l))

class FundPortfolio:

    def __init__(self, dbname, funds, testdays=30, mindays=180, totalval=10000, topn=None, perfer='A', minval=None):
        self.dbname = dbname
        self.testdays = testdays
        self.mindays = mindays
        self.totalval = totalval
        self.minval = minval
        self.db = sqlite3.connect(dbname)
        self.select_funds(funds, mindays, topn, perfer)
        df = None
        for fid in self.funds:
            newdf = pandas.read_sql("SELECT date, totalval/10000.0 totalval "
                "FROM fund_history WHERE fid=? ORDER BY date",
                self.db, 'date', params=[fid], parse_dates=['date'])
            newdf.rename(columns={'totalval': fid}, inplace=True)
            if df is None:
                df = newdf
            else:
                df = df.join(newdf, how='outer')
        self.df = df
        self.mu = pypfopt.expected_returns.ema_historical_return(df)
        self.S = pypfopt.risk_models.CovarianceShrinkage(df).ledoit_wolf()

    def select_funds(self, funds, mindays, topn, prefer='C'):
        fundnames = {}
        fundranks = {}
        fundtypes = collections.defaultdict(list)
        fundcompanys = collections.defaultdict(list)
        minvalues = {}
        for fid in funds:
            if fid in fundnames:
                continue
            row = self.db.execute(
                "SELECT name, coalesce(minval, 100)/100, type, company "
                "FROM funds "
                "WHERE fid=? AND julianday(updated) - julianday(since) > ?",
                (fid, mindays)).fetchone()
            if row is None:
                continue
            fname, minval, ftype, fcompany = row
            if '定期开放' in fname or '定开' in fname or 'LOF' in fname or 'FOF' in fname:
                continue
            if re_kind.search(fname) and fname[-1] != prefer:
                row = self.db.execute(
                    "SELECT f.fid, f.name, coalesce(f.minval, 100)/100, "
                    " f.type, f.company "
                    "FROM funds f INNER JOIN fund_simrank fs USING (fid) "
                    "WHERE name=?", (fname[:-1] + prefer,)).fetchone()
                if row:
                    print('Replace %s %s with %s %s' % (fid, fname, row[0], row[1]))
                    fid, fname, minval, ftype, fcompany = row
            fundnames[fid] = fname
            minvalues[fid] = max(self.minval, minval)
            fundtypes[ftype].append(fid)
            fundcompanys[fcompany].append(fid)
            ranks = [row[0] for row in self.db.execute(
                "SELECT 1-CAST(rank AS REAL)/total "
                "FROM fund_simrank WHERE fid=? ORDER BY date", (fid,))]
            fundranks[fid] = float(
                pandas.DataFrame(ranks).ewm(span=500).mean().iloc[-1])
        if topn is None:
            self.funds = fundnames
            self.fundranks = fundranks
            self.minvalues = minvalues
            return
        self.funds = {}
        self.fundranks = {}
        self.minvalues = {}
        usefid = {k:pulp.LpVariable('x_%s' % k, cat='Binary') for k in fundnames}
        useft = {}
        usecmp = {}
        m = pulp.LpProblem("FundSel", pulp.LpMaximize)
        m += pulp.lpSum(fundranks[k] * v for k, v in usefid.items())
        m += pulp.lpSum(usefid.values()) == min(len(fundnames), topn)
        for k, ftype in enumerate(fundtypes):
            useft[k] = pulp.LpVariable('ft_%s' % k, cat='Binary')
            fttotal = 0
            for fid in fundtypes[ftype]:
                fttotal += usefid[fid]
                m += useft[k] >= usefid[fid]
            m += fttotal >= useft[k]
        m += pulp.lpSum(useft.values()) >= min(len(fundtypes), 3)
        for k, fcompany in enumerate(fundcompanys):
            usecmp[k] = pulp.LpVariable('fc_%s' % k, 0)
            m += pulp.lpSum(usefid[fid] for fid in fundcompanys[fcompany]
                           ) <= int(len(fundnames) * 0.4)
        m.solve()
        for fid in fundranks:
            if int(usefid[fid].varValue):
                self.funds[fid] = fundnames[fid]
                self.fundranks[fid] = fundranks[fid]
                self.minvalues[fid] = minvalues[fid]
        print(self.fundranks)

    def evaluate(self, func):
        df_train = self.df[:-self.testdays]
        df_test = self.df[-self.testdays:]
        df_test_ret = (pypfopt.expected_returns.daily_price_returns(df_test)
            + 1).product() # / testdays * 252
        weights = func(df_train)
        alloc, rem, realweights = self.postprocess(weights)
        expret, volt, sr = pypfopt.base_optimizer.portfolio_performance(
            self.mu, self.S, realweights, risk_free_rate=RISK_FREE_RATE)
        newtotal = rem
        for k, v in alloc.items():
            newtotal += v * df_test_ret[k]
        realanret = (newtotal / self.totalval - 1) / self.testdays * 252
        return alloc, rem, expret, volt, sr, realanret

    def opt_max_sharpe(self, df):
        mu = pypfopt.expected_returns.ema_historical_return(df)
        S = pypfopt.risk_models.CovarianceShrinkage(df).ledoit_wolf()
        ef = pypfopt.cla.CLA(mu, S)
        #ef = pypfopt.efficient_frontier.EfficientFrontier(mu, S)
        return ef.max_sharpe()

    def opt_min_volatility(self, df):
        mu = pypfopt.expected_returns.ema_historical_return(df)
        S = pypfopt.risk_models.CovarianceShrinkage(df).ledoit_wolf()
        ef = pypfopt.cla.CLA(mu, S)
        #ef = pypfopt.efficient_frontier.EfficientFrontier(mu, S)
        return ef.min_volatility()

    def opt_hrp(self, df):
        returns = pypfopt.expected_returns.daily_price_returns(df)
        return pypfopt.hierarchical_risk_parity.hrp_portfolio(returns)

    def postprocess(self, weights):
        alloc, rem = pypfopt.discrete_allocation.portfolio_byvalue(
            weights, 1, self.minvalues, 1e9, self.totalval)
        return alloc, rem, {k: w / (self.totalval - rem) for k, w in alloc.items()}

    def run(self):
        for func in (self.opt_max_sharpe, self.opt_min_volatility, self.opt_hrp):
            alloc, rem, expret, volt, sr, realanret = self.evaluate(func)
            print('========== %s ==========' % func.__name__)
            for k, v in alloc.items():
                if v:
                    print("%s %s (Rank %.2f%%): %s" % (
                        k, self.funds[k], self.fundranks[k] * 100, v))
            print("Remaining: %.2f" % rem)
            print("Expected annual return: {:.2f}%".format(100 * expret))
            print("Actual annual return: {:.2f}%".format(100 * realanret))
            print("Annual volatility: {:.2f}%".format(100 * volt))
            print("Sharpe Ratio: {:.2f}".format(sr))

if __name__ == '__main__':
    #funds = input('funds> ').strip().split()
    fp = FundPortfolio(sys.argv[1], sys.argv[2:], 50, 180, 10000, 5, minval=200)
    fp.run()
