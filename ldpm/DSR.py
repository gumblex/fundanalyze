#!/usr/bin/env python
# On 20140607 by lopezdeprado@lbl.gov
import numpy as np,scipy.stats as ss,pandas as pd
from itertools import product
#-------------------------------------------------------------------------------
def getExpMaxSR(mu,sigma,numTrials):
    # Compute the expected maximum Sharpe ratio (Analytically)
    emc=0.5772156649 # Euler-Mascheroni constant
    maxZ=(1-emc)*ss.norm.ppf(1-1./numTrials)+emc*ss.norm.ppf(1-1./(numTrials*np.e))
    return mu+sigma*maxZ
#-------------------------------------------------------------------------------
def getDistMaxSR(mu,sigma,numTrials,numIters):
    # Compute the expected maximum Sharpe ratio (Numerically)
    maxSR,count=[],0
    while count<numIters:
        count+=1
        series=np.random.normal(mu,sigma,numTrials)
        maxSR.append(max(series))
    return np.mean(maxSR),np.std(maxSR)
#-------------------------------------------------------------------------------
def simulate(mu,sigma,numTrials,numIters):
    #1) Compute Expected[Max{Sharpe ratio}] ANALYTICALLY
    expMaxSR=getExpMaxSR(mu,sigma,numTrials)
    #2) Compute Expected[Max{Sharpe ratio}] NUMERICALLY
    meanMaxSR,stdMeanMaxSR=getDistMaxSR(mu,sigma,numTrials,numIters)
    return expMaxSR,meanMaxSR,stdMeanMaxSR
#-------------------------------------------------------------------------------
def main():
    numIters,sigma,output,count=1e4,1,[],0
    for prod_ in product(np.linspace(-100,100,101),list(range(10,1001,10))):
        mu,numTrials=prod_[0],prod_[1]
        expMaxSR,meanMaxSR,stdMaxSR=simulate(mu,sigma,numTrials,numIters)
        err=expMaxSR-meanMaxSR
        output.append([mu,sigma,numTrials,numIters,expMaxSR,meanMaxSR, \
            stdMaxSR,err])
    output=pd.DataFrame(output,columns=['mu','sigma','numTrials','numIters', \
        'expMaxSR','meanMaxSR','stdMaxSR','err'])
    output.to_csv('DSR.csv')
    return
#-------------------------------------------------------------------------------
if __name__=='__main__':
    main()