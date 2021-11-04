'''Set of utilities for working with RGA data.'''
import numpy as np
import pandas as pd
import os
import pdb
import datetime as dt
import pickle as p
from pylab import *
import pytz


def ProcessRGAPvTScan(ifile,tz='US/Mountain'):
    ''' Process a data text datalog file saved by RGA.  The time processing is real slow, so will take a long time for big files.  Saves the data to a pickle file with same path/name as ifile, but with .pkl as the extension.''' 

    # read the log file for header info
    f = open(ifile,'r')
    ll = f.readlines()
    f.close()


    ######## 
    #get the header information 
    #######

    #start time
    lino=16
    ln = ll[lino]
    sep = ', '
    dtstr = sep.join(ln.strip().split(',')[1::])
    ddt = dt.datetime.strptime(dtstr, " %b %d, %Y %I:%M:%S %p")


    # channel info 
    lino=17

    ln = ll[lino].split(',')
    ch = []
    for h in ln:
        ch.append(h.strip())

    lino += 2
    hflag=1

    cn = pd.DataFrame(np.array([ll[lino].split()]),columns=ch)


    while hflag:
        lino += 1
        try:
            ll[lino].split()[1]
        except IndexError:
            hflag = 0
            continue
        ci = pd.DataFrame(np.array([ll[lino].split()]),columns=ch)
        cn = cn.append(ci)

    cn.set_index('Channel',inplace=True)

    dd = pd.read_csv(ifile,skiprows=30,header=None,sep=",\s+|\s+",index_col=False,names=np.concatenate((['Time(s)'],cn['Mass(amu)'].values)))

    #make the index date rather than elapsed time.
    dd['Time']=np.zeros(len(dd['Time(s)']))
    tl = len(dd)
    for i in range(len(dd['Time(s)'])):
        print('processing dates')
        td_i = dt.timedelta(seconds=dd['Time(s)'][i])
        dd['Time'][i]=ddt+td_i
        print('%.2f perc. complete' %(float(i)/tl*100.))

    dd.set_index('Time',inplace=True)
    #make tz aware
    dd=dd.tz_localize(tz,ambiguous='NaT')
    del(dd['Time(s)']) 
    dfile = ifile.split('.')[0]+'.pkl'
    df=open(dfile,'wb')
    p.dump((cn,dd),df)
    df.close()
    return cn,dd
