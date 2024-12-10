# ===========================================================
# ========================= imports =========================
import sys
import datetime
import pathlib
from typing import Union
from gnssvod.funcs.date import (gpsweekday, datetime2doy)
from gnssvod.doc.IGS import IGS, is_IGS
# ===========================================================

def obsFileName(stationName, date, zipped = False):
    doy = datetime2doy(date, string = True)
    if len(doy) == 1:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "o"
    elif len(doy) == 2:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "o"
    else:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "o"
    
    if zipped == True:
        rinexFile = rinexFile + ".Z"
    
    return rinexFile

def sp3FileName(epoch: datetime.date, product: str ="gfz", dir: Union[str,None] = None):
    now = datetime.date.today() # today's date
    timeDif = now - epoch # time difference between rinex epoch and today
    gpsWeek, gpsWeekday = gpsweekday(epoch, Datetime = True)

    if gpsWeek<2038:
        # rely on older gnss orbits
        if len(str(gpsWeek)) == 3:
            sp3File = product.lower() + "0" + str(gpsWeekday) + ".sp3"
        else:
            sp3File = product.lower() + str(gpsWeekday) + ".sp3"
    else:
        if timeDif.days == 0:
            raise Warning("Orbit files are not released yet for", epoch.ctime())
            sys.exit("Exiting...") 
        # we will force using GFZ for now..
        product = 'GFZ0MGXRAP'
        suffix  = '0000_01D_05M_ORB.SP3'
        sp3File = product.upper() + "_" + epoch.strftime('%Y%j') + suffix
    
    if not dir is None:
        sp3File = str(pathlib.Path(dir,sp3File))

    return sp3File

def clockFileName(epoch, interval=30, product="cod", dir: Union[str,None] = None):
    now = datetime.date.today()
    timeDif = now - epoch
    gpsWeek, gpsWeekday = gpsweekday(epoch, Datetime = True) 

    if gpsWeek<2038:
        # rely on older gnss orbits
        if interval < 30:
            product = 'cod'
            extension = '.clk_05s'
        else:
            extension = '.clk'

        if len(str(gpsWeek)) == 3:
            clockFile = product.lower() + "0" + str(gpsWeekday) + extension
        else:
            clockFile = product.lower() + str(gpsWeekday) + extension
    else:
        if timeDif.days == 0:
            raise Warning("Clock files are not released yet for", epoch.ctime())
            sys.exit("Exiting...") 
        # we will force using GFZ for now..
        product = 'GFZ0MGXRAP'
        suffix  = '0000_01D_30S_CLK.CLK'
        clockFile = product.upper() + "_" + epoch.strftime('%Y%j') + suffix

    if not dir is None:
        clockFile = str(pathlib.Path(dir,clockFile))

    return clockFile

def ionFileName(date, product = "igs", zipped = False):
    doy = datetime2doy(date, string = True)
    if len(doy) == 1:
        ionFile = product + "g" + doy + "0." + str(date.year)[-2:] + "i"
    elif len(doy) == 2:
        ionFile = product + "g" + doy + "0." + str(date.year)[-2:] + "i"
    else:
        ionFile = product + "g" + doy + "0." + str(date.year)[-2:] + "i"

    if zipped == True:
        ionFile = ionFile + ".Z"
    
    return ionFile

def navFileName(stationName, date, zipped = False):
    doy = datetime2doy(date, string = True)
    if len(doy) == 1:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "n"
    elif len(doy) == 2:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "n"
    else:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "n"
    
    if zipped == True:
        rinexFile = rinexFile + ".Z"
    
    return rinexFile

def nav3FileName(stationName, date, zipped = False):
    doy = datetime2doy(date, string = True) # for RINEX data names
    siteInfo = IGS(stationName)
    if stationName.upper() == "BRDC":
        rinexFile = "BRDC00IGS_R_" + str(date.year) + str(doy) + "0000_01D_MN.rnx"
    else:
        rinexFile = siteInfo.SITE[0] + "_R_" + str(date.year) + str(doy) + "0000_01D_MN.rnx"
    """
    if len(doy) == 1:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "p"
    elif len(doy) == 2:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "p"
    else:
        rinexFile = stationName + doy + "0." + str(date.year)[-2:] + "p"
    """
    if zipped == True:
        rinexFile = rinexFile + ".gz"
    
    return rinexFile

def obs3FileName(stationName, date, zipped = False):
    doy = datetime2doy(date, string = True) # for RINEX data names
    siteInfo = IGS(stationName)
    rinexFile = siteInfo.SITE[0] + "_R_" + str(date.year) + str(doy) + "0000_01D_30S_MO.crx"
    if zipped == True:
        rinexFile = rinexFile + ".gz"
    
    return rinexFile
