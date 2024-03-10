"""
calc_vod calculates VOD according to specified pairing rules
"""
# ===========================================================
# ========================= imports =========================
import os
import time
import glob
import datetime
import numpy as np
import pandas as pd
import xarray as xr
import warnings
from gnssvod.io.preprocess import get_filelist
import pdb
#--------------------------------------------------------------------------
#----------------- CALCULATING VOD -------------------
#-------------------------------------------------------------------------- 

def calc_vod(filepattern,pairings,bands):
    """
    Combines a list of NetCDF files containing gathered GNSS receiver data, calculates VOD and returns that data.
    
    The gathered GNSS receiver data is typically generated with the function 'gather_stations'.
    
    VOD is calculated based on pairing rules referring to station names.
    
    Parameters
    ----------
    filepattern: dictionary 
        a UNIX-style pattern to find the processed NetCDF files.
        For example filepattern='/path/to/files/of/case1/*.nc'
    
    pairings: dictionary
        A dictionary of pairs of station names indicating first the reference station and second the ground station.
        For example pairings={'Laeg1':('Laeg2_Twr','Laeg1_Grnd')}

    bands: dictionary
        Dictionary of column names to be used for combining different bands
        For example bands={'VOD_L1':['S1','S1X','S1C']}
        
    Returns
    -------
    Dictionary of case names associated with dataframes containing the output for each case
    
    """
    files = get_filelist({'':filepattern})
    # read in all data
    data = [xr.open_mfdataset(x).to_dataframe().dropna(how='all') for x in files['']]
    # concatenate
    data = pd.concat(data)
    # calculate VOD based on pairings
    out = dict()
    for icase in pairings.items():
        iref = data.xs(icase[1][0],level='Station')
        igrn = data.xs(icase[1][1],level='Station')
        idat = iref.merge(igrn,on=['Epoch','SV'],suffixes=['_ref','_grn'])
        for ivod in bands.items():
            ivars = np.intersect1d(data.columns.to_list(),ivod[1])
            for ivar in ivars:
                irefname = f"{ivar}_ref"
                igrnname = f"{ivar}_grn"
                ielename = f"Elevation_grn"
                idat[ivar] = -np.log(np.power(10,(idat[igrnname]-idat[irefname])/10)) \
                            *np.cos(np.deg2rad(90-idat[ielename]))
            
            idat[ivod[0]] = np.nan
            for ivar in ivars:
                idat[ivod[0]] = idat[ivod[0]].fillna(idat[ivar])

        idat = idat[list(bands.keys())+['Azimuth_ref','Elevation_ref']].rename(columns={'Azimuth_ref':'Azimuth','Elevation_ref':'Elevation'})
        # store result in dictionary
        out[icase[0]]=idat
    return out