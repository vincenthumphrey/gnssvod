"""
preprocess reads files and returns analysis-ready DataSet

pair_obs merges and pairs observations from sites according to specified pairing rules over the desired time intervals
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
from gnssvod.io.readFile import read_obsFile
from gnssvod.funcs.checkif import (isfloat, isint, isexist)
from gnssvod.funcs.date import doy2date
from gnssvod.position.interpolation import sp3_interp_fast
from gnssvod.position.position import gnssDataframe
from gnssvod.funcs.constants import _system_name
import pdb
#-------------------------------------------------------------------------
#----------------- FILE SELECTION AND BATCH PROCESSING -------------------
#-------------------------------------------------------------------------
def preprocess(filepattern,
               orbit=True,
               interval=None,
               keepvars=None,
               outputdir=None,
               overwrite=False,
               outputresult=False):
    """
    Returns lists of Observation objects containing GNSS observations read from RINEX observation files
    
    Parameters
    ----------
    filepattern: dictionary 
        Dictionary of station names and UNIX-style patterns to match RINEX 
        observation files. For example filepattern={'station1':'/path/to/files/of/station1/*O'}
    
    orbit: bool (optional) 
        if orbit=True, will download orbit solutions and calculate Azimuth and Elevation parameters
        if orbit=False, will not calculate additional gnss parameters
        
    interval: string or None (optional)
        if interval = None, the observations will be returned at the same rate as they were saved
        if interval = pandas Timedelta or str, this will be used to resample (average) the obervations (e.g. interval="15S")
    
    keepvars: list of strings or None (optional)
        Defines what columns are kept after processing. This can help reduce the size of the saved data.
        For example keepvars = ['S1','S2','Azimuth','Elevation']
        If None, no columns are removed
        
    outputdir: dictionary (optional)
        A dictionary of station names and folders indicating where to save the preprocessed data
        For example outputdir={'station1':'/path/where/to/save/preprocessed/data'}
        Dictionary keys must be the same as in the filepattern argument
        Data will be saved as a netcdf file, recycling the original file name
        If this argument is None, data won't be saved

    overwrite: bool (optional)
        If False (default), RINEX files with an existing matching files in the 
        specified output directory will be skipped entirely

    outputresult: bool (optional)
        If True, observation objects will also be returned as a dictionary
        
    Returns
    -------
    Dictionary of station names associated with a list of xarray Datasets containing the data from each file
    For example output={'station1':[gnssvod.io.io.Observation,gnssvod.io.io.Observation,...]}
    
    """
    # grab all files matching the patterns
    filelist = get_filelist(filepattern)
    
    out = dict()
    for item in filelist.items():
        station_name = item[0]
        filelist = item[1]

        # checking which files will be skipped (if necessary)
        if (not overwrite) and (outputdir is not None):
            # gather all files that already exist in the outputdir
            files_to_skip = get_filelist({station_name:f"{outputdir[station_name]}*.nc"})
            files_to_skip = [os.path.basename(x) for x in files_to_skip[station_name]]
        else:
            files_to_skip = []
        
        # for each file
        result = []
        for i,filename in enumerate(filelist):
            # determine the name of the output file that will be saved at the end of the loop
            out_name = os.path.splitext(os.path.basename(filename))[0]+'.nc'
            # if the name of the saved output file is in the files to skip, skip processing
            if out_name in files_to_skip:
                print(f"{out_name} already exists, skipping.. (pass overwrite=True to overwrite)")
                continue # skip remainder of loop and go directly to next filename
            
            # read in the file
            x = read_obsFile(filename)
            print(f"Processing {len(x.observation):n} individual observations")

            # only keep required vars
            if keepvars is not None:
                x.observation = subset_vars(x.observation,keepvars)
                # update the observation_types list
                x.observation_types = x.observation.columns.to_list()
                
            # resample if required
            if interval is not None:
                x = resample_obs(x,interval)
                
            # calculate Azimuth and Elevation if required
            if orbit:
                print(f"Calculating Azimuth and Elevation")
                # note: orbit cannot be parallelized easily because it 
                # downloads and unzips third-party files in the current directory
                if not 'orbit_data' in locals():
                    # if there is no previous orbit data, the orbit data is returned as well
                    x, orbit_data = add_azi_ele(x)
                else:
                    # on following iterations the orbit data is tentatively recycled to reduce computational time
                    x, orbit_data = add_azi_ele(x, orbit_data)
            
            # make sure we drop any duplicates
            x.observation=x.observation[~x.observation.index.duplicated(keep='first')]
            
            # store result in memory
            if outputresult:
                result.append(x)
                
            # write to file if required
            if outputdir is not None:
                ioutputdir = outputdir[station_name]
                # check that the output directory exists
                if not os.path.exists(ioutputdir):
                    os.makedirs(ioutputdir)
                # delete file if it exists
                out_path = os.path.join(ioutputdir,out_name)
                if os.path.exists(out_path):
                    os.remove(out_path)
                # save as NetCDF
                ds = x.observation.to_xarray()
                ds.attrs['filename'] = x.filename
                ds.attrs['observation_types'] = x.observation_types
                ds.attrs['epoch'] = x.epoch.isoformat()
                ds.attrs['approx_position'] = x.approx_position
                ds.to_netcdf(out_path)
                print(f"Saved {len(x.observation):n} individual observations in {out_name}")
                
        # store station in memory if required
        if outputresult:
            out[station_name]=result

    if outputresult:
        return out
    else:
        return

def subset_vars(df,keepvars,force_epoch_system=True):
    # subselect those of the required columns that are present 
    tokeep = np.intersect1d(keepvars,df.columns.tolist())
    # + always keep 'epoch' and 'SYSTEM' as they are required for calculating azimuth and elevation
    if force_epoch_system:
        tokeep = np.unique(keepvars+['epoch','SYSTEM'])
    else:
        tokeep = np.unique(keepvars)
    # find columns not to keep
    todrop = np.setdiff1d(df.columns.tolist(),tokeep)
    # drop unneeded columns
    if len(todrop)>0:
        df = df.drop(columns=todrop)
    # drop rows for which all of the required vars are NA
    df = df.dropna(how='all')
    return df

def resample_obs(obs,interval):
    obs.observation = obs.observation.groupby([pd.Grouper(freq=interval, level='Epoch'),pd.Grouper(level='SV')]).mean()
    obs.observation['epoch'] = obs.observation.index.get_level_values('Epoch')
    obs.observation['SYSTEM'] = _system_name(obs.observation.index.get_level_values("SV"))
    obs.interval = pd.Timedelta(interval).seconds
    return obs

def add_azi_ele(obs, orbit_data=None):
    start_time = min(obs.observation.index.get_level_values('Epoch'))
    end_time = max(obs.observation.index.get_level_values('Epoch'))
    
    if orbit_data is None:
        do = True
    elif (orbit_data.start_time<start_time) and (orbit_data.end_time>end_time) and (orbit_data.interval==obs.interval):
        # if the orbit for the day corresponding to the epoch and interval is the same as the one that was passed, just reuse it. This drastically reduces the number of times orbit files have to be read and interpolated.
        do = False
    else:
        do = True
    
    if do:
        # read (=usually download) orbit data
        orbit = sp3_interp_fast(start_time, end_time, interval=obs.interval)
        # prepare an orbit object as well
        orbit_data = orbit
        orbit_data.start_time = orbit.index.get_level_values('Epoch').min()
        orbit_data.end_time = orbit.index.get_level_values('Epoch').max()
        orbit_data.interval = obs.interval
    else:
        orbit = orbit_data
    
    # calculate the gnss parameters (including azimuth and elevation)
    gnssdf = gnssDataframe(obs,orbit,cut_off=-10)
    # add the gnss parameters to the observation dataframe
    obs.observation = obs.observation.join(gnssdf[['Azimuth','Elevation']])
    return obs, orbit_data

def get_filelist(filepatterns):
    if not isinstance(filepatterns,dict):
        raise Exception(f"Expected the input of get_filelist to be a dictionary, got a {type(filepatterns)} instead")
    filelists = dict()
    for item in filepatterns.items():
        station_name = item[0]
        search_pattern = item[1]
        flist = glob.glob(search_pattern)
        if len(flist)==0:
            warnings.warn(f"Could not find any files matching the pattern {search_pattern}")
        filelists[station_name] = flist
    return filelists


#--------------------------------------------------------------------------
#----------------- PAIRING OBSERVATION FILES FROM SITES -------------------
#-------------------------------------------------------------------------- 

def pair_obs(filepattern,pairings,timeintervals,keepvars=None,outputdir=None):
    """
    Merges observations from sites according to specified pairing rules over the desired time intervals.
    The new dataframe will contain a new index level corresponding to each site, with keys corresponding to station names.
    
    Parameters
    ----------
    filepattern: dictionary 
        Dictionary of any number station names and UNIX-style patterns to find the preprocessed NetCDF files 
        observation files. For example filepattern={'station1':'/path/to/files/of/station1/*.nc',
                                                    'station2':'/path/to/files/of/station2/*.nc'}
    
    pairings: dictionary 
        Dictionary of case names associated to a tuple of station names indicating which stations to pair
        For example pairings={'case1':('station1','station2')} will take 'station1' as the reference station.
        If data is to be saved, the case name will be taken as filename.
        
    timeintervals: pandas fixed frequency IntervalIndex
        The time interval(s) over which to pair data
        For example timeperiod=pd.interval_range(start=pd.Timestamp('1/1/2018'), periods=8, freq='D') will pair 
        data for each of the 8 days in timeperiod and return one DataSet for each day.
        
    keepvars: list of strings or None (optional)
        Defines what columns are kept after pairing is made. This helps reduce the size of the saved paired data.
        For example keepvars = ['S1','S2','Azimuth','Elevation']
        If None, no columns are removed
        
    outputdir: dictionary (optional)
        A dictionary of station names and folders indicating where to save the preprocessed data
        For example outputdir={'case1':'/path/where/to/save/paired/data'}
        Data will be saved as a netcdf file, the dictionary has to be consistent with the 'pairings' argument
        If this argument is None, data will not be saved
        
    Returns
    -------
    Dictionary of case names associated with a list of pandas dataframes containing the merged
    data for each time interval contained in the 'timeperiod' argument.
    
    """
    out=dict()
    for item in pairings.items():
        case_name = item[0]
        station_names = item[1]
        print(f'Processing {case_name}')
        # define time interval over which we will need data
        overall_interval = pd.Interval(left=timeintervals.min().left,right=timeintervals.max().right)
        print(f'Listing the files matching with the interval')
        # get all files for all stations
        filenames = get_filelist(filepattern)
        iout = []
        for station_name in station_names:
            # get Epochs from all files
            epochs = [xr.open_mfdataset(x).Epoch for x in filenames[station_name]]
            # check which files have data that overlaps with the desired time intervals
            isin = [overall_interval.overlaps(pd.Interval(left=pd.Timestamp(x.values.min()),
                                                          right=pd.Timestamp(x.values.max()))) for x in epochs]
            print(f'Found {sum(isin)} files for {station_name}')
            print(f'Reading')
            # open those files and convert them to pandas dataframes
            idata = [xr.open_mfdataset(x).to_dataframe().dropna(how='all',subset=['epoch']) \
                    for x in np.array(filenames[station_name])[isin]]
            # concatenate, drop duplicates and sort the dataframes
            idata = pd.concat(idata)
            idata = idata[~idata.index.duplicated()].sort_index(level=['Epoch','SV'])
            # add the station data in the iout list
            iout.append(idata)
        
        print(f'Concatenating')
        iout = pd.concat(iout, keys=station_names, names=['Station'])
        # only keep required vars and drop potential empty rows
        if keepvars is not None:
            iout = subset_vars(iout,keepvars,force_epoch_system=False)
        # split the dataframe into multiple dataframes according to timeintervals
        out[case_name] = [x for x in iout.groupby(pd.cut(iout.index.get_level_values('Epoch').tolist(), timeintervals))]
        
    # output the files
    if outputdir:
        for item in out.items():
            # recover list of dataframes and output directory
            case_name = item[0]
            list_of_dfs = item[1]
            ioutputdir = outputdir[case_name]
            # check that the output directory exists for that station
            if not os.path.exists(ioutputdir):
                os.makedirs(ioutputdir)
            print(f'Saving files for {case_name} in {ioutputdir}')
            for df in list_of_dfs:
                # make timestamp for filename in format yyyymmddhhmmss_yyyymmddhhmmss
                ts = f"{df[0].left.strftime('%Y%m%d%H%M%S')}_{df[0].right.strftime('%Y%m%d%H%M%S')}"
                filename = f"{case_name}_{ts}.nc"
                # convert dataframe to xarray for saving to netcdf (if df is not empty)
                if len(df[1])>0:
                    ds = df[1].to_xarray()
                    ds.to_netcdf(os.path.join(ioutputdir,filename))
                    print(f"Saved {len(df[1])} obs in {filename}")
                else:
                    print(f"No data for timestep {ts}, no file saved")
    return out

#--------------------------------------------------------------------------
#----------------- CALCULATING VOD -------------------
#-------------------------------------------------------------------------- 

def calc_vod(filepattern,pairings):
    """
    Combines a list of NetCDF files containing paired GNSS receiver data, calculates VOD and returns that data.

    The paired GNSS receiver data is typically generated with the function 'pair_obs'.
    
    VOD is calculated based on custom pairing rules indicating the input variables that need to be used.
    
    Parameters
    ----------
    filepattern: dictionary 
        Dictionary of case names and UNIX-style patterns to find the processed NetCDF files.
        For example filepattern={'case1':'/path/to/files/of/case1/*.nc',
                                 'case2':'/path/to/files/of/case2/*.nc'}
    
    pairings: dictionary 
        Dictionary of names associated to a tuple of three variables names indicating what variables to use to calculate VOD, with the reference station given first, the subcanopy station second, and the elevation third.
        For example pairings={'VOD1':('S1C_ref','S1C_grn','Elevation_grn'),
                              'VOD2':('S2C_ref','S2C_grn','Elevation_grn')}
        
    Returns
    -------
    Dictionary of case names associated with dataframes containing the output for each case
    
    """
    out=dict()
    for item in filepattern.items():
        case_name = item[0]
        print(f'Processing {case_name}')
        files = get_filelist({case_name:filepattern[case_name]})
        # read in all data
        data = [xr.open_mfdataset(x).to_dataframe().dropna(how='all') for x in files[case_name]]
        # concatenate
        data = pd.concat(data)
        # calculate VOD based on pairings
        for ivod in pairings.items():
            varname_vod = ivod[0]
            varname_ref = ivod[1][0]
            varname_grn = ivod[1][1]
            varname_ele = ivod[1][2]
            data[varname_vod] = -np.log(np.power(10,(data[varname_grn]-data[varname_ref])/10)) \
                                *np.cos(np.deg2rad(90-data[varname_ele]))
        # store result in dictionary
        out[case_name]=data

    return out