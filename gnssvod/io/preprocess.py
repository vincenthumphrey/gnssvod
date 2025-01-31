"""
preprocess reads files and returns analysis-ready DataSet

gather_stations merges observations from sites according to specified pairing rules over the desired time intervals
"""
# ===========================================================
# ========================= imports =========================
import os
import glob
import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import fnmatch
from pathlib import Path
from typing import Union, Literal, Any
from gnssvod.io.io import Observation
from gnssvod.io.readFile import read_obsFile
from gnssvod.io.exporters import export_as_nc
from gnssvod.position.interpolation import sp3_interp_fast
from gnssvod.position.position import gnssDataframe
from gnssvod.funcs.constants import _system_name
#-------------------------------------------------------------------------
#----------------- FILE SELECTION AND BATCH PROCESSING -------------------
#-------------------------------------------------------------------------
def preprocess(filepattern: dict,
               orbit: bool = True,
               interval: Union[str,pd.Timedelta,None] = None,
               keepvars: Union[list,None] = None,
               outputdir: Union[dict, None] = None,
               overwrite: bool = False,
               encoding: Union[None, Literal['default'], dict] = 'default',
               outputresult: bool = False,
               aux_path: Union[str, None] = None,
               approx_position: list[float] = None) -> dict[Any,list[Observation]]:
    """
    Reads and processes structured lists of RINEX observation files.
    
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
        if interval = pandas Timedelta or str, this will be used to resample (average) the obervations (e.g. interval="15s")
    
    keepvars: list of strings or None (optional)
        Defines what columns are kept after processing. This can help reduce the size of the saved data.
        For example keepvars = ['S1','S2','Azimuth','Elevation']
        If None, no columns are removed
        
    outputdir: None, dictionary (optional)
        A dictionary of station names and folders indicating where to save the preprocessed data
        For example outputdir={'station1':'/path/where/to/save/preprocessed/data'}
        Dictionary keys must be the same as in the filepattern argument
        Data will be saved as a netcdf file, recycling the original file name
        If this argument is None, data won't be saved

    encoding: None, str, dict (optional)
        This argument is used to control compression options when saving netCDF files.
        If None is passed, no variable encodings are used when saving the file.
        If string 'default' is passed, will save all SNR, VOD, Azimuth, and Elevation data with some default encoding.
        Default encoding will be {"dtype": "int16", "scale_factor": 0.1, "zlib": True, "_FillValue":-9999}
        If a dict is passed, it should contain per-variable encodings that are passed to xr.to_netcdf(). This enables
        finer customization of the encoding.

    overwrite: bool (optional)
        If False (default), RINEX files with an existing matching file in the 
        specified output directory will be skipped entirely

    outputresult: bool (optional)
        If True and outputdir is None, observation objects will also be returned as a dictionary.

    aux_path: string or None (optional)
        If orbit is true, some external auxilliary orbit and clock files will be required and automatically downloaded.
        aux_path sets the directory where these files should be downloaded (or where they may already be found).
        If None is passed (default), a temporary directory is created and cleaned up if the processing succeeds.

    approx_position: list (optional)
        Position of the antenna provided as a list of cartesian coordinates [X,Y,Z]. This argument can be used to replace the 
        approximate position taken from the source RINEX files. 
        It is mandatory if source RINEX files actually miss the "APPROX POSITION XYZ" information in the header and the 
        'orbit' option is True. 
        To convert geographic coordinates (lat, lon, h) to cartesian (X,Y,Z) use gnssvod.geodesy.coordinate.ell2cart(lat,lon,h).

    Returns
    -------
    If outputresult = True, returns a dictionary. There is one key per station name and each item contains the GNSS observation object read 
    from each input RINEX file. For example output={'station1':[gnssvod.io.io.Observation,gnssvod.io.io.Observation,...]}
    
    """
    # set up temporary directory if necessary
    if orbit and (aux_path is None):
        tmp_folder = tempfile.TemporaryDirectory()
        aux_path = tmp_folder.name
        print(f"Created a temporary directory at {aux_path}")
    else: 
        tmp_folder = None
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
                # use a prescribed position if one was passed as argument
                if approx_position is not None:
                    x.approx_position = approx_position
                # check that an approximate position exists before proceeding
                if (x.approx_position == [0,0,0]) or (x.approx_position is None):
                    raise ValueError("Missing an approximate antenna position. Provide the argument 'approx_position' to preprocess()")
                print(f"Calculating Azimuth and Elevation")
                # note: orbit cannot be parallelized easily because it 
                # downloads and unzips third-party files in the current directory
                if not 'orbit_data' in locals():
                    # if there is no previous orbit data, the orbit data is returned as well
                    x, orbit_data = add_azi_ele(x, aux_path = aux_path)
                else:
                    # on following iterations the orbit data is tentatively recycled to reduce computational time
                    x, orbit_data = add_azi_ele(x, orbit_data, aux_path = aux_path)

            # make sure we drop any duplicates
            x.observation=x.observation[~x.observation.index.duplicated(keep='first')]
            
            # store result in memory if required
            if outputresult:
                result.append(x)
                
            # write to file if required
            if outputdir is not None:
                outpath = str(Path(outputdir[station_name],out_name))
                export_as_nc(ds = x.to_xarray(),
                             outpath = outpath,
                             encoding = encoding)
                print(f"Saved {len(x.observation):n} individual observations in {outpath}")

        # store station in memory if required
        if outputresult:
            out[station_name]=result

    # clean up temporary directory if one exists
    if tmp_folder is not None:
        tmp_folder.cleanup()
        print(f"Removed the temporary directory at {aux_path}")

    if outputresult:
        return out
    else:
        return

def subset_vars(df: pd.DataFrame,
                keepvars: list,
                force_epoch_system: bool = True) -> pd.DataFrame:
    # find all matches for all elements of keepvars
    keepvars = np.concatenate([fnmatch.filter(df.columns.tolist(),x) for x in keepvars])
    # subselect those of the required columns that are present 
    tokeep = np.intersect1d(keepvars,df.columns.tolist())
    # + always keep 'epoch' and 'SYSTEM' as they are required for calculating azimuth and elevation
    if force_epoch_system:
        tokeep = np.unique(np.concatenate((keepvars,['epoch','SYSTEM'])))
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

def resample_obs(obs: Observation, interval: str) -> Observation:
    # list all variables except SYSTEM and epoch as these are handled differently
    subset = np.setdiff1d(obs.observation.columns.to_list(),['epoch','SYSTEM'])
    # resample those variables using temporal averaging
    obs.observation = obs.observation[subset].groupby([pd.Grouper(freq=interval, level='Epoch'),pd.Grouper(level='SV')]).mean()
    # restore SYSTEM and epoch
    obs.observation['epoch'] = obs.observation.index.get_level_values('Epoch')
    obs.observation['SYSTEM'] = _system_name(obs.observation.index.get_level_values("SV"))
    obs.interval = pd.Timedelta(interval).seconds
    return obs

def add_azi_ele(obs: Observation, 
                orbit_data: Union[pd.DataFrame,None] = None, 
                aux_path: Union[str,None] = None) -> tuple[Observation,pd.DataFrame]:
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
        orbit = sp3_interp_fast(start_time, end_time, interval=obs.interval, aux_path=aux_path)
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
    # drop variables 'epoch' and 'SYSTEM' as they are not needed anymore by gnssDataframe
    obs.observation = obs.observation.drop(columns=['epoch','SYSTEM'])
    # update the observation_types list
    obs.observation_types = obs.observation.columns.to_list()
    return obs, orbit_data

def get_filelist(filepatterns: dict) -> dict:
    if not isinstance(filepatterns,dict):
        raise Exception(f"Expected the input of get_filelist to be a dictionary, got a {type(filepatterns)} instead")
    filelists = dict()
    for item in filepatterns.items():
        station_name = item[0]
        search_pattern = item[1]
        flist = glob.glob(search_pattern)
        if len(flist)==0:
            print(f"Could not find any files matching the pattern {search_pattern}")
        filelists[station_name] = flist
    return filelists


#--------------------------------------------------------------------------
#----------------- PAIRING OBSERVATION FILES FROM SITES -------------------
#-------------------------------------------------------------------------- 

def gather_stations(filepattern: dict,
                    pairings: dict,
                    timeintervals: Union[pd.IntervalIndex,None] = None,
                    keepvars: Union[list,None] = None,
                    outputdir: Union[dict, None] = None,
                    encoding: Union[None, Literal['default'], dict] = None,
                    outputresult: bool = False) -> dict[Any,pd.DataFrame]:
    """
    Merges observations from different sites according to specified pairing rules. The new dataframe will contain 
    a new index level corresponding to each site, with keys corresponding to station names.
    
    Parameters
    ----------
    filepattern: dictionary 
        Dictionary of any number station names and UNIX-style patterns to find the preprocessed NetCDF files 
        observation files. For example filepattern={'station1':'/path/to/files/of/station1/*.nc',
                                                    'station2':'/path/to/files/of/station2/*.nc'}
    
    pairings: dictionary 
        Dictionary of case names associated to a tuple of station names indicating which stations to gather
        For example pairings={'case1':('station1','station2')}
        If data is to be saved, the case name will be taken as filename.
        
    timeintervals: None, pd.IntervalIndex (optional)
        The time interval(s) over which to sequentially gather data.
        For example timeperiod=pd.interval_range(start=pd.Timestamp('1/1/2018'), periods=8, freq='D') will gather 
        data for each of the 8 days in timeperiod. Adequate sequential processing will avoid that the script attempts
        to load and pair too much data at once.
        If outputdir is not None, the frequency also defines how data is saved (here as daily files).
        If None, all files that can be found will be used.
        
    keepvars: list of strings or None (optional)
        Defines what columns are kept after the gathering is made. This helps reduce the size of the data when saved.
        For example keepvars = ['S1','S2','Azimuth','Elevation']
        If None, no columns are removed
        
    outputdir: dictionary (optional)
        A dictionary of station names and folders indicating where to save the gathered data
        For example outputdir={'case1':'/path/where/to/save/data'}
        Data will be saved as a netcdf file, the dictionary has to be consistent with the 'pairings' argument
        If this argument is None, data will not be saved

    encoding: None, str, dict (optional)
        This argument is used to control compression options when saving netCDF files.
        If None is passed, no variable encodings are used when saving the file.
        If string 'default' is passed, will save all SNR, Azimuth, and Elevation data with some default encoding.
        Default encoding will be {"dtype": "int16", "scale_factor": 0.1, "zlib": True, "_FillValue":-9999}
        If a dict is passed, it should contain per-variable encodings that are passed to xr.to_netcdf(). This enables
        finer customization of the encoding.

    outputresult: bool (optional)
        If True, observation objects will also be returned as a dictionary
        
    Returns
    -------
    If outputresult = True, returns a dictionary. There is one key per case, and the corresponding item is a
    pd.Dataframe containing the paired data.
    
    """
    # get all files for all stations
    filenames = get_filelist(filepattern)
    print(f'Extracting Epochs from files')
    # extract only Epoch timestamps from all files (should be fast enough)
    epochs = {key:[xr.open_mfdataset(x)['Epoch'].values for x in items] for key,items in filenames.items()}
    # get min and max timestamp for each file (will be used to select which files to read later)
    epochs_min = {key:[np.min(x) for x in items] for key,items in epochs.items()}
    epochs_max = {key:[np.max(x) for x in items] for key,items in epochs.items()}
    
    result=dict()
    for case_name, station_names in pairings.items():
        out = []
        print(f'----- Processing {case_name}')
        if timeintervals is None:
            timeintervals = pd.interval_range(start=epochs_min, end=epochs_max)
        for interval in timeintervals:
            print(f'-- Processing interval {interval}')
            iout = []
            # gather all data required for that interval
            for station_name in station_names:
                # check which files have data that overlaps with the desired time intervals
                isin = [interval.overlaps(pd.Interval(left=pd.Timestamp(tmin),
                    right=pd.Timestamp(tmax))) for tmin,tmax in zip(epochs_min[station_name],epochs_max[station_name])]
                print(f'Found {sum(isin)} file(s) for {station_name}')
                if sum(isin)>0:
                    print(f'Reading')
                    # open those files and convert them to pandas dataframes
                    idata = [xr.open_mfdataset(x).to_dataframe().dropna(how='all') \
                            for x in np.array(filenames[station_name])[isin]]
                    # concatenate
                    idata = pd.concat(idata)
                    # keep only data falling within the interval
                    idata = idata.loc[[x in interval for x in idata.index.get_level_values('Epoch')]]
                    # drop duplicates and sort the dataframes
                    idata = idata[~idata.index.duplicated()].sort_index(level=['Epoch','SV'])
                    # add the station data in the iout list
                    iout.append(idata)
                else:
                    iout.append(pd.DataFrame())
                    print(f"No data for station {station_name}.")
                    continue

            if not all([x.empty for x in iout]):
                print(f'Concatenating stations')
                iout = pd.concat(iout, keys=station_names, names=['Station'])

                # only keep required vars and drop potential empty rows
                if keepvars is not None:
                    iout = subset_vars(iout,keepvars,force_epoch_system=False)
                    if len(iout)==0:
                        print(f"No observations left after subsetting columns (argument 'keepvars')")
                        continue
                
                # output the data as .nc if required
                if outputdir:
                    ioutputdir = outputdir[case_name]
                    print(f'Saving result in {ioutputdir}')
                    # make destination path
                    ts = f"{interval.left.strftime('%Y%m%d%H%M%S')}_{interval.right.strftime('%Y%m%d%H%M%S')}"
                    filename = f"{case_name}_{ts}.nc"
                    outpath = str(Path(ioutputdir,filename))
                    # sort dimensions
                    ds = iout.to_xarray()
                    ds = ds.sortby(['Epoch','SV','Station'])
                    # write nc file
                    export_as_nc(ds = ds,
                        outpath = outpath,
                        encoding = encoding)
                    print(f"Saved {len(iout)} observations in {filename}")

                # add interval in memory if required
                if outputresult:
                    out.append(iout)
            else:
                print(f"No data at all for that interval, skipping..")

        # store case in memory if required
        if outputresult and len(out)>0:
            result[case_name] = pd.concat(out)

    return result