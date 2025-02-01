"""
hemibuild is used to build an equi-angular hemispheric grid
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
import multiprocessing
import pdb
from matplotlib.patches import Rectangle, Circle
# ===========================================================
"""
Class definition for hemispheric polar grid object
"""
# ======================================================================
class Hemi:
    """
    Hemispheric grid class
    """
    def __init__(self,angular_resolution,grid,elelims,azilims,CellIDs):
        self.angular_resolution = angular_resolution
        self.ncells = len(grid)
        self.grid = grid
        self.coords = self.grid.loc[:,['azi','ele']]
        self.elelims = elelims
        self.azilims = azilims
        self.CellIDs = CellIDs

    def patches(self):
        '''
        return a series of patches
        '''
        def plotpatch(dfrow):
            azimin = np.deg2rad(dfrow.azimin)
            elemax = 90-dfrow.elemax
            azimax = np.deg2rad(dfrow.azimax)
            elemin = 90-dfrow.elemin
            return(Rectangle([azimin,elemax],azimax-azimin,elemin-elemax,fill=True))
        patches = self.grid.apply(plotpatch,axis=1)
        return(patches.rename('Patches'))
    
    def add_CellID(self,
                  df: pd.DataFrame,
                  aziname: str='Azimuth',
                  elename: str='Elevation',
                  idname: str='CellID',
                  drop: bool=True):
        '''
        return the index of the grid cell where each observation belongs
        '''
        # check that columns specified by aziname and elename exist in df
        if not aziname in df:
            raise ValueError(f"No column '{aziname}' in the dataframe, indicate which column should be used with azi='ColumnName'")
        if not aziname in df:
            raise ValueError(f"No column '{elename}' in the dataframe, indicate which column should be used with ele='ColumnName'")

        # extract a subset of the df which we can manipulate 
        idf = df.loc[:,(aziname,elename)]
        # remove all data missing azi or ele
        idf = idf[~idf.isnull().any(axis=1)]
        # use modulo to ensure all azimuths are [0-360] (not i.e. -10 or 370)
        idf[aziname] = np.mod(idf[aziname]+360*10,360)
        # first cut the data by elevation band
        idf['eleind'] = pd.cut(idf[elename],
                              bins = np.concatenate((np.flip(self.elelims),[90])),
                              labels = np.flip(list(range(len(self.elelims)))))
        
        # define a function that retrieves indices for each elevation band, cutting with azimuthal edges specific to the given band
        def azicut(idf):
            # if elevation band contains no obs, just return empty result
            if len(idf)==0:
                idf[idname] = pd.Series(dtype=int)
                return(idf[idname])
            # find out which band we are in
            iele = idf['eleind'].iloc[0]
            # retrieve the corresponding azimuthal edges
            iazilims = self.azilims[iele]
            # cut data with azimuthal edges to retrieve azimuthal index
            idf['aziind'] = pd.cut(idf[aziname], 
                                  bins = np.concatenate((iazilims,[360])),
                                  labels = False,
                                  right = False)
            # return the corresponding CellID values
            idf[idname] = self.CellIDs[iele][idf['aziind'].values]
            return(idf[idname])

        # apply the azicut function to each elevation band
        idf=idf.groupby('eleind',group_keys=False, observed=False)[['eleind',aziname]].apply(azicut) # groupby will drop rows with eleind=NaN
        if idname in df:
            df = df.drop(columns=idname)
        # if drop is True, we are returning the input df with only rows that have a CellID
        
        if drop:
            return(df.join(idf,how='inner'))
        # if drop is False, we are returning the entire input df, including NaN CellIDs
        else:
            return(df.join(idf,how='left'))
        
    
    # plot(), plot empty grid, or if passing dataframe + ID name + var name, make a join and plot the data

#-------------------------------------------------------------------------
#----------------- building hemispheric grids and meshes -------------------
#-------------------------------------------------------------------------
def hemibuild(angular_resolution,cutoff=0):
    """
    Calculates a hemispheric grid where cells have approximately equal angular size. Returns grid properties as dataframe.
    
    Parameters
    ----------
    angular_resolution: numeric
        defines the diameter of the central (zenith) cell in degrees. 
        Rings of surrounding cells are build until the cutoff elevation angle is met
    
    cutoff: numeric (default 0)
        defines the elevation angle (in degrees) at which the hemispheric grid stops
        
    Returns
    -------
    dataframe of cell IDs with edge and center coordinates

    References
    ----------
    Beckers, B., & Beckers, P. (2012). A general rule for disk and hemisphere partition into equal-area cells. Computational Geometry, 45(7), 275-283.
    
    """
    # calculate number of rings
    ringlims = np.arange(angular_resolution/2,90-cutoff,angular_resolution)
    # calculate area of a cell
    cell_area = 2*np.pi*(1-np.cos(np.deg2rad(angular_resolution/2)))

    # instantiate empty lists
    cells = []
    elelims = []
    azilims = []
    CellIDs = []
    # add first zenith cell as df
    cells.append(pd.DataFrame(data={'azi':[0],
                                    'ele':[90],
                                    'azimin':[0],
                                    'azimax':[360],
                                    'elemin':[90-angular_resolution/2],
                                    'elemax':[90]}))
    elelims.append(90-angular_resolution/2)
    azilims.append(np.array([0]))
    CellIDs.append(np.array([0]))
    nextCellID = 1
    
    # add cells, ring by ring
    for iring, outer_radius in enumerate(ringlims[1:]):
        inner_radius = ringlims[iring]
        # calculate area of ring
        ring_area = 2*np.pi*(1-np.cos(np.deg2rad(outer_radius)))-2*np.pi*(1-np.cos(np.deg2rad(inner_radius)))
        # evenly split ring according to requested cell area
        numcells = round(ring_area/cell_area)
        # span of a single cell
        azispan = 360/numcells
        # generate CellIDs
        CellID = list(range(nextCellID,nextCellID+numcells))
        # also prepare the starting CellID for the next iteration
        nextCellID = CellID[-1]+1
        # add all cells into a list of dataframes
        azimin = np.linspace(0,360.0-azispan,numcells)
        cells.append(pd.DataFrame(data={'azi':np.linspace(azispan/2,360-azispan/2,numcells),
                                    'ele':np.full(numcells,90-(inner_radius+angular_resolution/2)),
                                    'azimin':azimin,
                                    'azimax':np.concatenate((azimin[1:],[360.0])),
                                    'elemin':np.full(numcells,90-outer_radius),
                                    'elemax':np.full(numcells,90-inner_radius)}))
        elelims.append(90-outer_radius)
        azilims.append(np.array(azimin))
        CellIDs.append(np.array(CellID))
    # concatenate all cells from all rings
    cells = pd.concat(cells).reset_index(drop=True).rename_axis('CellID')
    
    # instantiate Hemi object
    return Hemi(angular_resolution,cells,np.array(elelims),azilims,CellIDs)