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
# ===========================================================

#-------------------------------------------------------------------------
#----------------- FILE SELECTION AND BATCH PROCESSING -------------------
#-------------------------------------------------------------------------
def hemibuild(nrings=10, ringlims=(0,90)):
    """
    Returns a hemispheric grid object where cells have approximately equal angular size
    
    Parameters
    ----------
    nrings: integer
        defines in how many rings along the elevation direction the hemisphere should be divided 
        For example, nrings = 9 will create 9 concentric rings and split them into cells of 
        approximately equal size
    
    ringlims: list, array or tuple of 2 numerics (optional)
        defines the upper and lower elevation for the hemispheric domain
        For example, ringlims = (0,90) to cover the whole hemisphere
    
    Returns
    -------


    References
    ----------
    Beckers, B., & Beckers, P. (2012). A general rule for disk and hemisphere partition into equal-area cells. Computational Geometry, 45(7), 275-283.
    
    """
    
    
    return