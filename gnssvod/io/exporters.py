"""
export_as_nc()
export_output()
"""
# ===========================================================
# ========================= imports =========================
import numpy as np
import xarray as xr
from fnmatch import fnmatch
from typing import Union, Literal
from pathlib import Path
# ===========================================================
def export_as_nc(ds: xr.Dataset, outpath: str, encoding: Union[None, Literal['default'], dict] = None):
    outdir = Path(outpath).parent
    outname = Path(outpath)
    # check that the output directory exists
    if not outdir.exists():
        outdir.mkdir()
    # delete file if it exists
    if outname.exists():
        outname.unlink()
    # save as NetCDF
    if encoding is not None:
        if isinstance(encoding, str):
            encoding = get_default_encodings(ds, encoding)
        ds.to_netcdf(outpath,encoding=encoding)
    else:
        ds.to_netcdf(outpath)

def get_default_encodings(ds: xr.Dataset, encoding: Literal['default']) -> dict:
    if encoding == 'default':
        enc_1decimal = {"dtype": "int16", "scale_factor": 0.1, "zlib": True, "_FillValue":-9999}
        to_compress_1decimal = [fnmatch(x,'S??') | 
                    fnmatch(x,'S?') | 
                    fnmatch(x,'Azimuth') | 
                    fnmatch(x,'Elevation') for x in list(ds.keys())]
        encoding_1decimal = {x:enc_1decimal for x in np.array(list(ds.keys()))[to_compress_1decimal]}
        enc_2decimal = {"dtype": "int16", "scale_factor": 0.2, "zlib": True, "_FillValue":-9999}
        to_compress_2decimal = [fnmatch(x,'VOD*') for x in list(ds.keys())]
        encoding_2decimal = {x:enc_2decimal for x in np.array(list(ds.keys()))[to_compress_2decimal]}
        encoding = encoding_1decimal | encoding_2decimal
    else:
        raise ValueError("Unexpected input value for encoding, expected 'default'")
    return encoding