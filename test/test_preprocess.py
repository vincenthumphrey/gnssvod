from pathlib import Path
from gnssvod.io.preprocess import preprocess, gather_stations
from gnssvod.io.io import Observation
from gnssvod.geodesy.coordinate import cart2ell, ell2cart
import pytest
import os
import pandas as pd

@pytest.mark.parametrize(
    "filepattern, approx_position",
    [
        (str(Path("test","rinex","3_03","ReachLaeg1G_raw_20230801010733.23O")),
         None),
        (str(Path("test","rinex","3_03","*.*O")),
         None),
        (str(Path("test","rinex","special_cases","MACROCOSM-2_raw_202401281751.24O")),
         [2481150.1472, -5525646.5039, 1992267.2964])
    ]
)
def test_preprocess(filepattern: str, approx_position: list, tmp_path: Path) -> None:
    out = preprocess({'dummy_station':filepattern},
                     orbit=True,
                     interval='15s',
                     keepvars=['S?','S??'],
                     outputdir={'dummy_station':tmp_path},
                     encoding='default',
                     outputresult=True,
                     aux_path=tmp_path,
                     approx_position=approx_position)
    # check output is a dict
    assert(isinstance(out,dict))
    # check output is not an empty list
    assert(len(out['dummy_station'])>0)
    # check elements of list are Observation
    assert(all(isinstance(x,Observation) for x in out['dummy_station']))
    # check all expected output .nc files exist
    assert(all(os.path.exists(tmp_path/(Path(x.filename).stem+'.nc')) for x in out['dummy_station']))

def test_gather_stations(tmp_path: Path) -> None:
    pattern={'Laeg2_Twr':str(Path("test","nc_raw","ReachLaeg2T_*")),
         'Laeg1_Grnd':str(Path("test","nc_raw","ReachLaeg1G_*"))}
    startday = pd.to_datetime('31-07-2023',format='%d-%m-%Y')
    timeintervals=pd.interval_range(start=startday, periods=4, freq='D', closed='left')
    pairings={'Laeg':('Laeg2_Twr','Laeg1_Grnd')}
    # run function
    out = gather_stations(filepattern=pattern,
                          pairings=pairings,
                          timeintervals=timeintervals,
                          outputdir={'Laeg':tmp_path},
                          encoding='default',
                          outputresult=True)
    # check output is a dict
    assert(isinstance(out,dict))
    # check item is pd.DataFrame
    assert(isinstance(out['Laeg'],pd.DataFrame))
    # check all expected output .nc files exist
    startday = pd.to_datetime('01-08-2023',format='%d-%m-%Y')
    timeintervals=pd.interval_range(start=startday, periods=2, freq='D', closed='left')
    for interval in timeintervals:
        ts = f"{interval.left.strftime('%Y%m%d%H%M%S')}_{interval.right.strftime('%Y%m%d%H%M%S')}"
        filename = f"{'Laeg'}_{ts}.nc"
        assert(os.path.exists(tmp_path/filename))