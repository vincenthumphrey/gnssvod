from pathlib import Path
from gnssvod.io.preprocess import preprocess, gather_stations
from gnssvod.io.io import Observation
import pytest
import os
import pandas as pd

@pytest.mark.parametrize(
    "filepattern",
    [str(Path("test","rinex","3_03","ReachLaeg1G_raw_20230801010733.23O")),
     str(Path("test","rinex","3_03","*.*O"))]
)
def test_preprocess(filepattern: str, tmp_path: Path) -> None:
    out = preprocess({'dummy_station':filepattern},
                     orbit=True,
                     interval='15s',
                     keepvars=['S?','S??'],
                     outputdir={'dummy_station':tmp_path},
                     compress=True,
                     outputresult=True,
                     aux_path=tmp_path)
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
    startday = pd.to_datetime('01-08-2023',format='%d-%m-%Y')
    timeintervals=pd.interval_range(start=startday, periods=3, freq='D', closed='left')
    pairings={'Laeg':('Laeg2_Twr','Laeg1_Grnd')}
    # run function
    out = gather_stations(filepattern=pattern,
                          pairings=pairings,
                          timeintervals=timeintervals,
                          outputdir={'Laeg':tmp_path},
                          compress=True)
    # check output is a dict
    assert(isinstance(out,dict))
    # check elements of list are tuple with interval and dataframe
    assert(all(isinstance(x[0],pd.Interval) for x in out['Laeg']))
    assert(all(isinstance(x[1],pd.DataFrame) for x in out['Laeg']))
    # check all expected output .nc files exist
    assert(all(os.path.exists(tmp_path/(f"Laeg_{x[0].left.strftime('%Y%m%d%H%M%S')}_{x[0].right.strftime('%Y%m%d%H%M%S')}.nc")) for x in out['Laeg']))