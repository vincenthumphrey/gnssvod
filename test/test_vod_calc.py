from gnssvod.analysis.vod_calc import calc_vod
import pandas as pd

def test_calc_vod() -> None:
    pattern = 'test/nc_paired/*.nc'
    pairings = {'Laeg':('CH-Laeg_ref','CH-Laeg_grn')}
    bands = {'VOD1':['S1','S1X','S1C'],'VOD2':['S2','S2C','S2I','S2X'],'VOD7':['S7','S7C','S7I','S7X']}
    out = calc_vod(filepattern=pattern,
                   pairings=pairings,
                   bands=bands)
    # check output is a dict
    assert(isinstance(out,dict))
    # check item is a DataFrame
    assert(isinstance(out['Laeg'],pd.DataFrame))
    # check shape of table is as expected
    assert(out['Laeg'].shape==(167110,5))
