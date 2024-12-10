from gnssvod.hemistats.hemistats import hemibuild, Hemi
from matplotlib.patches import Rectangle
import numpy as np
import pandas as pd
import pytest
from typing import Any

# TODO: add sanity check in hemibuild that angular_resolution is strictly positive, and not too large
# TODO: change self.elelims = elelims to a fnc of grid to ensure consistency with 'grid'?
# same with patches() and add_CellID(), make it always dependent on grid
# TODO: add cutoff as attribute

@pytest.mark.parametrize(
    ("size","cutoff"),
    [(0.8,0),
     (4,0),
     (0.8,30),
     (4,30)]
)
def test_hemistats(size: float, cutoff: float) -> None:
    hemi = hemibuild(size,cutoff)
    # check class
    assert(isinstance(hemi,Hemi))
    # check lowest cell elevation is higher than cutoff
    assert(min(hemi.grid['elemin'])>cutoff)
    # check elelims consistent with grid
    grid_elelims = np.sort(np.unique(hemi.grid['elemin']))
    np.testing.assert_allclose(grid_elelims,np.sort(hemi.elelims))
    # check azilims consistent with grid
    grid_azilims = [np.unique(x[1]['azimin']) for x in hemi.grid.loc[:,('azimin','elemin')].groupby('elemin')]
    grid_azilims.reverse()
    np.testing.assert_allclose(np.concatenate(grid_azilims),np.concatenate(hemi.azilims))
    # check ncells consistent with CellID
    assert(len(np.concatenate(hemi.CellIDs))==hemi.ncells)

    # check patch generation
    patches = hemi.patches()
    # from the rectangle's coordinates, check that area is close to the one expected from the angular resolution
    top_cap_area = 2*np.pi*(1-np.cos(np.deg2rad(hemi.angular_resolution/2)))
    def calc_area(x: Rectangle)->float:
        azimin, elemax = x.xy
        azilen = x.get_width()
        elelen = x.get_height()
        rect_area = (np.sin(np.deg2rad(90-elemax))-np.sin(np.deg2rad(90-(elemax+elelen))))*azilen
        return rect_area

    area = [calc_area(x) for x in patches]
    # area should match within 2%
    np.testing.assert_allclose(area,top_cap_area,rtol=0.02)
    # check sum of areas corresponds to represented hemisphere
    whole_cap_area = 2*np.pi*(1-np.cos(np.deg2rad(90-min(hemi.grid['elemin']))))
    np.testing.assert_allclose(sum(area),whole_cap_area)

    # test add_CellID
    df = pd.DataFrame(data={'Azimuth': [0,360,0,360,80,80],
                            'Elevation': [90,90,88,88,45,0],
                            'S1C':[40,40,40,40,40,40]})
    df = hemi.add_CellID(df)
    def verify_cellid(row: pd.Series) -> None:
        # get CellID properties
        icell = hemi.grid.loc[row["CellID"]]
        # verify Azimuth within cell
        assert(np.mod(row["Azimuth"],360)>=icell.azimin)
        assert(np.mod(row["Azimuth"],360)<=icell.azimax)
        # verify Elevation within cell
        assert(row["Elevation"]>=icell.elemin)
        assert(row["Elevation"]<=icell.elemax)
    df.apply(verify_cellid,axis=1)