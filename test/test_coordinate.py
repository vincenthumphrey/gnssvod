import pytest
import numpy as np

from gnssvod.geodesy.coordinate import ell2cart, cart2ell

coordinate_pairs = [{'cartesian':{'x':4201197.602,
                                 'y':168347.839,
                                 'z':4780461.69},
                     'geodetic':{'lat':48.858093,
                                  'lon':2.294694,
                                  'height':360}},
                    {'cartesian':{'x':4283607.333,
                                 'y':-4023489.49,
                                 'z':-2472000.982},
                     'geodetic':{'lat':-22.950996,
                                  'lon':-43.206499,
                                  'height':713}},
                    {'cartesian':{'x':6378137.0,
                                 'y':0,
                                 'z':0},
                     'geodetic':{'lat':0,
                                  'lon':0,
                                  'height':0}},
                    {'cartesian':{'x':0,
                                 'y':-6378137.0,
                                 'z':0},
                     'geodetic':{'lat':0,
                                  'lon':-90,
                                  'height':0}},
                    {'cartesian':{'x':0,
                                 'y':0,
                                 'z':6356752.314},
                     'geodetic':{'lat':90,
                                  'lon':0,
                                  'height':0}},
                    {'cartesian':{'x':0,
                                 'y':0,
                                 'z':-6356752.314},
                     'geodetic':{'lat':-90,
                                  'lon':0,
                                  'height':0}}]

@pytest.mark.parametrize(
    'coords',
    coordinate_pairs
)
def test_cart2ell(coords: dict) -> None:
    geodetic_actual = cart2ell(coords['cartesian']['x'],
                     coords['cartesian']['y'],
                     coords['cartesian']['z'],
                     ellipsoid='GRS80')
    geodetic_expected = [coords['geodetic']['lat'],
                         coords['geodetic']['lon'],
                         coords['geodetic']['height']]
    np.testing.assert_allclose(geodetic_actual,geodetic_expected,rtol=1e-07,atol=1e-3)

@pytest.mark.parametrize(
    'coords',
    coordinate_pairs
)
def test_ell2cart(coords: dict) -> None:
    cartesian_actual = ell2cart(coords['geodetic']['lat'],
                         coords['geodetic']['lon'],
                         coords['geodetic']['height'],
                         ellipsoid='GRS80')
    cartesian_expected = [coords['cartesian']['x'],
                     coords['cartesian']['y'],
                     coords['cartesian']['z']]
    np.testing.assert_allclose(cartesian_actual,cartesian_expected,rtol=1e-07,atol=1e-3)