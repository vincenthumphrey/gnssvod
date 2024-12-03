from pathlib import Path
from gnssvod.io.readFile import read_obsFile
from gnssvod.io.io import Observation, Header

import pytest

#TODO fix test failing if folder has a dot in the name (e.g. "2.11" instead of "2_11" due to funcs/checkif.py)

@pytest.mark.parametrize(
    "path",
    [str(Path("test","rinex","2_11","Reach_Dav1_Grnd-raw_202104282106.21O")),
     str(Path("test","rinex","3_03","ReachLaeg1G_raw_20230801230811.23O"))]
)
def test_readFile(path: str) -> None:
    assert(isinstance(read_obsFile(path),Observation))
    assert(isinstance(read_obsFile(path,header=True),Header))
