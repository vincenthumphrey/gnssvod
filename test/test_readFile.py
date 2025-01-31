from pathlib import Path
from gnssvod.io.readFile import read_obsFile, read_sp3File, read_clockFile
from gnssvod.io.io import Observation, Header
from gnssvod.funcs.filename import sp3FileName, clockFileName
import datetime
import pytest

### Test reading rinex observation files

#TODO fix function failing if folder has a dot in the name (e.g. "2.11" instead of "2_11" due to funcs/checkif.py)

@pytest.mark.parametrize(
    "path",
    [str(Path("test","rinex","2_11","Reach_Dav1_Grnd-raw_202104282106.21O")),
     str(Path("test","rinex","3_03","ReachLaeg1G_raw_20230801230811.23O")),
     str(Path("test","rinex","special_cases","MACROCOSM-2_raw_202401281751.24O"))]
)
def test_readFile(path: str) -> None:
    assert(isinstance(read_obsFile(path),Observation))
    assert(isinstance(read_obsFile(path,header=True),Header))

### Test downloading+reading SP3 and clock

test_dates = ([datetime.date(2019,1,25),datetime.date(2019,2,1),datetime.date(2019,2,8)]+ # testing around week 2038
              [datetime.date(2023,1,27),datetime.date(2023,2,3),datetime.date(2023,2,10)]+ # testing around week 2247
              [datetime.date.today() - datetime.timedelta(days=20)]) # testing 3 weeks ago

@pytest.mark.parametrize(
    "test_date",test_dates
)
def test_readSP3(test_date: datetime.date, tmp_path: Path):
    filename = sp3FileName(test_date,'gfz',dir=tmp_path)
    sp3 = read_sp3File(filename)

@pytest.mark.parametrize(
    "test_date",test_dates
)
def test_readclock(test_date: datetime.date, tmp_path: Path):
    filename = clockFileName(test_date,interval=5,product='gfz',dir=tmp_path)
    sp3 = read_clockFile(filename)