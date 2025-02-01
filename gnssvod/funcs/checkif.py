# ===========================================================
# ========================= imports =========================
import os
from pathlib import Path
from gnssvod.funcs.date import doy2date
from gnssvod import download
from gnssvod.doc.IGS import is_IGS
from hatanaka import decompress_on_disk
# ===========================================================


def isfloat(value):
    """ To check if any variable can be converted to float or not """
    try:
        float(value)
        return True
    except ValueError:
        return False

def isint(value):
    """ To check if any variable can be converted to integer """
    try:
        int(value)
        return True
    except ValueError:
        return False
    
def iszip(fileName):
    if fileName.lower().endswith((".z",".zip",".gz")):
        return True
    else:
        return False

def does_a_zip_exist(fileName):
    if os.path.exists(fileName + ".z"):
        return fileName+".z"
    if os.path.exists(fileName + ".Z"):
        return fileName+".Z"
    if os.path.exists(fileName + ".gz"):
        return fileName+".gz"
    if os.path.exists(fileName + ".zip"):
        return fileName+".zip"
    else:
        return False

def isexist(fileName):
    if os.path.exists(fileName) == False:
        if not does_a_zip_exist(fileName):
            # --------- case where neither a file nor a zip of the file exist ------------
            print(f"This file does not exist: {fileName}")
            # --------- a download is attempted -------------------------
            extension = Path(fileName).suffix[1:].lower()
            if extension[-1] == "o":
                if is_IGS(fileName[:4]):
                    print(fileName + ".Z does not exist in working directory | Downloading...")
                    fileEpoch = doy2date(fileName)
                    download.get_rinex([fileName[:4]], fileEpoch, Datetime = True)
                    print(" | Download completed for", fileName + ".Z", " | Extracting...")
                    decompress_on_disk(fileName + ".Z", delete=True)
                else:
                    raise Warning(fileName,"does not exist in directory and cannot be found in IGS Station list!")
            elif extension == "rnx":
                if is_IGS(fileName[:4]):
                    print(fileName + " does not exist in working directory | Downloading...")
                    fileName = fileName.split(".")[0] + ".crx"
                    fileEpoch = doy2date(fileName)
                    download.get_rinex3([fileName[:4]], fileEpoch, Datetime = True)
                    print(" | Download completed for", fileName + ".gz", " | Extracting...")
                    decompress_on_disk(fileName + ".gz", delete=True)
                else:
                    raise Warning(fileName,"does not exist in directory and cannot be found in IGS Station list!")
            elif extension == "crx":
                if is_IGS(fileName[:4]):
                    print(fileName + ".gz does not exist in working directory | Downloading...")
                    fileEpoch = doy2date(fileName)
                    download.get_rinex3([fileName[:4]], fileEpoch, Datetime = True)
                    decompress_on_disk(fileName + ".gz", delete=True)
                else:
                    raise Warning(fileName,"does not exist in directory and cannot be found in IGS Station list!")
            elif extension[-1] in {"n","p","g"}:
                if is_IGS(fileName[:4]):
                    print(fileName + ".Z does not exist in working directory | Downloading...")
                    fileEpoch = doy2date(fileName)
                    download.get_navigation([fileName[:4]], fileEpoch, Datetime = True)
                    decompress_on_disk(fileName + ".gz", delete=True)
            elif extension in {"clk","clk_05s"}:
                download.get_clock(fileName)
            elif extension == "sp3":
                download.get_sp3(fileName)
            elif extension[-1].lower() == "i":
                download.get_ionosphere(fileName)
                decompress_on_disk(fileName + ".gz", delete=True)
            else:
                raise Warning("Unknown file extension:", extension)
                
# --------- case where a zip of the required file exists but not the file itself -------
        else:
            fileName = does_a_zip_exist(fileName)
            print(fileName + " exists | Extracting...")
            decompress_on_disk(fileName, delete=True)
            
 # --------- case where the required file and also a zip of it exist -------------------
    elif does_a_zip_exist(fileName):
        # one could decide to re-extract the zip again instead
        print(fileName + " exists | Reading...")
        
 # --------- case where the required file exists and is a zip file ---------------------
    elif iszip(fileName):
        import pdb
        decompress_on_disk(fileName, delete=True)
        print(fileName + " exists | Reading...") 
        
 # --------- case where the required file exists and is not a zip file -----------------
    else:
        print(fileName + " exists | Reading...")