"""
GPS

"""
# ===========================================================
# ========================= imports =========================
import os
import http.client
import urllib.request as url
from pathlib import Path
import datetime
from hatanaka import decompress_on_disk
from dateutil.relativedelta import relativedelta
from gnssvod.funcs.filename import (obsFileName,
                                navFileName, nav3FileName, 
                                obs3FileName)
from gnssvod.funcs.date import (datetime2doy,gpsweekday)
from tqdm import tqdm
# ===========================================================

__all__ = ["get_rinex", "get_rinex3", "get_navigation", "get_clock", "get_sp3", "get_ionosphere"]

server_root = 'ftp://gssc.esa.int/gnss'

def check_internet():
    """ To check if there is an internet connection for FTP downloads """
    connection = http.client.HTTPConnection("www.google.com", timeout=5)
    try:
        connection.request("HEAD", "/")
        connection.close()
        return True
    except:
        connection.close()
        return False

def get_rinex(stationList, date_start, date_finish=None, period='day', Datetime=False, directory=os.getcwd()):
    """
    This function downloads IGS rinex observation file from NASA CDDIS ftp server.
    
    Usage: 
        get_rinex(['mate'],'02-01-2017')
        get_rinex(['mate', 'onsa'],'01-01-2017')
        get_rinex(['mate'], date_start = '01-01-2017', date_finish = '05-01-2017', period = 'day')
        get_rinex(['mate'], date_start = '01-01-2017', date_finish = '01-06-2017', period = 'month')
        get_rinex(['mate', 'onsa'], date_start = '01-01-2017', date_finish = '03-01-2017', period = 'month')
        get_rinex(['mate'], date_start = '01-01-2017', date_finish = '01-01-2018', period = 'year')
    """
    internet = check_internet()
    if internet == False:
        raise Warning('No internet connection! | Cannot download RINEX file...')
    
    if Datetime == False:
        date_start = datetime.date(year = int(date_start[-4:]), month = int(date_start[-7:-5]), day = int(date_start[-10:-8]))
        if date_finish != None:
            date_finish = datetime.date(year = int(date_finish[-4:]), month = int(date_finish[-7:-5]), day = int(date_finish[-10:-8]))
    
    timedelta = {'day'   : relativedelta(days   = 1),
                 'month' : relativedelta(months = 1),
                 'year'  : relativedelta(years  = 1)}[period]
    dateList = [date_start] # dates of observation files
    if date_finish != None:
        while dateList[-1] != date_finish:
            date = dateList[-1] + timedelta
            dateList.append(date)

    obsFileDir = 'data/daily' # observation file directory in ftp server
    
    for stationName in stationList:
        for date in dateList:
            doy = datetime2doy(date, string = True)
            fileName = obsFileName(stationName, date, zipped = True)
            # check if the file already exist in the directory
            if os.path.exists(fileName)  == True:
                if os.path.exists(fileName[:-2])  == True:
                    print(fileName[:-2] + " exists in working directory")
                    continue
                else:
                    print(fileName + " exists in working directory | Extracting...")
                    Archive(fileName).extractall(os.getcwd())
                    continue
            file_topath = os.path.join(directory, fileName)
            fileDir = [server_root, obsFileDir, str(date.year), doy, str(date.year)[-2:] + 'o', fileName] # file directory
            ftp = '/'.join(fileDir)
            # Download the file
            try:
                print('Downloading:', fileName, end= '')
                url.urlretrieve(ftp, file_topath)
                print(" | Download completed for", fileName, " | Extracting...")
                Archive(fileName).extractall(os.getcwd())
            except:
                raise Warning("Requested file", fileName, "cannot be not found!")

def get_navigation(stationList, date_start, date_finish=None, period='day', Datetime=False, directory=os.getcwd()):
    """
    This function downloads mutli-gnss navigation file (.p) from NASA CDDIS ftp server.
    
    Usage: 
        get_navigation(['mate'],'02-01-2017')
        get_navigation(['mate', 'onsa'],'01-01-2017')
        get_navigation(['mate'], date_start = '01-01-2017', date_finish = '05-01-2017', period = 'day')
        get_navigation(['mate'], date_start = '01-01-2017', date_finish = '01-06-2017', period = 'month')
        get_navigation(['mate', 'onsa'], date_start = '01-01-2017', date_finish = '03-01-2017', period = 'month')
        get_navigation(['mate'], date_start = '01-01-2017', date_finish = '01-01-2018', period = 'year')
    """

    internet = check_internet()
    if internet == False:
        raise Warning('No internet connection! | Cannot download rinex file')
    
    if Datetime == False:
        date_start = datetime.date(year = int(date_start[-4:]), month = int(date_start[-7:-5]), day = int(date_start[-10:-8]))
        if date_finish != None:
            date_finish = datetime.date(year = int(date_finish[-4:]), month = int(date_finish[-7:-5]), day = int(date_finish[-10:-8]))
    
    timedelta = {'day'   : relativedelta(days   = 1),
                 'month' : relativedelta(months = 1),
                 'year'  : relativedelta(years  = 1)}[period]
    dateList = [date_start]
    if date_finish != None:
        while dateList[-1] != date_finish:
            date = dateList[-1] + timedelta
            dateList.append(date)

    obsFileDir = 'data/daily'
    
    for stationName in stationList:
        for date in dateList:
            if date >= datetime.date(year=2016,month=1,day=1):
                print("Downloading RINEX3 navigation file...")
                doy = datetime2doy(date, string = True)
                fileName = nav3FileName(stationName, date, zipped = True)
                if os.path.exists(fileName)  == True:
                    if os.path.exists(fileName[:-2])  == True:
                        print(fileName[:-2] + " exists in working directory")
                        continue
                    else:
                        print(fileName + " exists in working directory | Extracting...")
                        Archive(fileName).extractall(os.getcwd())
                        continue
                file_topath = os.path.join(directory, fileName)
                fileDir = [server_root, obsFileDir, str(date.year), doy, str(date.year)[-2:] + 'p', fileName]
                ftp = '/'.join(fileDir)
                try:
                    print('Downloading:', fileName, end= '')
                    url.urlretrieve(ftp, file_topath)
                    print(" | Download completed for", fileName, " | Extracting...")
                    Archive(fileName).extractall(os.getcwd())
                except:
                    print("| Requested navigation file", fileName, "cannot be not found! | Checking for IGS Navigation File..." )
                    try:
                        igsFileName = nav3FileName("BRDC", date, zipped = True)
                        
                        if os.path.exists(igsFileName)  == True:
                            if os.path.exists(igsFileName[:-3])  == True:
                                print(igsFileName[:-3] + " exists in working directory")
                                continue
                            else:
                                print(igsFileName + " exists in working directory | Extracting...")
                                Archive(igsFileName).extractall(os.getcwd())
                                continue
                        file_topath = os.path.join(directory, igsFileName)
                        fileDir = [server_root, obsFileDir, str(date.year), doy, str(date.year)[-2:] + 'p', igsFileName]
                        ftp = '/'.join(fileDir) 
                        print('Downloading:', igsFileName, end= '')
                        url.urlretrieve(ftp, file_topath)
                        print(" | Download completed for", igsFileName, " | Extracting...")
                        Archive(igsFileName).extractall(os.getcwd())
                    except:
                        raise Warning("IGS Navigation File", igsFileName, "cannot be not found!")
            else:
                print("Downloading RINEX2 navigation file...")
                doy = datetime2doy(date, string = True)
                fileName = navFileName(stationName, date, zipped = True)
                if os.path.exists(fileName)  == True:
                    if os.path.exists(fileName[:-2])  == True:
                        print(fileName[:-2] + " exists in working directory")
                        continue
                    else:
                        print(fileName + " exists in working directory | Extracting...")
                        Archive(fileName).extractall(os.getcwd())
                        continue
                file_topath = os.path.join(directory, fileName)
                fileDir = [server_root, obsFileDir, str(date.year), doy, str(date.year)[-2:] + 'n', fileName]
                ftp = '/'.join(fileDir) # FTP link of file
                try:
                    print('Downloading:', fileName, end= '')
                    url.urlretrieve(ftp, file_topath)
                    print(" | Download completed for", fileName, " | Extracting...")
                    Archive(fileName).extractall(os.getcwd())
                except:
                    print("| Requested navigation file", fileName, "cannot be not found! | Checking for IGS Navigation File..." )
                    try:
                        igsFileName = navFileName("brdc", date, zipped = True)
                        if os.path.exists(igsFileName)  == True:
                            if os.path.exists(igsFileName[:-2])  == True:
                                print(fileName[:-2] + " exists in working directory")
                                continue
                            else:
                                print(igsFileName + " exists in working directory | Extracting...")
                                Archive(igsFileName).extractall(os.getcwd())
                                continue
                        file_topath = os.path.join(directory, igsFileName)
                        fileDir = [server_root, obsFileDir, str(date.year), doy, str(date.year)[-2:] + 'n', igsFileName]
                        ftp = '/'.join(fileDir)
                        print('Downloading:', igsFileName, end= '')
                        url.urlretrieve(ftp, file_topath)
                        print(" | Download completed for", igsFileName, " | Extracting...")
                        Archive(igsFileName).extractall(os.getcwd())
                    except:
                        raise Warning("IGS Navigation File", igsFileName, "cannot be not found!")


def get_rinex3(stationList, date_start, date_finish=None, period='day', Datetime=False, directory=os.getcwd()):
    """
    This function downloads IGS rinex observation file from NASA CDDIS ftp server.
    
    Usage: 
        get_rinex(['mate'],'02-01-2017')
        get_rinex(['mate', 'onsa'],'01-01-2017')
        get_rinex(['mate'], date_start = '01-01-2017', date_finish = '05-01-2017', period = 'day')
        get_rinex(['mate'], date_start = '01-01-2017', date_finish = '01-06-2017', period = 'month')
        get_rinex(['mate', 'onsa'], date_start = '01-01-2017', date_finish = '03-01-2017', period = 'month')
        get_rinex(['mate'], date_start = '01-01-2017', date_finish = '01-01-2018', period = 'year')
    """
    internet = check_internet()
    if internet == False:
        raise Warning('No internet connection! | Cannot download RINEX file...')
    
    if Datetime == False:
        date_start = datetime.date(year = int(date_start[-4:]), month = int(date_start[-7:-5]), day = int(date_start[-10:-8]))
        if date_finish != None:
            date_finish = datetime.date(year = int(date_finish[-4:]), month = int(date_finish[-7:-5]), day = int(date_finish[-10:-8]))
    
    timedelta = {'day'   : relativedelta(days   = 1),
                 'month' : relativedelta(months = 1),
                 'year'  : relativedelta(years  = 1)}[period]
    dateList = [date_start]
    if date_finish != None:
        while dateList[-1] != date_finish:
            date = dateList[-1] + timedelta
            dateList.append(date)

    obsFileDir = 'data/daily' # observation file directory in ftp server
    
    for stationName in stationList:
        for date in dateList:
            doy = datetime2doy(date, string = True)
            fileName = obs3FileName(stationName, date, zipped = True)
            # check if the file already exist in the directory
            if os.path.exists(fileName)  == True:
                if os.path.exists(fileName[:-2])  == True:
                    print(fileName[:-2] + " exists in working directory")
                    continue
                else:
                    print(fileName + " exists in working directory | Extracting...")
                    Archive(fileName).extractall(os.getcwd())
                    continue
            file_topath = os.path.join(directory, fileName)
            fileDir = [server_root, obsFileDir, str(date.year), doy, str(date.year)[-2:] + 'd', fileName] 
            ftp = '/'.join(fileDir) 
            try:
                print('Downloading:', fileName, end= '')
                with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=fileName) as t:
                    url.urlretrieve(ftp, file_topath, reporthook=t.update_to)
                print(" | Download completed for", fileName, " | Extracting...")
                Archive(fileName).extractall(os.getcwd())
            except:
                raise Warning("Requested file", fileName, "cannot be not found!")


def get_sp3(sp3_path: str) -> None:
    """
    Given the path of a GFZ orbit file, this function will make sure that orbit file
    is at that location.

    The function will 
    1) do nothing if the file already exists
    2) if a zipped version of the file exists, it will unzip it in the same folder
    3) if no file or zipped file exists, it will download it and unzip it
    """
    sp3path = Path(sp3_path)
    if sp3path.suffix=='.sp3':
        gpsWeek = sp3path.name[3:-5]
        zipped_path = Path(sp3path.parent,sp3path.name + ".Z")
    elif sp3path.suffix=='.SP3':
        sp3Date = datetime.datetime.strptime(sp3path.name.split('_')[1],'%Y%j%H%M').date()
        gpsWeek, gpsWeekday = gpsweekday(sp3Date, Datetime = True)
        gpsWeek = str(gpsWeek)
        zipped_path = Path(sp3path.parent,sp3path.name + ".gz")
    else:
        raise Warning("sp3 filename must either end in .sp3 (gpsWeek < 2238) or .SP3 (gpsWeek >= 2238)")
        sys.exit("Exiting...")
    
    # if the file exists, nothing else to do
    if sp3path.exists():
        print(f"{sp3path.name} exists in {sp3path.parent}")
        return
    # if a zip of the file already exists, unzip it, then leave
    if zipped_path.exists():
        print(f"{zipped_path.name} exists in {zipped_path.parent} | Extracting...")
        decompress_on_disk(zipped_path, delete=True)
        return
    
    # if this is reached, we proceed to downloading
    internet = check_internet()
    if internet == False:
        raise Warning('No internet connection! | Cannot download orbit file')
    
    # define remote path
    sp3FileDir = 'products'
    if int(gpsWeek)<2038:
        intermediateDir = ''
    elif int(gpsWeek)<2247: 
        intermediateDir = 'mgex'
    else:
        intermediateDir = ''
    fileDir = [server_root, sp3FileDir, gpsWeek, intermediateDir, zipped_path.name]
    ftp = '/'.join(fileDir) # FTP link of file
    
    # attempt download
    try:
        print('Downloading:', zipped_path.name, end = '')
        with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=zipped_path.name) as t:
            url.urlretrieve(ftp, zipped_path, reporthook=t.update_to)
        print(' | Download completed for', zipped_path.name)
        decompress_on_disk(zipped_path, delete=True)
    except:
        print(" | Requested file", zipped_path.name, "cannot be not found!")

    return
    
def get_clock(clock_path):
    """
    Given the path of a GFZ clock file, this function will make sure that clock file
    is at that location.

    The function will 
    1) do nothing if the file already exists
    2) if a zipped version of the file exists, it will unzip it in the same folder
    3) if no file or zipped file exists, it will download it and unzip it
    """
    clockpath = Path(clock_path)
    if clockpath.suffix=='.clk':
        gpsWeek = clockpath.name[3:-7]
        zipped_path = Path(clockpath.parent,clockpath.name + ".Z")
    elif clockpath.suffix=='.clk_05s':
        gpsWeek = clockpath.name[3:-9]
        zipped_path = Path(clockpath.parent,clockpath.name + ".Z")
    elif clockpath.suffix=='.CLK':
        clockDate = datetime.datetime.strptime(clockpath.name.split('_')[1],'%Y%j%H%M').date()
        gpsWeek, gpsWeekday = gpsweekday(clockDate, Datetime = True)
        gpsWeek = str(gpsWeek)
        zipped_path = Path(clockpath.parent,clockpath.name + ".gz")
    else:
        raise Warning("clock filename must either end in .clk (gpsWeek < 2238) or .CLK (gpsWeek >= 2238)")
        sys.exit("Exiting...")
        
    # if the file exists, nothing else to do
    if clockpath.exists():
        print(f"{clockpath.name} exists in {clockpath.parent}")
        return
    # if a zip of the file already exists, unzip it, then leave
    if zipped_path.exists():
        print(f"{zipped_path.name} exists in {zipped_path.parent} | Extracting...")
        decompress_on_disk(zipped_path, delete=True)
        return
    
    # if this is reached, we proceed to downloading
    internet = check_internet()
    if internet == False:
        raise Warning('No internet connection! | Cannot download clock file')
    
    # define remote path
    clockFileDir = 'products'
    if int(gpsWeek)<2038:
        intermediateDir = ''
    elif int(gpsWeek)<2247:
        intermediateDir = 'mgex'
    else:
        intermediateDir = ''
    fileDir = [server_root, clockFileDir, gpsWeek, intermediateDir, zipped_path.name] 
    ftp = '/'.join(fileDir)

    try:
        print('Downloading:', zipped_path.name, end = '')
        with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=zipped_path.name) as t:
            url.urlretrieve(ftp, zipped_path, reporthook=t.update_to)
        print(' | Download completed for', zipped_path.name)
        decompress_on_disk(zipped_path, delete=True)
        return
    except:
        print("Requested file", zipped_path.name, "cannot be not found in ftp server")
        # try alternative name
        altname = "gfz" + zipped_path.name[3:]
        fileDir = [server_root, clockFileDir, altname[3:7], altname] 
        ftp = '/'.join(fileDir)
        try:
            print("Looking for GFZ clock file in ftp server...")
            print('Downloading:', altname, end = '')
            with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=zipped_path.name) as t:
                url.urlretrieve(ftp, zipped_path, reporthook=t.update_to)
            print(' | Download completed for', zipped_path.name)
            decompress_on_disk(zipped_path, delete=True)
        except:
            raise Warning("Requested file", zipped_path.name, "cannot be not found in FTP server | Exiting")

def get_ionosphere(ionFile, directory=os.getcwd()):
    """
    This function downloads Ionosphere file from NASA CDDIS ftp server.
    
    Usage: 
    
    """
    internet = check_internet()
    if internet == False:
        raise Warning('No internet connection! | Cannot download ionosphere file')
    
    fileName = ionFile + ".Z"
    year = int(ionFile[-3:-1])
    if 79 < year < 100:
        year += 1900
    elif year <= 79:
        year += 2000
    # FTP download
    ionFileDir = 'products/ionex'
    file_topath = os.path.join(directory, fileName)
    fileDir = [server_root, ionFileDir, str(year), fileName[4:7], fileName] 
    ftp = '/'.join(fileDir)

    try:
        print('Downloading:', fileName, end = '')
        with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=fileName) as t:
            url.urlretrieve(ftp, file_topath, reporthook=t.update_to)
        print(' | Download completed for', fileName)
        return fileName[:-2]
    except:
        print("Requested file", fileName, "cannot be not found in FTP server")
        fileName = "igs" + ionFile[3:] + ".Z"
        file_topath = os.path.join(directory, fileName)
        fileDir = [server_root, ionFileDir, fileName[3:7], fileName]
        ftp = '/'.join(fileDir)
        try:
            print("Looking for ionosphere file in FTP server...")
            print('Downloading:', fileName, end = '')
            with TqdmUpTo(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=fileName) as t:
                url.urlretrieve(ftp, file_topath, reporthook=t.update_to)
            print(' | Download completed for', fileName)
            return fileName[:-2]
        except:
            raise Warning("Requested file", fileName, "cannot be not found in FTP server | Exiting")


class TqdmUpTo(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        return self.update(b * bsize - self.n)
