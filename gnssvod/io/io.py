"""
Class definitions for I/O opreations
"""
# ===========================================================
# ========================= imports =========================
import xarray as xr

# ======================================================================
class Observation:
    """
    Observations class for RINEX Observation (*.*o) files
    """
    def __init__(self, filename=None, epoch=None, observation=None, approx_position=None,
                 receiver_type=None, antenna_type=None, interval=None,
                 receiver_clock=None, version=None, observation_types=None):
        self.filename          = filename 
        self.epoch             = epoch 
        self.observation       = observation
        self.approx_position   = approx_position
        self.receiver_type     = receiver_type
        self.antenna_type      = antenna_type
        self.interval          = interval
        self.receiver_clock    = receiver_clock
        self.version           = version
        self.observation_types = observation_types

    def to_xarray(self) -> xr.Dataset:
        ds = self.observation.to_xarray()
        ds = ds.assign_attrs({'filename' : self.filename,
                            'observation_types' : self.observation_types,
                            'epoch' : self.epoch.isoformat(),
                            'approx_position' : self.approx_position})
        return ds

class _ObservationTypes:
    def __init__(self, ToB_GPS=None, ToB_GLONASS=None, ToB_GALILEO=None,
                 ToB_COMPASS=None, ToB_QZSS=None, ToB_IRSS=None, ToB_SBAS=None):
        self.GPS     = ToB_GPS
        self.GLONASS = ToB_GLONASS
        self.GALILEO = ToB_GALILEO
        self.COMPASS = ToB_COMPASS
        self.QZSS    = ToB_QZSS
        self.IRSS    = ToB_IRSS
        self.SBAS    = ToB_SBAS
# ======================================================================

# ======================================================================
class Header:
    """
    Header class for RINEX Observation (*.*o) files
    """
    def __init__(self, filename=None, approx_position=None, receiver_type=None, 
                 antenna_type=None, start_date=None, end_date=None,
                 version=None, observation_types=None):
        self.filename          = filename
        self.approx_position   = approx_position
        self.receiver_type     = receiver_type
        self.antenna_type      = antenna_type
        self.start_date        = start_date
        self.end_date          = end_date
        self.version           = version
        self.observation_types = observation_types
# ======================================================================

# ======================================================================
class Navigation:
    """
    Navigation class for RINEX Observation (*.*n/p) files
    """
    def __init__(self, epoch = None, navigation = None, version = None):
        self.epoch           = epoch
        self.navigation      = navigation
        self.version         = version
# ======================================================================

# ======================================================================
class PEphemeris:
    """
    Class definition for SP3 file (Precise Ephemeris)
    """
    def __init__(self, epoch=None, ephemeris=None):
        self.epoch = epoch
        self.ephemeris = ephemeris
