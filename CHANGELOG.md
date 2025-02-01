## v2025.1.2
This version focuses mainly on making the package more robust and maintainable in the long run.
### Breaking changes
- Argument `compress` in `preprocess()` and `gather_stations()` is replaced with argument `encoding`. By default `encoding` is set to `'default'` which enables compression. Compression can be skipped by passing `encoding = None` or encoding options interpretable by `xarray.DataSet.to_netcdf()` can be passed as a dictionary.
- `gather_stations()` now behaves similarly to `preprocess()` in the sense that it only returns an output if the argument `outputresult` is set to `True` (default is `False`). The returned output is now a simple dictionary of `pd.DataFrame` instead of a dictionary of lists of `pd.DataFrame`.
### New features
- In `preprocess()`, the folder where orbit files are downloaded can now be specified with the argument `aux_path`. This avoids polluting the current directory with orbit files. If `aux_path` is not provided, a temporary folder will be created automatically and deleted afterwards.
- In `preprocess()`, the position of the antenna in cartesian coordinates (X,Y,Z) can now be specified with the argument `approx_position`. This option guarantees that azimuths and elevations are always calculated from the same position, instead of relying on the (varying) APPROX_POSITION information from the individual RINEX files. To convert geographic coordinates (lat, lon, h) to cartesian (X,Y,Z) use `gnssvod.geodesy.coordinate.ell2cart(lat,lon,h)`. Providing this option also resolves an error that happened when the source RINEX file was missing the APPROX_POSITION information (or if that position was set to 0,0,0), which occurs in very brief RINEX files if the receiver was unable to determine an approximate position.
### Other changes
- Package and dependencies are now more robustly managed with `poetry`.
- The core package functions are tested with `pytest`. Further tests will be added as the package grows.
- A bunch of bugs were resolved.
- Most pandas deprecation warnings were resolved.
- Proper typing was partially added as the code was updated. 
- Exporting of NetCDF files is now handled in a dedicated function `gnssvod.io.exporters.export_as_nc()`.
- `gather_stations()` was refactored to process each time interval sequentially (instead of loading all files).
- An error in `ell2cart()` was corrected.