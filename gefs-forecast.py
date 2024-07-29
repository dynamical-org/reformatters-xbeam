import functools
import tempfile

import apache_beam as beam
import cfgrib
import dask
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import requests.adapters
import xarray as xr
import xarray_beam as xbeam
import zarr

# Not publicly exported but still useful for io bound tasks. See module docstring.
XbeamThreadMap = xbeam._src.threadmap.ThreadMap


def create_pipeline(start_time: pd.Timestamp, end_time: pd.Timestamp):
    template_ds, target_chunks = make_template_dataset(start_time, end_time)

    with beam.Pipeline() as p:
        (
            p
            | beam.Create(source_file_keys(template_ds))
            | XbeamThreadMap(
                lambda key: download_and_load_source_file(key, template_ds)
            )
            | xbeam.ConsolidateChunks(
                {dim: target_chunks[dim] for dim in ["init_time", "lead_time"]}
            )
            | xbeam.SplitChunks(
                {dim: target_chunks[dim] for dim in ["latitude", "longitude"]}
            )
            | xbeam.ChunksToZarr("data/test9.zarr", template_ds)
        )


def make_template_dataset(
    start_time: pd.Timestamp, end_time: pd.Timestamp
) -> xr.Dataset:
    dims = ("init_time", "lead_time", "latitude", "longitude")
    chunks = {"init_time": 1, "lead_time": 2, "latitude": 145, "longitude": 144}

    coords = {
        "init_time": pd.date_range(start_time, end_time, freq="6h"),
        # "lead_time": pd.timedelta_range("3h", "240h", freq="3h"),
        "lead_time": pd.timedelta_range("3h", "9h", freq="3h"),
        # latitude descends when north is up
        "latitude": np.flip(np.arange(-90, 90.25, 0.25)),
        "longitude": np.arange(-180, 180, 0.25),
    }

    assert dims == tuple(chunks.keys())
    assert dims == tuple(coords.keys())

    var_names = (
        "pwat",
        "t2m",
        "u10",
        "v10",
        "tcc",
        "gh",
        "st",
        "soilw",
        "d2m",
        "tmax",
        "tmin",
        "r2",
        "hlcy",
        "prmsl",
        "mslet",
        "sp",
        "vis",
        "sde",
        "tp",
        "msshf",
        "mslhf",
        "crain",
        "cfrzr",
        "cicep",
        "csnow",
        "cpofp",
        "sdwe",
        "gust",
        "dswrf",
        "uswrf",
        "dlwrf",
        "sithick",
    )
    compressor = zarr.Blosc(cname="zstd", clevel=4)
    dtype = np.float32
    encodings = {
        var_name: {"chunks": [chunks[dim] for dim in dims], "compressor": compressor}
        for var_name in var_names
    }
    shape = [len(coords[dim]) for dim in dims]
    data_vars = {
        var_name: (dims, dask.array.zeros(shape=shape, dtype=dtype))
        for var_name in var_names
    }

    ds = xr.Dataset(data_vars=data_vars, coords=coords).chunk(chunks)

    for var in ds.data_vars:
        ds[var].encoding = encodings[var]

    return ds, chunks


def source_file_keys(ds):
    yield from (
        xbeam.Key(
            {
                "init_time": init_time_i,
                "lead_time": lead_time_i,
                "latitude": 0,
                "longitude": 0,
            }
        )
        for init_time_i in range(len(ds["init_time"]))
        for lead_time_i in range(len(ds["lead_time"]))
    )


def download_and_load_source_file(
    key: xbeam.Key, template_ds: xr.Dataset
) -> tuple[xbeam.Key, xr.Dataset]:
    dims = ["init_time", "lead_time"]
    offset_ds = template_ds.isel(**{dim: key.offsets[dim] for dim in dims})
    lead_time_hours = (offset_ds["lead_time"].dt.total_seconds() / (60 * 60)).item()
    init_date_str = offset_ds["init_time"].dt.strftime("%Y%m%d").item()
    init_hour_str = offset_ds["init_time"].dt.strftime("%H").item()

    # or gefs.20200927/00/atmos/pgrb2sp25/geavg.t00z.pgrb2s.0p25.f000
    url = (
        "https://storage.googleapis.com/gfs-ensemble-forecast-system/"
        "https://noaa-gefs-pds.s3.amazonaws.com/"
        f"gefs.{init_date_str}/{init_hour_str}/atmos/pgrb2sp25/"
        f"geavg.t{init_hour_str}z.pgrb2s.0p25.f{lead_time_hours:03.0f}"
    )

    # cfgrib/eccodes appears to only read files from disk, so write downloaded data there first.
    with tempfile.NamedTemporaryFile() as file:
        download(url, file.name)

        # Open the grib as N different datasets to pedantically map grib to xarray
        datasets = cfgrib.open_datasets(file.name)
        # TODO compat="minimal" is dropping 3 variables
        ds = xr.merge(
            datasets, compat="minimal", join="exact", combine_attrs="no_conflicts"
        )

        renames = {"time": "init_time", "step": "lead_time"}
        ds = ds.rename(renames).expand_dims(tuple(renames.values()))
        ds = ds.assign_coords({"longitude": template_ds["longitude"]})

        # TODO move cordinates about level height to something more natural for xarray
        ds = ds.drop_vars([c for c in ds.coords if c not in template_ds.coords])

        ds.load()

    return key, ds


@functools.cache
def http_session():
    session = requests.Session()
    retry = requests.adapters.Retry(
        total=5,
        redirect=1,
        backoff_factor=0.5,
        backoff_jitter=0.5,
        status_forcelist=(500, 502, 503, 504),
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def download(url: str, local_path: str):
    with http_session().get(url, stream=True, timeout=10) as response:
        response.raise_for_status()
        with open(local_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=None):
                file.write(chunk)


if __name__ == "__main__":
    create_pipeline(pd.Timestamp("2024-01-01T00:00"), pd.Timestamp("2024-01-01T06:00"))
