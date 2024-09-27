import backoff
import numpy as np
import rasterio
from mpire import WorkerPool
from pyproj import Transformer
from rasterio import RasterioIOError
from rasterio.crs import CRS
from rasterio.transform import rowcol
from rasterio.transform import xy
from rasterio.windows import Window
from requests.exceptions import HTTPError


@backoff.on_exception(
    backoff.expo,
    (HTTPError, ConnectionError, RasterioIOError),
    max_tries=10,
    max_time=60,
    jitter=backoff.full_jitter,
)
def get_utm_coords(target_utm_crs: CRS, lon: float, lat: float) -> tuple[float, float]:
    transformer = Transformer.from_crs(CRS.from_epsg(4326), target_utm_crs, always_xy=True)
    utm_x, utm_y = transformer.transform(lon, lat)
    return utm_x, utm_y


@backoff.on_exception(
    backoff.expo,
    (HTTPError, ConnectionError, RasterioIOError),
    max_tries=10,
    max_time=60,
    jitter=backoff.full_jitter,
)
def read_window_from_raster(
    url: str, utm_x: float, utm_y: float, window_size=3
) -> np.ndarray:
    if (window_size % 2) != 1:
        raise ValueError("window size must be odd")
    with rasterio.open(url) as ds:
        src_trans = ds.transform
    row, col = rowcol(src_trans, utm_x, utm_y)
    row_m1 = max(row - window_size // 2, 0)
    col_m1 = max(col - window_size // 2, 0)
    window = Window(col_m1, row_m1, window_size, window_size)

    with rasterio.open(url) as ds:
        arr = ds.read(window=window)
        t_window = ds.window_transform(window)
        p_ref = ds.profile

    prof = p_ref.copy()
    prof['transform'] = t_window
    prof['height'], prof['width'] = arr.shape[-2], arr.shape[-1]

    return arr, prof


@backoff.on_exception(
    backoff.expo,
    (RuntimeError, OSError),
    max_tries=20,
    max_time=60,
    jitter=backoff.full_jitter,
)
def get_burst_time_series_around_point(
    urls: list[str], lon_site, lat_site, window_size=3, n_workers: int = 10
) -> list[np.ndarray]:

    with rasterio.open(urls[0]) as ds:
        utm_crs = ds.crs
    utm_x, utm_y = get_utm_coords(utm_crs, lon_site, lat_site)

    def read_window_from_raster_p(url: str):
        return read_window_from_raster(url, utm_x, utm_y, window_size=window_size)

    with WorkerPool(n_jobs=n_workers, use_dill=True) as pool:
        vals_l = pool.map(
            read_window_from_raster_p,
            urls,
            progress_bar=True,
            progress_bar_style="std",
            concatenate_numpy_output=False,
        )
    return vals_l
