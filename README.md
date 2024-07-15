# Dist-S1 Validation Harness

Utilizes OPERA RTC-S1 data to over DIST-HLS sites to:

1. generate baseline disturbance delineation over DIST-HLS sites ([data](data/val_sites_subset.geojson)) via Mahalonobis distance baseline
2. calculate basic accuracy metrics (total accuracy/user accuracy/producer accuracy) from baseline disturbance method and serialize data
3. Aggregate accuracy across all sites and with respect to diturbance type in the validation data


## Installation

```
mamba env update -f environment.yml
conda activate dist-s1
python -m ipykernel install --user --name dist-s1
```

## Usage

Run all the sites (in the `dist-s1` environment) via `python run_all_sites.py`.