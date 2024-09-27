from pathlib import Path

import click
import geopandas as gpd
import papermill as pm
from tqdm import tqdm


DISTMETRIC_NAMES = [
    "transformer",
    "mahalanobis_2d",
    "mahalanobis_vh",
    "mahalanobis_1d_max",
    "log_ratio_vh",
    "cusum_vh",
    "cusum_prob_max",
]


def get_site_burst_count():
    df_val_bursts = gpd.read_parquet(
        "data/validation_bursts_v1_coverage_updated.parquet"
    )

    df_site_burst_count = (
        df_val_bursts.groupby("site_id")
        .size()
        .rename(index="bursts")
        .reset_index(drop=False)
    )
    return df_site_burst_count


@click.option(
    "--param_idx_start",
    required=False,
    type=int,
    default=0,
    help="which parameter index to start (site_id/burst_idx combo) for all steps",
)
@click.option(
    "--n_cpus",
    required=False,
    type=int,
    default=5,
    help="which parameter index to start (site_id/burst_idx combo)",
)
@click.option(
    "--step_start", required=False, type=int, default=1, help="Which Notebook to start"
)
@click.command()
def main(param_idx_start: int, n_cpus: int, step_start: int):
    in_nbs = [Path("1_Localize_RTC_Data.ipynb"), Path("2_Localize_Metric_tables.ipynb")]

    p = "out_notebooks"
    ipynb_dir = Path(p)
    ipynb_dir.mkdir(exist_ok=True, parents=True)

    df_count = get_site_burst_count()
    site_ids, counts = df_count.site_id.tolist(), df_count.bursts.tolist()
    parameters = [
        (site_id, burst_idx)
        for (site_id, count) in zip(site_ids, counts)
        for burst_idx in range(count)
    ]
    parameters = [
        (param_idx, site_id, count)
        for param_idx, (site_id, count) in enumerate(parameters)
    ]
    # Localize RTC Data
    if param_idx_start:
        parameters = parameters[param_idx_start:]
    nb_idx = step_start - 1
    for in_nb in in_nbs[nb_idx:]:
        step_ipynb_dir = ipynb_dir / in_nb.stem
        step_ipynb_dir.mkdir(exist_ok=True, parents=True)
        for param_idx, site_id, burst_idx in tqdm(
            parameters, desc="sites/bursts combos"
        ):
            print(f"{param_idx=}", f"{site_id=}", f"{burst_idx=}")
            out_nb = (
                step_ipynb_dir
                / f"{param_idx}__site{site_id}_burstidx{burst_idx}_{in_nb.stem[2:]}.ipynb"
            )
            if "1_" in in_nb.stem:
                pm.execute_notebook(
                    in_nb,
                    output_path=out_nb,
                    parameters=dict(
                        SITE_ID=site_id, IDX_BURST=burst_idx, N_CPUS=n_cpus
                    ),
                )
            if "2_" in in_nb.stem:
                for distmetric_name in DISTMETRIC_NAMES[:]:
                    pm.execute_notebook(
                        in_nb,
                        output_path=out_nb,
                        parameters=dict(
                            SITE_ID=site_id,
                            IDX_BURST=burst_idx,
                            DISTMETRIC_NAME=distmetric_name,
                        ),
                    )


if __name__ == "__main__":
    main()
