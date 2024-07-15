from pathlib import Path

import click
import papermill as pm
import geopandas as gpd
from tqdm import tqdm


@click.command()
def main():
    in_nb = Path('ts-explore-by-site-stream.ipynb')
    p = 'out_notebooks'
    ipynb_dir = Path(p)
    ipynb_dir.mkdir(exist_ok=True, parents=True)

    df_val_bursts = gpd.read_parquet('data/validation_bursts_v1_coverage_updated.parquet')

    site_ids = df_val_bursts.site_id.unique().tolist()
    df_count = df_val_bursts.groupby('site_id').size().rename(index='bursts').reset_index(drop=False)
    site_ids, counts = df_count.site_id.tolist(), df_count.bursts.tolist()
    parameters = [(site_id, burst_idx) for (site_id, count) in zip(site_ids, counts) for burst_idx in range(count)]

    for param in tqdm(parameters, desc='sites/bursts'):
        site_id, burst_idx = param
        print(f'{site_id=}', f'{burst_idx=}')
        out_nb = ipynb_dir / f'{site_id}_{burst_idx}.ipynb'
        pm.execute_notebook(in_nb,
                            output_path=out_nb,
                                parameters=dict(SITE_ID=site_id,
                                                IDX_BURST=burst_idx)
                            )

if __name__ == '__main__':
    main()