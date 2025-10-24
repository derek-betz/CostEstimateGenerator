import os
from costest.config import load_config
from costest.cli import run
from pathlib import Path

env = dict(os.environ)
env['QUANTITIES_XLSX'] = str(Path('data_sample/2000030_project_quantities.xlsx').resolve())
env['EXPECTED_TOTAL_CONTRACT_COST'] = f"{5_000_000:.2f}"
env['PROJECT_REGION'] = '2'
env['PROJECT_DISTRICT'] = 'CRAWFORDSVILLE'
env['BIDTABS_CONTRACT_FILTER_PCT'] = f"{50.0:.6f}"
env['APPLY_DM23_21'] = '1'

cfg = load_config(env, None)
run(runtime_config=cfg)
