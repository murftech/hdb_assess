import os
import subprocess
import sys

from pathlib import Path

import os
from pathlib import Path
# print(os.getcwd())

p = Path.cwd()
print(p)


while not (p / 'venv').exists() and p != p.parent:
    print('walk upwards')
    p = p.parent
MACROECONS_ROOT = p
print(MACROECONS_ROOT)

# try:
#     MACROECONS_ROOT = Path(__file__).resolve().parents[2]
#     print('running in python call')
# except NameError:
#     MACROECONS_ROOT = Path.cwd()
#     print('running in ipython')
#     print(f'cwd path in your system is is {MACROECONS_ROOT}')

if Path.cwd().name != 'hdb_assess':
    raise RuntimeError('Please run scripts from the hdb_assess root folder.')

def runpy(script):
    env = os.environ.copy()
    env['PYTHONPATH'] = str(MACROECONS_ROOT)
    subprocess.run([sys.executable, script], check=True, env=env)


runpy('scripts/0_data_unification.py')
# runpy('scripts/1_data_profiling.py')
runpy('scripts/2_data_validation.py')
runpy('scripts/3_data_cleaning.py')
runpy('scripts/4_transformation.py')
runpy('scripts/5_hash.py')
