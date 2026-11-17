from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent

def pytest_configure(config):
    (ROOT/'tmp').mkdir(exist_ok=True)
    config.option.basetemp = str(ROOT/'tmp/pytest')

def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(ROOT/'tmp/pytest', ignore_errors=True)
