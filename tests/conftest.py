# content of conftest.py

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--runpanda", action="store_true", default=False, help="run panda tests"
        )

    
def pytest_configure(config):
    config.addinivalue_line("markers", "panda: mark test as panda to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runpanda"):
        # --runpanda given in cli: do not skip panda tests
        return
    skip_panda = pytest.mark.skip(reason="need --runpanda option to run")
    for item in items:
        if "panda" in item.keywords:
            item.add_marker(skip_panda)
