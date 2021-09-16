# conftest.py

import logging
import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--stress", action="store_true", default=False, help="Run stress tests."
    )
    parser.addoption("--endpoint_xaod", action="store", default="xaod")
    parser.addoption("--endpoint_cms", action="store", default="cms_run1_aod")

def pytest_configure(config):
    config.addinivalue_line("markers", "stress: mark test as a stress test")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--stress"):
        return
    skip_stress = pytest.mark.skip(reason="need --stress option to run")
    for item in items:
        if "stress" in item.keywords:
            item.add_marker(skip_stress)

@pytest.fixture()
def endpoint_xaod(pytestconfig):
    return pytestconfig.getoption("endpoint_xaod")


@pytest.fixture()
def endpoint_cms(pytestconfig):
    return pytestconfig.getoption("endpoint_cms")


@pytest.fixture(autouse=True)
def turn_on_logging():
    logging.basicConfig(level=logging.DEBUG)
    yield None
    logging.basicConfig(level=logging.WARNING)