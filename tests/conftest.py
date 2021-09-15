# conftest.py

import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--stress", action="store_true", default=False, help="Run stress tests."
    )
    parser.addoption("--endpoint_xaod", action="store", default="xaod")

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
