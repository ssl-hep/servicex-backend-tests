# conftest.py

import logging
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--stress", action="store_true", default=False, help="Run stress tests."
    )
    parser.addoption("--endpoint_xaod", action="store", default="xaod")
    parser.addoption("--endpoint_cms", action="store", default="cms_run1_aod")
    parser.addoption("--endpoint_uproot", action="store", default="uproot")


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


@pytest.fixture()
def endpoint_uproot(pytestconfig):
    return pytestconfig.getoption("endpoint_uproot")


@pytest.fixture
def uproot_single_file():
    return "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root"


@pytest.fixture
def xaod_did():
    return "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00"


@pytest.fixture
def large_xaod_did():
    return 'data18_13TeV:data18_13TeV.periodO.physics_Main.PhysCont.DAOD_PHYS.grp18_v01_p5002'


@pytest.fixture(autouse=True)
def turn_on_logging():
    logging.basicConfig(level=logging.DEBUG)
    yield None
    logging.basicConfig(level=logging.WARNING)
