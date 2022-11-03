# This script checks if a query using ServiceX is actually returning data correctly.
# This is the Uproot version.
# Written by David Liu at the University of Washington, Seattle.
# 6 July 2020

from func_adl_servicex import ServiceXSourceUpROOT
from servicex import ServiceXDataset
from servicex import ignore_cache


def test_retrieve_simple_jet_pts_uproot(endpoint_uproot, uproot_single_file):
    """Check open data returns the expected number of leptons

    * Checks that ServiceX is up and responding
    * Bypasses the DID finder capabilities of ServiceX

    """
    with ignore_cache():
        sx = ServiceXDataset([uproot_single_file],
                             backend_name=endpoint_uproot,
                             status_callback_factory=None)
        src = ServiceXSourceUpROOT(sx, 'mini')
        r = src.Select(lambda e: {'lep_pt': e['lep_pt']}).AsAwkwardArray().value()

    print(r)

    assert len(r.lep_pt) == 164716
