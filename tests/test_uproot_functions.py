# This script checks if a query using ServiceX is actually returning data correctly. This is the Uproot version.
# Written by David Liu at the University of Washington, Seattle.
# 6 July 2020

from func_adl_servicex import ServiceXSourceUpROOT
from servicex import ignore_cache

# Test with a data file that is directly accessible
dataset_name = "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root"

def test_retrieve_simple_jet_pts_uproot(endpoint_uproot):
    '''Check open data returns the expected number of leptons
    
    * Checks that ServiceX is up and responding
    * Bypasses the DID finder capabilities of ServiceX

    '''
    with ignore_cache():
        src = ServiceXSourceUpROOT([dataset_name], 'mini', backend_name=endpoint_uproot)
        r = (
            src.Select(lambda e: {'lep_pt': e['lep_pt']}).AsAwkwardArray().value()
        )

    print(r)

    assert len(r.lep_pt) == 164716

# test if lambda capture is functional for uproot backend
# def test_lambda_capture():
#     dataset_uproot = "data15_13TeV:data15_13TeV.00282784.physics_Main.deriv.DAOD_PHYSLITE.r9264_p3083_p4165_tid21568807_00"
#     uproot_transformer_image = "sslhep/servicex_func_adl_uproot_transformer:issue6"

#     sx_dataset = ServiceXDataset(dataset_uproot, image=uproot_transformer_image)
#     ds = ServiceXSourceUpROOT("data15_13TeV:data15_13TeV.00282784.physics_Main.deriv.DAOD_PHYSLITE.r9264_p3083_p4165_tid21568807_00", "CollectionTree")
#     data = ds.Select("lambda e: e.Select(lambda jet: {'JetPt': jet.pt})")
