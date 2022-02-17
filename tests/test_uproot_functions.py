# This script checks if a query using ServiceX is actually returning data correctly. This is the Uproot version.
# Requires that ServiceX be running with appropriate port-forward commands on ports 5000 and 9000 for ServixeX and Minio.
# Written by David Liu at the University of Washington, Seattle.
# 6 July 2020

from func_adl_servicex import ServiceXSourceUpROOT

dataset_name = "root://eospublic.cern.ch//eos/opendata/atlas/OutreachDatasets/2020-01-22/4lep/MC/mc_345060.ggH125_ZZ4lep.4lep.root"

# test if we can retrieve the Pts from this particular data set, and that we get back the correct number of lines.


def test_retrieve_simple_jet_pts_uproot():
    src = ServiceXSourceUpROOT([dataset_name], 'mini', backend_name="uproot")
    r = (
        src.Select(lambda e: {'lep_pt': e['lep_pt']}).AsAwkwardArray().value()
    )

    print(r)

    # assert len(r.JetPT) == 52909

# test if lambda capture is functional for uproot backend
# def test_lambda_capture():
#     dataset_uproot = "data15_13TeV:data15_13TeV.00282784.physics_Main.deriv.DAOD_PHYSLITE.r9264_p3083_p4165_tid21568807_00"
#     uproot_transformer_image = "sslhep/servicex_func_adl_uproot_transformer:issue6"

#     sx_dataset = ServiceXDataset(dataset_uproot, image=uproot_transformer_image)
#     ds = ServiceXSourceUpROOT("data15_13TeV:data15_13TeV.00282784.physics_Main.deriv.DAOD_PHYSLITE.r9264_p3083_p4165_tid21568807_00", "CollectionTree")
#     data = ds.Select("lambda e: e.Select(lambda jet: {'JetPt': jet.pt})")
