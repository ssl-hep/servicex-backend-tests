# This script checks if a query using ServiceX is actually returning data correctly. This is the Uproot version.
# Requires that ServiceX be running with appropriate port-forward commands on ports 5000 and 9000 for ServixeX and Minio.
# Written by David Liu at the University of Washington, Seattle.
# 6 July 2020

import servicex
from servicex import ServiceXDataset
from func_adl_servicex import ServiceXSourceUpROOT
import uproot_methods
import pandas as pd
import awkward as awk
from numpy import genfromtxt

# test if we can retrieve the Pts from this particular data set, and that we get back the correct number of lines.
def test_retrieve_simple_jet_pts_uproot():
    ds = ServiceXSourceUpROOT("data15_13TeV:data15_13TeV.00282784.physics_Main.deriv.DAOD_PHYSLITE.r9264_p3083_p4165_tid21568807_00", "CollectionTree")
    data = ds.Select("lambda e: {'JetPT': e['AnalysisJetsAuxDyn.pt']}") \
        .AsParquetFiles('junk.parquet') \
        .value()

    columnar_data = awk.fromparquet(data[1])

    assert len(columnar_data.JetPT) == 52909

# test if lambda capture is functional for uproot backend
# def test_lambda_capture():
#     dataset_uproot = "data15_13TeV:data15_13TeV.00282784.physics_Main.deriv.DAOD_PHYSLITE.r9264_p3083_p4165_tid21568807_00"
#     uproot_transformer_image = "sslhep/servicex_func_adl_uproot_transformer:issue6"

#     sx_dataset = ServiceXDataset(dataset_uproot, image=uproot_transformer_image)
#     ds = ServiceXSourceUpROOT("data15_13TeV:data15_13TeV.00282784.physics_Main.deriv.DAOD_PHYSLITE.r9264_p3083_p4165_tid21568807_00", "CollectionTree")
#     data = ds.Select("lambda e: e.Select(lambda jet: {'JetPt': jet.pt})")