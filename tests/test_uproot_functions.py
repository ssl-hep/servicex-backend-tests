# This script checks if a query using ServiceX is actually returning data correctly. This is the Uproot version.
# Requires that ServiceX be running with appropriate port-forward commands on ports 5000 and 9000 for ServixeX and Minio.
# Written by David Liu at the University of Washington, Seattle.
# 6 July 2020

import servicex
from servicex import ServiceXDataset
from func_adl_servicex import ServiceXDatasetSource
import uproot_methods
from numpy import genfromtxt

# test if we can retrieve the Pts from this particular data set, and that we get back the correct number of lines.
def test_retrieve_simple_jet_pts_uproot():

    dataset = "user.kchoi:user.kchoi.ttHML_80fb_ttbar"
    uproot_transformer_image = "sslhep/servicex_func_adl_uproot_transformer:issue6"

    sx_dataset = ServiceXDataset(dataset, image=uproot_transformer_image)
    source = ServiceXDatasetSource(sx_dataset, "nominal")
    data = source.Select("lambda e: {'lep_pt_1': e.lep_Pt_1}") \
        .AsParquetFiles('junk.parquet') \
        .value()

    assert len(data.index) == 11355980